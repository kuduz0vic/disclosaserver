# worker_poll.py
# ──────────────────────────────────────────────────────────────────────────────
# Background worker:
#  - Polls public.scrape_jobs
#  - For each job:
#      * fetches own + competitor hotels from scraper_core
#      * scrapes Booking.com in parallel via scrape_hotel_for_dates()
#      * archives previous snapshot
#      * upserts into room_prices_raw (incl. rate-plan flags: dinner, HB, etc.)
#      * maintains soldout_markers + alerts
#  - Has improved cancellation handling to avoid Playwright CancelledError spam
# ──────────────────────────────────────────────────────────────────────────────

import os
import time
import datetime as dt
from datetime import timezone
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import threading

import requests
from requests import HTTPError

from scraper_core import (
    SUPABASE_URL,
    SUPABASE_KEY,
    supabase_headers,
    get_own_hotel,
    get_competitor_hotels,
    resolve_cc_for_slug,
    RESOLVE_CC_IF_MISSING,
    DEFAULT_CC,
    scrape_hotel_for_dates,
    dedupe_min_per_room_and_occupancy,
)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
MAX_JOB_DURATION_SEC = int(os.getenv("MAX_JOB_DURATION_SEC", "5400"))
HEARTBEAT_EVERY_STEPS = int(os.getenv("HEARTBEAT_EVERY_STEPS", "8"))
STALE_JOB_MINUTES = int(os.getenv("STALE_JOB_MINUTES", "90"))

JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
HISTORY_FUNC = "fn_archive_day_rows"
SOLDOUT_TABLE = "soldout_markers"
ALERTS_TABLE = "alert_events"
RAW_ON_CONFLICT = "user_id,slug,checkin,room,occupancy"

RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"

# IMPORTANT:
# Make sure RAW_ALLOWED_KEYS env in Railway includes the flag columns:
#  user_id,slug,checkin,room,occupancy,price,hotel,job_id,
#  breakfast_included,dinner_included,half_board,
#  free_cancellation,nonrefundable,prepay_required,rate_plan
_raw_allowed_default = ",".join(
    [
        "user_id",
        "slug",
        "checkin",
        "room",
        "occupancy",
        "price",
        "hotel",
        "job_id",
        "breakfast_included",
        "dinner_included",
        "half_board",
        "free_cancellation",
        "nonrefundable",
        "prepay_required",
        "rate_plan",
    ]
)
RAW_ALLOWED_KEYS = {
    k.strip()
    for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",")
    if k.strip()
}
if not RAW_INCLUDE_JOB_ID and "job_id" in RAW_ALLOWED_KEYS:
    RAW_ALLOWED_KEYS.remove("job_id")

# ──────────────────────────────────────────────────────────────────────────────
# Generic HTTP helpers for Supabase REST
# ──────────────────────────────────────────────────────────────────────────────


def now_iso_z() -> str:
    return (
        dt.datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def http_get(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = requests.get(
        url, params=params, headers=supabase_headers(json_pref=False), timeout=30
    )
    r.raise_for_status()
    return r


def http_patch(
    path: str,
    params: Dict[str, Any],
    json_body: Dict[str, Any],
    prefer_return: bool = False,
) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers()
    if prefer_return:
        headers["Prefer"] = "return=representation"
    r = requests.patch(
        url, params=params, json=json_body, headers=headers, timeout=30
    )
    r.raise_for_status()
    return r


def http_post(
    path: str,
    params: Dict[str, Any],
    json_body: Any,
    upsert: bool = False,
    prefer: Optional[str] = None,
    timeout_sec: int = 60,
) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers(upsert=upsert)
    if prefer:
        headers["Prefer"] = prefer
    r = requests.post(
        url, params=params, json=json_body, headers=headers, timeout=timeout_sec
    )
    r.raise_for_status()
    return r


def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = requests.delete(
        url, params=params, headers=supabase_headers(json_pref=False), timeout=30
    )
    r.raise_for_status()
    return r


# ──────────────────────────────────────────────────────────────────────────────
# scrape_jobs helpers
# ──────────────────────────────────────────────────────────────────────────────


def fetch_next_pending_job() -> Optional[dict]:
    """
    Old behaviour: pick the oldest 'pending' job (any user).
    You already enforce one active job per user with:
        scrape_jobs_one_active_per_user (partial unique index).
    """
    r = http_get(
        f"/rest/v1/{JOBS_TABLE}",
        {
            "select": "*",
            "status": "eq.pending",
            "order": "created_at.asc",
            "limit": 1,
        },
    )
    rows = r.json()
    return rows[0] if rows else None


def claim_job(job_id: str) -> Optional[dict]:
    """
    Atomically move job from pending -> running.
    If another worker raced us, PATCH will affect 0 rows and we return None.
    """
    try:
        r = http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}", "status": "eq.pending"},
            {
                "status": "running",
                "started_at": now_iso_z(),
                "heartbeat_at": now_iso_z(),
            },
            prefer_return=True,
        )
        rows = r.json() if r.text else []
        return rows[0] if rows else None
    except HTTPError as e:
        print(
            "❌ claim_job error:",
            e,
            getattr(e.response, "text", "")[:400],
        )
        return None


def set_job_fields(job_id: str, **patch):
    """
    Generic patch helper. On HTTP error we log and re-raise.
    """
    try:
        http_patch(f"/rest/v1/{JOBS_TABLE}", {"id": f"eq.{job_id}"}, patch)
    except HTTPError as e:
        print(
            "❌ set_job_fields error:",
            e,
            "| payload:",
            patch,
            "| resp:",
            getattr(e.response, "text", "")[:400],
        )
        raise


def set_heartbeat(job_id: str):
    """
    Lightweight heartbeat. We swallow all errors here by design.
    """
    try:
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}"},
            {"heartbeat_at": now_iso_z()},
        )
    except Exception:
        pass


def is_canceled(job_id: str) -> bool:
    """
    Check if job moved to canceled / failed while scraping.
    Any fetch error -> treat as canceled.
    """
    try:
        r = http_get(
            f"/rest/v1/{JOBS_TABLE}",
            {"select": "status", "id": f"eq.{job_id}", "limit": 1},
        )
        rows = r.json()
        if not rows:
            return True
        status = (rows[0].get("status") or "").lower()
        return status in ("canceled", "failed")
    except HTTPError as e:
        print(
            "⚠️ is_canceled check failed:",
            e,
            getattr(e.response, "text", "")[:200],
        )
        return True


def reap_stale_jobs():
    """
    Mark jobs as failed if they've been 'running' but missing heartbeat for too long.
    """
    try:
        cutoff = (
            dt.datetime.now(timezone.utc)
            - dt.timedelta(minutes=STALE_JOB_MINUTES)
        ).isoformat().replace("+00:00", "Z")
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {
                "status": "eq.running",
                "or": f"(heartbeat_at.is.null,heartbeat_at.lt.{cutoff})",
            },
            {
                "status": "failed",
                "finished_at": now_iso_z(),
                "last_error": "stale (reaped)",
            },
        )
    except HTTPError as e:
        print(
            "⚠️ stale reaper error:",
            e,
            getattr(e.response, "text", "")[:400],
        )


# ─────────────────────────────────────────────────────────────────────────────-
# Archiving + sold-out helpers
# ─────────────────────────────────────────────────────────────────────────────-


def archive_previous_snapshot(user_id: str, slug: str, checkin: str):
    """
    Move old rows for (user,slug,checkin) into history table before we upsert new ones.
    """
    try:
        r = http_post(
            f"/rest/v1/rpc/{HISTORY_FUNC}",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=30,
        )
        print(
            f"📦 Archived {int(r.json() or 0)} old rows for {slug} {checkin} before upsert"
        )
    except HTTPError as e:
        print(
            "⚠️ archive_previous_snapshot failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )


def delete_current_snapshot(user_id: str, slug: str, checkin: str):
    """
    Delete current rows (if any) when we detect "no rooms available" / sold out.
    """
    try:
        r = http_post(
            "/rest/v1/rpc/fn_delete_day_rows",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=30,
        )
        print(
            f"🧹 Deleted {int(r.json() or 0)} rows for {slug} {checkin} (sold out)"
        )
    except HTTPError as e:
        print(
            "⚠️ delete_current_snapshot failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )


def ensure_soldout_marker(user_id: str, slug: str, checkin: str) -> bool:
    """
    Insert soldout_markers(user_id,slug,checkin) via upsert.
    Returns True if we inserted a new marker, False if it already existed.
    """
    try:
        r = http_post(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"on_conflict": "user_id,slug,checkin"},
            [
                {
                    "user_id": user_id,
                    "slug": slug.lower(),
                    "checkin": checkin,
                }
            ],
            upsert=True,
            prefer="resolution=ignore-duplicates,return=representation",
        )
        rows = r.json() if r.text else []
        return bool(rows)
    except HTTPError as e:
        print(
            "⚠️ ensure_soldout_marker failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )
        return False


def clear_soldout_marker(user_id: str, slug: str, checkin: str):
    """
    Remove marker once we have at least one room again.
    """
    try:
        http_delete(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {
                "user_id": f"eq.{user_id}",
                "slug": f"eq.{slug.lower()}",
                "checkin": f"eq.{checkin}",
            },
        )
    except HTTPError as e:
        print(
            "⚠️ clear_soldout_marker failed:",
            e,
            getattr(e.response, "text", "")[:200],
        )
    except Exception as e:
        print("⚠️ clear_soldout_marker error:", e)


def insert_soldout_alert(user_id: str, slug: str, checkin: str):
    """
    Emit AUTO_SOLD_OUT alert into alert_events.
    """
    payload = {
        "type": "AUTO_SOLD_OUT",
        "user_id": user_id,
        "slug": slug.lower(),
        "checkin": checkin,
        "price": None,
        "payload": {
            "source": "worker_poll",
            "reason": "no rooms parsed for any occupancy",
        },
    }
    try:
        http_post(
            f"/rest/v1/{ALERTS_TABLE}",
            {},
            payload,
            upsert=False,
            timeout_sec=30,
        )
        print(
            f"🔔 ALERT AUTO_SOLD_OUT for {slug} {checkin} (user={user_id})"
        )
    except HTTPError as e:
        print(
            "⚠️ insert_soldout_alert failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )


# ─────────────────────────────────────────────────────────────────────────────-
# Upsert into room_prices_raw (including flags)
# ─────────────────────────────────────────────────────────────────────────────-


def _sanitize_raw_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {}
    for k in RAW_ALLOWED_KEYS:
        if k in row:
            out[k] = row[k]

    if "slug" in out and isinstance(out["slug"], str):
        out["slug"] = out["slug"].strip().lower()
    if "checkin" in out and isinstance(out["checkin"], str):
        out["checkin"] = out["checkin"][:10]
    if "occupancy" in out:
        try:
            out["occupancy"] = int(out["occupancy"])
        except Exception:
            out["occupancy"] = None
    if "price" in out:
        try:
            out["price"] = float(out["price"])
        except Exception:
            out["price"] = None

    required_keys = ("user_id", "slug", "checkin", "room", "occupancy")
    for rk in required_keys:
        if not out.get(rk):
            return None

    if "hotel" in RAW_ALLOWED_KEYS and not out.get("hotel"):
        return None
    if out.get("price") is None:
        return None

    return out


def upsert_room_prices(user_id: str, job_id: str, rows: List[dict]):
    if not rows:
        return

    clean: List[Dict[str, Any]] = []
    for r in rows:
        base = {
            "user_id": user_id,
            "slug": r.get("slug"),
            "hotel": r.get("hotel"),
            "checkin": r.get("checkin"),
            "room": r.get("room"),
            "occupancy": r.get("occupancy"),
            "price": r.get("price"),
            # Rate-plan flags – these MUST be set in scraper_core.scrape_hotel_for_dates
            "breakfast_included": r.get("breakfast_included"),
            "dinner_included": r.get("dinner_included"),
            "half_board": r.get("half_board"),
            "free_cancellation": r.get("free_cancellation"),
            "nonrefundable": r.get("nonrefundable"),
            "prepay_required": r.get("prepay_required"),
            "rate_plan": r.get("rate_plan"),
        }
        if RAW_INCLUDE_JOB_ID:
            base["job_id"] = job_id

        s = _sanitize_raw_row(base)
        if s:
            clean.append(s)

    if not clean:
        return

    # De-duplicate inside batch to avoid "row is affected twice" on ON CONFLICT
    uniq: Dict[tuple, Dict[str, Any]] = {}
    for r in clean:
        key = (r["user_id"], r["slug"], r["checkin"], r["room"], r["occupancy"])
        if key not in uniq:
            uniq[key] = r
        else:
            # keep cheaper rate if duplicates slip in
            if r["price"] < uniq[key]["price"]:
                uniq[key] = r

    clean = list(uniq.values())

    for i in range(0, len(clean), 300):
        batch = clean[i : i + 300]
        try:
            http_post(
                f"/rest/v1/{RAW_TABLE}",
                {"on_conflict": RAW_ON_CONFLICT},
                batch,
                upsert=True,
                timeout_sec=60,
            )
            print(f"✅ Upserted {len(batch)} rows into room_prices_raw")
        except HTTPError as e:
            print(
                "❌ upsert_room_prices error:",
                e,
                getattr(e.response, "text", "")[:400],
            )
            if batch:
                print("   sample row:", batch[0])
            raise


# ─────────────────────────────────────────────────────────────────────────────-
# Scrape step
# ─────────────────────────────────────────────────────────────────────────────-

STEP_LOCK = threading.Lock()


def _scrape_once(
    job_id: str,
    user_id: str,
    hotel: Dict[str, Any],
    checkin: str,
    checkout: str,
) -> Dict[str, Any]:
    slug = (hotel.get("slug") or "").lower()
    try:
        cc = (
            hotel.get("cc")
            or (resolve_cc_for_slug(slug) if RESOLVE_CC_IF_MISSING else None)
            or DEFAULT_CC
        )

        # scraper_core.scrape_hotel_for_dates must honour should_cancel()
        raw_rows = scrape_hotel_for_dates(
            hotel["name"],
            slug,
            cc,
            checkin,
            checkout,
            should_cancel=lambda: is_canceled(job_id),
        )

        deduped = dedupe_min_per_room_and_occupancy(raw_rows)

        for r in deduped:
            if not r.get("hotel"):
                r["hotel"] = hotel["name"]
            r["user_id"] = user_id
        return {"slug": slug, "checkin": checkin, "rows": deduped}
    except Exception as e:
        print("⚠️ scrape task error:", e)
        return {"slug": slug, "checkin": checkin, "rows": []}


# ─────────────────────────────────────────────────────────────────────────────-
# Job processing
# ─────────────────────────────────────────────────────────────────────────────-


def process_job(first_row: dict):
    job_id = first_row["id"]
    claimed = claim_job(job_id)
    if not claimed:
        print(f"↩️ Job {job_id} already claimed. Skip.")
        return

    job = claimed
    user_id = job["user_id"]
    range_days = int(job.get("range_days") or 1)
    start_date = dt.date.today()
    job_started = dt.datetime.now(timezone.utc)

    def over_time_budget() -> bool:
        return (
            dt.datetime.now(timezone.utc) - job_started
        ).total_seconds() > MAX_JOB_DURATION_SEC

    def canceled() -> bool:
        return is_canceled(job_id)

    # Fetch own + competitors via scraper_core
    own = get_own_hotel(user_id)
    own_id = own["hotel_id"] if own else None
    competitors = get_competitor_hotels(user_id, own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append(
            {
                "name": own["name"],
                "slug": own["slug"],
                "cc": own.get("cc"),
                "own": True,
            }
        )
    for c in competitors:
        hotels.append(
            {
                "name": c["name"],
                "slug": c["slug"],
                "cc": c.get("cc"),
                "own": False,
            }
        )

    # Optional job slug filter (meta.slugs)
    job_slugs = set()
    try:
        raw_slugs = (job.get("meta") or {}).get("slugs") or []
        if isinstance(raw_slugs, list):
            for s in raw_slugs:
                if isinstance(s, str):
                    job_slugs.add(s.strip().lower())
    except Exception:
        pass

    if job_slugs:
        before = len(hotels)
        seen, deduped = set(), []
        for h in hotels:
            slug = (h.get("slug") or "").lower()
            if h.get("own") or slug in job_slugs:
                if slug and slug not in seen:
                    seen.add(slug)
                    deduped.append(h)
        hotels = deduped
        print(
            f"🔎 job slug filter: had {before}, now {len(hotels)} (own forced in)"
        )

    if not hotels:
        print(
            f"❌ job {job_id} has no hotels to scrape for user {user_id}"
        )
        set_job_fields(
            job_id,
            status="failed",
            finished_at=now_iso_z(),
            last_error="no hotels configured",
        )
        return

    print(
        f"🚀 process_job {job_id} (user {user_id}) | range_days={range_days} | hotels={len(hotels)}"
    )
    total_steps = len(hotels) * range_days
    set_job_fields(
        job_id, total_steps=(total_steps or None), completed_steps=0
    )

    completed_steps = 0
    buffer_rows: List[dict] = []
    buffer_lock = threading.Lock()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = []
            for h in hotels:
                for d in range(range_days):
                    checkin = (start_date + dt.timedelta(days=d)).strftime(
                        "%Y-%m-%d"
                    )
                    checkout = (start_date + dt.timedelta(days=d + 1)).strftime(
                        "%Y-%m-%d"
                    )
                    futures.append(
                        pool.submit(
                            _scrape_once,
                            job_id,
                            user_id,
                            h,
                            checkin,
                            checkout,
                        )
                    )

            for fut in as_completed(futures):
                # 🔥 Improved cancellation behaviour:
                # If job is canceled or time budget exceeded:
                #  - cancel remaining futures
                #  - avoid hammering Playwright with CancelledError spam
                if canceled():
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    print(f"🛑 job {job_id} canceled mid-run")
                    break

                if over_time_budget():
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    set_job_fields(
                        job_id,
                        status="failed",
                        finished_at=now_iso_z(),
                        last_error="time budget exceeded",
                    )
                    print(f"⏱️ job {job_id} exceeded time budget; failing")
                    return

                try:
                    result = fut.result() or {}
                except CancelledError:
                    # expected when we cancel futures – just skip
                    continue
                except Exception as e:
                    print("⚠️ parallel scrape step error:", e)
                    set_job_fields(job_id, last_error=str(e))
                    continue

                slug = (result.get("slug") or "").lower()
                checkin = result.get("checkin")
                rows = result.get("rows") or []

                if rows:
                    if slug and checkin:
                        archive_previous_snapshot(user_id, slug, checkin)

                    with buffer_lock:
                        buffer_rows.extend(rows)
                        if len(buffer_rows) >= 300:
                            upsert_room_prices(user_id, job_id, buffer_rows)
                            # clear soldout markers for these pairs
                            seen_pairs = set()
                            for rr in buffer_rows:
                                key = (rr.get("slug"), rr.get("checkin"))
                                if (
                                    key not in seen_pairs
                                    and rr.get("slug")
                                    and rr.get("checkin")
                                ):
                                    clear_soldout_marker(
                                        user_id, rr["slug"], rr["checkin"]
                                    )
                                    seen_pairs.add(key)
                            buffer_rows.clear()
                else:
                    # No rows => treat as sold-out for that hotel/checkin
                    if slug and checkin:
                        try:
                            archive_previous_snapshot(user_id, slug, checkin)
                        except Exception:
                            pass
                        delete_current_snapshot(user_id, slug, checkin)
                        ensure_soldout_marker(user_id, slug, checkin)
                        insert_soldout_alert(user_id, slug, checkin)

                # Progress / heartbeat
                with STEP_LOCK:
                    completed_steps += 1
                    set_job_fields(job_id, completed_steps=completed_steps)
                    if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                        set_heartbeat(job_id)

        # Flush remaining buffer
        if buffer_rows:
            upsert_room_prices(user_id, job_id, buffer_rows)
            seen_pairs = set()
            for rr in buffer_rows:
                key = (rr.get("slug"), rr.get("checkin"))
                if (
                    key not in seen_pairs
                    and rr.get("slug")
                    and rr.get("checkin")
                ):
                    clear_soldout_marker(user_id, rr["slug"], rr["checkin"])
                    seen_pairs.add(key)

        set_heartbeat(job_id)
        if canceled():
            set_job_fields(
                job_id,
                status="canceled",
                finished_at=now_iso_z(),
            )
        else:
            set_job_fields(
                job_id,
                status="done",
                finished_at=now_iso_z(),
            )

    except HTTPError as http_err:
        print(
            "💥 HTTP error in process_job:",
            http_err,
            getattr(http_err.response, "text", "")[:400],
        )
        try:
            set_job_fields(
                job_id,
                status="failed",
                finished_at=now_iso_z(),
                last_error=str(http_err),
            )
        except Exception:
            pass
    except Exception as fatal:
        print("💥 fatal in process_job:", fatal)
        try:
            set_job_fields(
                job_id,
                status="failed",
                finished_at=now_iso_z(),
                last_error=str(fatal),
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────-
# Main loop
# ─────────────────────────────────────────────────────────────────────────────-


def main():
    print("⏳ Worker online. Polling scrape_jobs...")
    while True:
        try:
            reap_stale_jobs()
            job = fetch_next_pending_job()
            if not job:
                time.sleep(10)
                continue
            print("🎯 picked job", job.get("id"))
            process_job(job)
        except HTTPError as http_err:
            print(
                "worker loop HTTP error:",
                http_err,
                getattr(http_err.response, "text", "")[:400],
            )
            time.sleep(10)
        except Exception as e:
            print("worker loop error:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
