# worker_poll.py
# ------------------------------------------------------------
# Robust scrape worker:
#  - polls scrape_jobs
#  - claims job -> scrapes (hotel x day x adults)
#  - archives old snapshot per (user, slug, checkin)
#  - upserts room_prices_raw using on_conflict keys
#  - watchdog + stale reaper
#
# IMPORTANT:
#  - Default RAW_ON_CONFLICT assumes variants enabled:
#      user_id,slug,checkin,room,occupancy,rate_key
#  - If your DB still has legacy unique (without rate_key),
#    the worker will auto-fallback, but you should run the migration.
# ------------------------------------------------------------

import os
import time
import datetime as dt
from datetime import timezone
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import threading
import random
import requests
from requests import HTTPError

from scraper_core import (
    SUPABASE_URL,
    supabase_headers,
    get_own_hotel,
    get_competitor_hotels,
    DEFAULT_CC,
    scrape_hotel_for_dates,
    dedupe_min_per_room_and_occupancy,
)

# ---------- Config ----------
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
MAX_JOB_DURATION_SEC = int(os.getenv("MAX_JOB_DURATION_SEC", "7200"))
HEARTBEAT_EVERY_STEPS = int(os.getenv("HEARTBEAT_EVERY_STEPS", "8"))
STALE_JOB_MINUTES = int(os.getenv("STALE_JOB_MINUTES", "90"))
NO_PROGRESS_KILL_SEC = int(os.getenv("NO_PROGRESS_KILL_SEC", "1800"))

HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "25"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_BASE_SLEEP = float(os.getenv("RETRY_BASE_SLEEP", "0.6"))

JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
HISTORY_FUNC = "fn_archive_day_rows"
SOLDOUT_TABLE = "soldout_markers"
ALERTS_TABLE = "alert_events"

RAW_ON_CONFLICT_PRIMARY = os.getenv(
    "RAW_ON_CONFLICT",
    "user_id,slug,checkin,room,occupancy,rate_key",
)
RAW_ON_CONFLICT_LEGACY = os.getenv(
    "RAW_ON_CONFLICT_LEGACY",
    "user_id,slug,checkin,room,occupancy",
)

RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"

# allowlist payload keys
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
        "rate_key",
    ]
)
RAW_ALLOWED_KEYS = {k.strip() for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",") if k.strip()}
if not RAW_INCLUDE_JOB_ID:
    RAW_ALLOWED_KEYS.discard("job_id")

# ---------- HTTP helpers ----------

def now_iso_z() -> str:
    return (
        dt.datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _req_with_retry(method, path, *, params=None, json_body=None, headers=None, timeout=None):
    url = f"{SUPABASE_URL}{path}"
    for attempt in range(HTTP_RETRIES + 1):
        try:
            r = requests.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers or supabase_headers(json_pref=True),
                timeout=timeout or HTTP_TIMEOUT_SEC,
            )
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt >= HTTP_RETRIES:
                raise
            sleep = RETRY_BASE_SLEEP * (2 ** attempt) * (1 + 0.25 * random.random())
            print(
                f"🌐 retrying {method} {path} in {sleep:.2f}s (attempt {attempt+1}/{HTTP_RETRIES}) due to {type(e).__name__}"
            )
            time.sleep(sleep)


def http_get(path: str, params: Dict[str, Any]) -> requests.Response:
    return _req_with_retry("GET", path, params=params, headers=supabase_headers(json_pref=False))


def http_patch(path: str, params: Dict[str, Any], json_body: Dict[str, Any], prefer_return=False) -> requests.Response:
    h = supabase_headers(json_pref=True)
    if prefer_return:
        h["Prefer"] = "return=representation"
    return _req_with_retry("PATCH", path, params=params, json_body=json_body, headers=h)


def http_post(path: str, params: Dict[str, Any], json_body: Any, upsert=False, prefer: Optional[str] = None, timeout_sec: int = None) -> requests.Response:
    h = supabase_headers(json_pref=True, upsert=upsert)
    if prefer:
        h["Prefer"] = prefer
    return _req_with_retry("POST", path, params=params, json_body=json_body, headers=h, timeout=timeout_sec or HTTP_TIMEOUT_SEC)


def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    return _req_with_retry("DELETE", path, params=params, headers=supabase_headers(json_pref=False))

# ---------- Jobs helpers ----------

def fetch_next_pending_job() -> Optional[dict]:
    r = http_get(
        f"/rest/v1/{JOBS_TABLE}",
        {"select": "*", "status": "eq.pending", "order": "created_at.asc", "limit": 1},
    )
    rows = r.json() or []
    return rows[0] if rows else None


def claim_job(job_id: str) -> Optional[dict]:
    try:
        r = http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}", "status": "eq.pending"},
            {"status": "running", "started_at": now_iso_z(), "heartbeat_at": now_iso_z()},
            prefer_return=True,
        )
        rows = r.json() if r.text else []
        return rows[0] if rows else None
    except HTTPError as e:
        print("❌ claim_job error:", e, getattr(e.response, "text", "")[:400])
        return None


def set_job_fields(job_id: str, **patch):
    http_patch(f"/rest/v1/{JOBS_TABLE}", {"id": f"eq.{job_id}"}, patch, prefer_return=False)


def set_heartbeat(job_id: str):
    try:
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}"},
            {"heartbeat_at": now_iso_z()},
        )
    except Exception:
        pass


def is_canceled(job_id: str) -> bool:
    try:
        r = http_get(
            f"/rest/v1/{JOBS_TABLE}",
            {"select": "status", "id": f"eq.{job_id}", "limit": 1},
        )
        rows = r.json() or []
        if not rows:
            return True
        status = (rows[0].get("status") or "").lower()
        return status in ("canceled", "failed")
    except Exception:
        return True


def reap_stale_jobs():
    try:
        cutoff = (
            dt.datetime.now(timezone.utc) - dt.timedelta(minutes=STALE_JOB_MINUTES)
        ).isoformat().replace("+00:00", "Z")
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"status": "eq.running", "or": f"(heartbeat_at.is.null,heartbeat_at.lt.{cutoff})"},
            {"status": "failed", "finished_at": now_iso_z(), "last_error": "stale (reaped)"},
        )
    except Exception as e:
        print("⚠️ stale reaper error:", e)

# ---------- Snapshot + soldout helpers ----------

def archive_previous_snapshot(user_id: str, slug: str, checkin: str):
    try:
        r = http_post(
            f"/rest/v1/rpc/{HISTORY_FUNC}",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=25,
        )
        try:
            n = int(r.json() or 0)
        except Exception:
            n = 0
        print(f"📦 Archived {n} old rows for {slug} {checkin} before upsert")
    except Exception as e:
        print("⚠️ archive_previous_snapshot failed:", e, getattr(getattr(e, "response", None), "text", "")[:200])


def delete_current_snapshot(user_id: str, slug: str, checkin: str):
    try:
        r = http_post(
            "/rest/v1/rpc/fn_delete_day_rows",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=25,
        )
        try:
            n = int(r.json() or 0)
        except Exception:
            n = 0
        print(f"🧹 Deleted {n} rows for {slug} {checkin} (sold out)")
    except Exception as e:
        print("⚠️ delete_current_snapshot failed:", e)


def ensure_soldout_marker(user_id: str, slug: str, checkin: str) -> bool:
    try:
        r = http_post(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"on_conflict": "user_id,slug,checkin"},
            [{"user_id": user_id, "slug": slug.lower(), "checkin": checkin}],
            upsert=True,
            prefer="resolution=ignore-duplicates,return=representation",
            timeout_sec=25,
        )
        rows = r.json() if r.text else []
        return bool(rows)
    except Exception as e:
        print("⚠️ ensure_soldout_marker failed:", e)
        return False


def clear_soldout_marker(user_id: str, slug: str, checkin: str):
    try:
        http_delete(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"user_id": f"eq.{user_id}", "slug": f"eq.{slug.lower()}", "checkin": f"eq.{checkin}"},
        )
        print(f"🧽 Cleared sold-out marker for {slug} {checkin}")
    except Exception:
        pass

# ---------- RAW upsert ----------

def _sanitize_raw_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {}
    for k in RAW_ALLOWED_KEYS:
        if k in row:
            out[k] = row[k]

    if isinstance(out.get("slug"), str):
        out["slug"] = out["slug"].strip().lower()
    if isinstance(out.get("checkin"), str):
        out["checkin"] = out["checkin"][:10]
    if out.get("rate_key") is None:
        out["rate_key"] = ""

    try:
        out["occupancy"] = int(out.get("occupancy"))
    except Exception:
        out["occupancy"] = None

    try:
        out["price"] = float(out.get("price"))
    except Exception:
        out["price"] = None

    required = ("user_id", "slug", "checkin", "room", "occupancy", "price", "hotel")
    for rk in required:
        if out.get(rk) in (None, ""):
            return None
    if out["price"] < 5:
        return None
    return out


def _is_42p10(resp_text: str) -> bool:
    return "42P10" in (resp_text or "") or "no unique or exclusion constraint" in (resp_text or "")


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
            "breakfast_included": r.get("breakfast_included"),
            "dinner_included": r.get("dinner_included"),
            "half_board": r.get("half_board"),
            "free_cancellation": r.get("free_cancellation"),
            "nonrefundable": r.get("nonrefundable"),
            "prepay_required": r.get("prepay_required"),
            "rate_plan": r.get("rate_plan"),
            "rate_key": r.get("rate_key"),
        }
        if RAW_INCLUDE_JOB_ID:
            base["job_id"] = job_id
        s = _sanitize_raw_row(base)
        if s:
            clean.append(s)

    if not clean:
        return

    # keep cheapest per (user, slug, date, room, occ, rate_key)
    uniq: Dict[tuple, Dict[str, Any]] = {}
    for r in clean:
        key = (r["user_id"], r["slug"], r["checkin"], r["room"], r["occupancy"], r.get("rate_key") or "")
        if key not in uniq or r["price"] < uniq[key]["price"]:
            uniq[key] = r
    clean = list(uniq.values())

    def _post_batch(batch: List[Dict[str, Any]], on_conflict: str):
        return http_post(
            f"/rest/v1/{RAW_TABLE}",
            {"on_conflict": on_conflict},
            batch,
            upsert=True,
            timeout_sec=45,
        )

    for i in range(0, len(clean), 300):
        batch = clean[i : i + 300]
        try:
            _post_batch(batch, RAW_ON_CONFLICT_PRIMARY)
            print(f"✅ Upserted {len(batch)} rows into {RAW_TABLE}")
        except HTTPError as e:
            txt = getattr(e.response, "text", "") or ""
            if _is_42p10(txt):
                # Legacy DB: collapse to rate_key='' and retry with legacy key
                for rr in batch:
                    rr["rate_key"] = ""
                _post_batch(batch, RAW_ON_CONFLICT_LEGACY)
                print(
                    f"✅ Upserted {len(batch)} rows into {RAW_TABLE} (LEGACY on_conflict)"
                )
            else:
                print("❌ upsert_room_prices error:", e, txt[:500])
                if batch:
                    print("   sample row:", batch[0])
                raise

# ---------- Step wrapper ----------

STEP_LOCK = threading.Lock()


def _scrape_once(job_id: str, user_id: str, hotel: Dict[str, Any], checkin: str, checkout: str) -> Dict[str, Any]:
    slug_key = (hotel.get("scrape_slug") or hotel.get("slug") or "").strip().lower()
    base_slug, cc = slug_key.split("__", 1) if "__" in slug_key else (slug_key, hotel.get("cc") or "")
    cc = (cc or hotel.get("cc") or DEFAULT_CC).strip().lower() or DEFAULT_CC
    stored_slug = f"{base_slug}__{cc}" if cc else base_slug

    try:
        # scraper_core.scrape_hotel_for_dates signature is:
        #   scrape_hotel_for_dates(name, slug, cc, checkin, checkout, should_cancel=None)
        raw_rows = scrape_hotel_for_dates(
            name=hotel.get("name") or "",
            slug=base_slug,
            cc=cc,
            checkin=checkin,
            checkout=checkout,
            should_cancel=lambda: is_canceled(job_id),
        )
        deduped = dedupe_min_per_room_and_occupancy(raw_rows)
        for r in deduped:
            r["user_id"] = user_id
            r["slug"] = stored_slug
            if not r.get("hotel"):
                r["hotel"] = hotel.get("name")
        return {"slug": stored_slug, "checkin": checkin, "rows": deduped}
    except Exception as e:
        print("⚠️ scrape task error:", e)
        return {"slug": stored_slug, "checkin": checkin, "rows": []}

# ---------- Main job loop ----------

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
        return (dt.datetime.now(timezone.utc) - job_started).total_seconds() > MAX_JOB_DURATION_SEC

    # Discover hotels
    own = get_own_hotel(user_id)
    own_id = own["hotel_id"] if own else None
    competitors = get_competitor_hotels(user_id, own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append(
            {
                "name": own["name"],
                "slug": own["slug"],
                "cc": own.get("cc") or DEFAULT_CC,
                "scrape_slug": f"{own['slug']}__{(own.get('cc') or DEFAULT_CC)}",
                "own": True,
            }
        )
    for c in competitors:
        hotels.append(
            {
                "name": c.get("name") or "",
                "slug": c.get("slug") or "",
                "cc": c.get("cc") or DEFAULT_CC,
                "scrape_slug": f"{(c.get('slug') or '').strip().lower()}__{(c.get('cc') or DEFAULT_CC).strip().lower()}",
                "own": False,
            }
        )

    # Optional job slug filter (meta.slugs)
    job_slugs = set()
    raw_slugs = ((job.get("meta") or {}).get("slugs") or None)
    if isinstance(raw_slugs, list):
        for s in raw_slugs:
            if isinstance(s, str) and s.strip():
                job_slugs.add(s.strip().lower())

    if job_slugs:
        before = len(hotels)
        hotels = [h for h in hotels if h.get("own") or h.get("scrape_slug") in job_slugs or (h.get("slug") or "") in job_slugs]
        print(f"🔎 job slug filter: had {before}, now {len(hotels)} (own forced in)")

    if not hotels:
        print(f"❌ job {job_id} has no hotels to scrape for user {user_id}")
        set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error="no hotels configured")
        return

    print(f"🚀 process_job {job_id} (user {user_id}) | range_days={range_days} | hotels={len(hotels)}")

    total_steps = len(hotels) * range_days
    set_job_fields(job_id, total_steps=total_steps, completed_steps=0)

    completed_steps = 0
    buffer_rows: List[dict] = []
    buffer_lock = threading.Lock()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = []
            for h in hotels:
                for d in range(range_days):
                    checkin = (start_date + dt.timedelta(days=d)).strftime("%Y-%m-%d")
                    checkout = (start_date + dt.timedelta(days=d + 1)).strftime("%Y-%m-%d")
                    futures.append(pool.submit(_scrape_once, job_id, user_id, h, checkin, checkout))

            pending = set(futures)
            last_progress = time.time()

            while pending:
                if is_canceled(job_id):
                    break
                if over_time_budget():
                    set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error="time budget exceeded")
                    print(f"⏱️ job {job_id} exceeded time budget; failing")
                    return

                done, pending = wait(pending, timeout=15, return_when=FIRST_COMPLETED)
                if not done:
                    if (time.time() - last_progress) > NO_PROGRESS_KILL_SEC:
                        try:
                            set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error=f"worker hang: no progress for {NO_PROGRESS_KILL_SEC}s")
                        except Exception:
                            pass
                        print(f"🧨 No progress for {NO_PROGRESS_KILL_SEC}s -> exiting worker so platform restarts")
                        os._exit(2)
                    continue

                for fut in done:
                    last_progress = time.time()

                    try:
                        result = fut.result()
                    except Exception as e:
                        print("⏳ step error:", type(e).__name__, e)
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(job_id, completed_steps=completed_steps, last_error="step error")
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)
                        continue

                    try:
                        slug = (result.get("slug") or "").lower()
                        checkin = result.get("checkin")
                        rows = result.get("rows") or []

                        if rows and slug and checkin:
                            archive_previous_snapshot(user_id, slug, checkin)

                        if rows:
                            with buffer_lock:
                                buffer_rows.extend(rows)
                                if len(buffer_rows) >= 300:
                                    upsert_room_prices(user_id, job_id, buffer_rows)
                                    # clear sold-out markers for those pairs
                                    seen_pairs = set()
                                    for rr in buffer_rows:
                                        k = (rr.get("slug"), rr.get("checkin"))
                                        if k not in seen_pairs and rr.get("slug") and rr.get("checkin"):
                                            clear_soldout_marker(user_id, rr["slug"], rr["checkin"])
                                            seen_pairs.add(k)
                                    buffer_rows.clear()
                        else:
                            if slug and checkin:
                                delete_current_snapshot(user_id, slug, checkin)
                                ensure_soldout_marker(user_id, slug, checkin)

                    except Exception as e:
                        print("⚠️ parallel scrape step error:", e)
                        set_job_fields(job_id, last_error=str(e))
                    finally:
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(job_id, completed_steps=completed_steps)
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)

        # flush buffer
        if buffer_rows:
            try:
                upsert_room_prices(user_id, job_id, buffer_rows)
            except Exception as e:
                print("⚠️ final upsert buffer err:", e)
            seen_pairs = set()
            for rr in buffer_rows:
                k = (rr.get("slug"), rr.get("checkin"))
                if k not in seen_pairs and rr.get("slug") and rr.get("checkin"):
                    clear_soldout_marker(user_id, rr["slug"], rr["checkin"])
                    seen_pairs.add(k)

        set_heartbeat(job_id)
        if is_canceled(job_id):
            set_job_fields(job_id, status="canceled", finished_at=now_iso_z())
        else:
            set_job_fields(job_id, status="done", finished_at=now_iso_z())

    except HTTPError as http_err:
        print("💥 HTTP error in process_job:", http_err, getattr(http_err.response, "text", "")[:500])
        try:
            set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error=str(http_err))
        except Exception:
            pass
    except Exception as fatal:
        print("💥 fatal in process_job:", fatal)
        try:
            set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error=str(fatal))
        except Exception:
            pass


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
            print("worker loop HTTP error:", http_err, getattr(http_err.response, "text", "")[:500])
            time.sleep(10)
        except Exception as e:
            print("worker loop error:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
