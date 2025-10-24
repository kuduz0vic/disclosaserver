# worker_poll.py
# ------------------------------------------------------------------------------
# Background worker for scraping Booking, persisting prices, and generating
# alerts. This version:
#
# - Uses occupancy-aware scrape (1..4 adults).
# - Writes per-room, per-occupancy prices to room_prices_raw (upsert).
# - Archives the previous snapshot for (user_id, slug, checkin) BEFORE writing
#   fresh rows, so we can compare history for price drop/spike.
# - Sold-out logic with debounce (marker table + AUTO_SOLD_OUT alert).
# - Does NOT delete old rows when sold out, so we still have price history.
#
# ENV (required):
#   SUPABASE_URL
#   SUPABASE_SERVICE_ROLE_KEY
#
# ENV (optional):
#   MAX_WORKERS=4
#   DEFAULT_CC=si
#   RESOLVE_CC_IF_MISSING=1
#   NO_SANDBOX=1
#
# Tables used (must exist):
#   scrape_jobs
#   user_hotels → hotels → hotel_profiles
#   room_prices_raw
#   room_prices_history
#   soldout_markers
#   alert_events
#
# RPCs / helpers required in DB:
#   fn_archive_day_rows(p_user_id uuid, p_slug text, p_checkin date)
#
# IMPORTANT INDEX/UNIQUE in room_prices_raw:
#   UNIQUE (user_id, slug, checkin, room, occupancy)
# ------------------------------------------------------------------------------

import os
import time
import datetime as dt
from datetime import timezone
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
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

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# table / view names
JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
HISTORY_FUNC = "fn_archive_day_rows"
SOLDOUT_TABLE = "soldout_markers"
ALERTS_TABLE = "alert_events"

# upsert conflict target for prices
RAW_ON_CONFLICT = "user_id,slug,checkin,room,occupancy"

# optional: store job_id in raw for debugging
RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"

# whitelist of columns we allow into room_prices_raw
_raw_allowed_default = "user_id,slug,checkin,room,occupancy,price,hotel,job_id"
RAW_ALLOWED_KEYS = {
    k.strip()
    for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",")
    if k.strip()
}
if not RAW_INCLUDE_JOB_ID and "job_id" in RAW_ALLOWED_KEYS:
    RAW_ALLOWED_KEYS.remove("job_id")

# ------------------------------------------------------------------------------
# basic helpers
# ------------------------------------------------------------------------------

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
        url,
        params=params,
        headers=supabase_headers(json_pref=False),
        timeout=30,
    )
    r.raise_for_status()
    return r


def http_patch(
    path: str,
    params: Dict[str, Any],
    json_body: Dict[str, Any],
    prefer_return=False,
) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers()
    if prefer_return:
        headers["Prefer"] = "return=representation"
    r = requests.patch(
        url,
        params=params,
        json=json_body,
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return r


def http_post(
    path: str,
    params: Dict[str, Any],
    json_body: Any,
    upsert=False,
    prefer: Optional[str] = None,
    timeout_sec: int = 60,
) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers(upsert=upsert)
    if prefer:
        headers["Prefer"] = prefer
    r = requests.post(
        url,
        params=params,
        json=json_body,
        headers=headers,
        timeout=timeout_sec,
    )
    r.raise_for_status()
    return r


def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = requests.delete(
        url,
        params=params,
        headers=supabase_headers(json_pref=False),
        timeout=30,
    )
    r.raise_for_status()
    return r


# ------------------------------------------------------------------------------
# scrape_jobs helpers
# ------------------------------------------------------------------------------

def fetch_next_pending_job() -> Optional[dict]:
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
    """atomically flip pending→running"""
    try:
        r = http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}", "status": "eq.pending"},
            {"status": "running", "started_at": now_iso_z()},
            prefer_return=True,
        )
        rows = r.json() if r.text else []
        return rows[0] if rows else None
    except HTTPError as e:
        print("❌ claim_job error:", e, getattr(e.response, "text", "")[:400])
        return None


def set_job_fields(job_id: str, **patch):
    try:
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}"},
            patch,
            prefer_return=False,
        )
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


def is_canceled(job_id: str) -> bool:
    """check if job is canceled/failed, so we can bail early"""
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


# ------------------------------------------------------------------------------
# archiving + sold-out alert helpers
# ------------------------------------------------------------------------------

def archive_previous_snapshot(user_id: str, slug: str, checkin: str):
    """
    Call fn_archive_day_rows(user, slug, checkin) to snapshot current rows
    from room_prices_raw into room_prices_history *before* we overwrite them
    with new scrape results.
    """
    payload = {
        "p_user_id": user_id,
        "p_slug": slug,
        "p_checkin": checkin,
    }
    try:
        # call RPC via PostgREST
        r = http_post(
            f"/rest/v1/rpc/{HISTORY_FUNC}",
            {},
            payload,
            upsert=False,
            timeout_sec=30,
        )
        moved_count = int(r.json() or 0)
        print(
            f"📦 Archived {moved_count} old rows for {slug} {checkin} before upsert"
        )
    except HTTPError as e:
        # we don't fail the whole scrape if archive fails
        print(
            "⚠️ archive_previous_snapshot failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )


def ensure_soldout_marker(user_id: str, slug: str, checkin: str) -> bool:
    """
    Insert (user_id, slug, checkin) into soldout_markers.
    Return True if it's a NEW marker (first time we noticed sold-out),
    False if there was already one (=> now we should raise alert).
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
        # if rows[] is non-empty, we actually inserted a new row
        is_new = bool(rows)
        return is_new
    except HTTPError as e:
        print(
            "⚠️ ensure_soldout_marker failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )
        return False


def clear_soldout_marker(user_id: str, slug: str, checkin: str):
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
        # not fatal; usually means marker didn't exist
        print(
            "⚠️ clear_soldout_marker failed:",
            e,
            getattr(e.response, "text", "")[:200],
        )
    except Exception as e:
        print("⚠️ clear_soldout_marker error:", e)


def insert_soldout_alert(user_id: str, slug: str, checkin: str):
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
        # unique constraint (user_id,type,slug,checkin) might block dupes
        print(
            "⚠️ insert_soldout_alert failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )


# ------------------------------------------------------------------------------
# room price upsert
# ------------------------------------------------------------------------------

def _sanitize_raw_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Keep only whitelisted keys, coerce types, make sure required fields exist.
    """
    out: Dict[str, Any] = {}

    for k in RAW_ALLOWED_KEYS:
        if k in row:
            out[k] = row[k]

    # normalize slug
    if "slug" in out and isinstance(out["slug"], str):
        out["slug"] = out["slug"].strip().lower()

    # coerce checkin (YYYY-MM-DD)
    if "checkin" in out and isinstance(out["checkin"], str):
        out["checkin"] = out["checkin"][:10]

    # coerce occupancy
    if "occupancy" in out:
        try:
            out["occupancy"] = int(out["occupancy"])
        except Exception:
            out["occupancy"] = None

    # coerce price
    if "price" in out:
        try:
            out["price"] = float(out["price"])
        except Exception:
            out["price"] = None

    # we require these for the upsert uniqueness
    required_keys = ("user_id", "slug", "checkin", "room", "occupancy")
    for rk in required_keys:
        if not out.get(rk):
            return None

    # require hotel to satisfy NOT NULL in table
    if "hotel" in RAW_ALLOWED_KEYS and not out.get("hotel"):
        return None

    # drop row if price is missing, because we compare price changes
    if out.get("price") is None:
        return None

    return out


def upsert_room_prices(user_id: str, job_id: str, rows: List[dict]):
    """
    Chunked upsert to room_prices_raw using on_conflict on
    (user_id, slug, checkin, room, occupancy).
    """
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
        }
        if RAW_INCLUDE_JOB_ID:
            base["job_id"] = job_id

        s = _sanitize_raw_row(base)
        if s:
            clean.append(s)

    if not clean:
        return

    # batch insert to avoid 413s
    for i in range(0, len(clean), 400):
        batch = clean[i : i + 400]
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
            # log 1 sample
            print("   sample row:", batch[0])
            raise


# ------------------------------------------------------------------------------
# scrape task for a single (hotel, date)
# ------------------------------------------------------------------------------

def _scrape_once(
    job_id: str,
    user_id: str,
    hotel: Dict[str, Any],
    checkin: str,
    checkout: str,
) -> Dict[str, Any]:
    """
    Returns {slug, checkin, rows: [{hotel,slug,checkin,room,occupancy,price}] }
    rows is already de-duped (min per room/occ).
    """
    slug = (hotel.get("slug") or "").lower()
    if is_canceled(job_id):
        return {"slug": slug, "checkin": checkin, "rows": []}

    try:
        # figure out country code
        cc = (
            hotel.get("cc")
            or (resolve_cc_for_slug(slug) if RESOLVE_CC_IF_MISSING else None)
            or DEFAULT_CC
        )

        raw_rows = scrape_hotel_for_dates(
            hotel["name"],
            slug,
            cc,
            checkin,
            checkout,
            should_cancel=lambda: is_canceled(job_id),
        )

        if is_canceled(job_id):
            return {"slug": slug, "checkin": checkin, "rows": []}

        deduped = dedupe_min_per_room_and_occupancy(raw_rows)

        # make sure hotel name & user_id ride along downstream
        for r in deduped:
            if not r.get("hotel"):
                r["hotel"] = hotel["name"]
            r["user_id"] = user_id

        return {"slug": slug, "checkin": checkin, "rows": deduped}

    except Exception as e:
        print("⚠️ scrape task error:", e)
        return {"slug": slug, "checkin": checkin, "rows": []}


# ------------------------------------------------------------------------------
# main job processing
# ------------------------------------------------------------------------------

STEP_LOCK = threading.Lock()

def process_job(first_row: dict):
    job_id = first_row["id"]

    claimed = claim_job(job_id)
    if not claimed:
        print(
            f"↩️ Job {job_id} was already claimed or not pending anymore. Skip."
        )
        return

    job = claimed
    user_id = job["user_id"]
    range_days = int(job.get("range_days") or 1)
    start_date = dt.date.today()

    # figure out which hotels we scrape
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

    # optional per-job slug filter (always keep own)
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
        hotels = [
            h
            for h in hotels
            if h.get("own") or (h.get("slug") or "").lower() in job_slugs
        ]
        # dedupe by slug
        deduped = []
        seen = set()
        for h in hotels:
            slug = (h.get("slug") or "").lower()
            if slug and slug not in seen:
                seen.add(slug)
                deduped.append(h)
        hotels = deduped
        print(
            f"🔎 job slug filter: had {before}, now {len(hotels)} (own forced in)"
        )

    print(
        f"🚀 process_job {job_id} (user {user_id}) "
        f"| range_days={range_days} | hotels={len(hotels)}"
    )

    total_steps = len(hotels) * range_days
    set_job_fields(
        job_id,
        total_steps=(total_steps or None),
        completed_steps=0,
    )

    completed_steps = 0

    def canceled():
        return is_canceled(job_id)

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = []

            for h in hotels:
                if canceled():
                    break
                for d in range(range_days):
                    if canceled():
                        break

                    checkin = (start_date + dt.timedelta(days=d)).strftime(
                        "%Y-%m-%d"
                    )
                    checkout = (
                        start_date + dt.timedelta(days=d + 1)
                    ).strftime("%Y-%m-%d")

                    # progress hint
                    set_job_fields(
                        job_id,
                        meta={
                            "last_hotel": h["name"],
                            "last_checkin": checkin,
                            "own": bool(h.get("own")),
                        },
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

            # buffer upserts for performance
            buffer_rows: List[dict] = []
            buffer_lock = threading.Lock()

            for fut in as_completed(futures):
                if canceled():
                    break

                try:
                    result = fut.result() or {}
                    slug = (result.get("slug") or "").lower()
                    checkin = result.get("checkin")
                    rows = result.get("rows") or []

                    if rows:
                        # we got fresh data:
                        # 1) archive previous snapshot for this slug+day
                        if slug and checkin:
                            archive_previous_snapshot(user_id, slug, checkin)

                        # 2) stage rows for upsert
                        with buffer_lock:
                            for r in rows:
                                buffer_rows.append(r)

                            # flush in chunks
                            if len(buffer_rows) >= 300:
                                upsert_room_prices(user_id, job_id, buffer_rows)
                                # clear soldout marker because we HAVE prices now
                                try:
                                    for rr in buffer_rows:
                                        if rr.get("slug") and rr.get("checkin"):
                                            clear_soldout_marker(
                                                user_id,
                                                rr["slug"],
                                                rr["checkin"],
                                            )
                                except Exception as e:
                                    print(
                                        "⚠️ clear_soldout_marker batch err:",
                                        e,
                                    )
                                buffer_rows.clear()

                    else:
                        # 0 prices for this slug/date -> possible sold out
                        if slug and checkin:
                            first_time = ensure_soldout_marker(
                                user_id, slug, checkin
                            )
                            if not first_time:
                                # second consecutive time with no rooms
                                insert_soldout_alert(
                                    user_id, slug, checkin
                                )

                except Exception as e:
                    print("⚠️ parallel scrape step error:", e)
                    set_job_fields(job_id, last_error=str(e))

                finally:
                    # step progress
                    with STEP_LOCK:
                        completed_steps += 1
                        set_job_fields(
                            job_id,
                            completed_steps=completed_steps,
                        )

        # flush leftovers
        if buffer_rows:
            upsert_room_prices(user_id, job_id, buffer_rows)
            try:
                seen_pairs = set()
                for rr in buffer_rows:
                    key = (rr.get("slug"), rr.get("checkin"))
                    if key not in seen_pairs and rr.get("slug") and rr.get("checkin"):
                        clear_soldout_marker(
                            user_id,
                            rr["slug"],
                            rr["checkin"],
                        )
                        seen_pairs.add(key)
            except Exception as e:
                print("⚠️ final clear_soldout_marker err:", e)
            buffer_rows = []

        # finalize job
        if canceled():
            set_job_fields(
                job_id,
                status="canceled",
                finished_at=now_iso_z(),
            )
            print(f"🟠 job {job_id} canceled by user")
        else:
            set_job_fields(
                job_id,
                status="done",
                finished_at=now_iso_z(),
            )
            print(f"✅ job {job_id} done")

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


# ------------------------------------------------------------------------------
# main loop
# ------------------------------------------------------------------------------

def main():
    print("⏳ Worker online. Polling scrape_jobs...")
    while True:
        try:
            job = fetch_next_pending_job()
            if not job:
                time.sleep(3)
                continue
            print("🎯 picked job", job.get("id"))
            process_job(job)
        except HTTPError as http_err:
            print(
                "worker loop HTTP error:",
                http_err,
                getattr(http_err.response, "text", "")[:400],
            )
            time.sleep(3)
        except Exception as e:
            print("worker loop error:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
