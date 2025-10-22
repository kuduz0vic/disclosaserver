# worker_poll.py
# --------------------------------------------------------------------------
# Railway worker: polls scrape_jobs, scrapes (adults 1..4), then
# ARCHIVE+REPLACE per (user_id, slug, checkin) so stale occupancies disappear.
#
# Sold-out debounce:
#   - 1st consecutive miss: create soldout_markers row (no delete yet)
#   - 2nd consecutive miss: emit AUTO_SOLD_OUT and archive+delete stale rows
#
# Also clears soldout markers when fresh rows appear again.
# --------------------------------------------------------------------------

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

# ---- DB tables ----
JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
ALERTS_TABLE = "alert_events"
SOLDOUT_TABLE = "soldout_markers"

# IMPORTANT: include occupancy to avoid mixing rates
RAW_ON_CONFLICT = "user_id,slug,checkin,room,occupancy"

# Whitelist for room_prices_raw
_raw_allowed_default = "user_id,slug,checkin,room,occupancy,price,hotel"
RAW_ALLOWED_KEYS = {k.strip() for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",") if k.strip()}
RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"
if RAW_INCLUDE_JOB_ID:
    RAW_ALLOWED_KEYS.add("job_id")


# ────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ────────────────────────────────────────────────────────────────────────────
def now_iso_z() -> str:
    return dt.datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_get(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=30)
    r.raise_for_status()
    return r


def http_patch(path: str, params: Dict[str, Any], json_body: Dict[str, Any], prefer_return=False) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers()
    if prefer_return:
        headers["Prefer"] = "return=representation"
    r = requests.patch(url, params=params, json=json_body, headers=headers, timeout=30)
    r.raise_for_status()
    return r


def http_post(path: str, params: Dict[str, Any], json_body: Any, upsert=False, prefer: Optional[str] = None) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers(upsert=upsert)
    if prefer:
        headers["Prefer"] = prefer
    r = requests.post(url, params=params, json=json_body, headers=headers, timeout=60)
    r.raise_for_status()
    return r


def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = requests.delete(url, params=params, headers=supabase_headers(json_pref=False), timeout=30)
    r.raise_for_status()
    return r


# ────────────────────────────────────────────────────────────────────────────
# Jobs
# ────────────────────────────────────────────────────────────────────────────
def fetch_next_pending_job() -> Optional[dict]:
    r = http_get(
        f"/rest/v1/{JOBS_TABLE}",
        {"select": "*", "status": "eq.pending", "order": "created_at.asc", "limit": 1},
    )
    rows = r.json()
    return rows[0] if rows else None


def claim_job(job_id: str) -> Optional[dict]:
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
        http_patch(f"/rest/v1/{JOBS_TABLE}", {"id": f"eq.{job_id}"}, patch, prefer_return=False)
    except HTTPError as e:
        print("❌ set_job_fields error:", e, "| payload:", patch, "| resp:", getattr(e.response, "text", "")[:400])
        raise


def is_canceled(job_id: str) -> bool:
    try:
        r = http_get(f"/rest/v1/{JOBS_TABLE}", {"select": "status", "id": f"eq.{job_id}", "limit": 1})
        rows = r.json()
        if not rows:
            return True
        status = (rows[0].get("status") or "").lower()
        return status in ("canceled", "failed")
    except HTTPError as e:
        print("⚠️ is_canceled check failed:", e, getattr(e.response, "text", "")[:200])
        return True


# ────────────────────────────────────────────────────────────────────────────
# Archive + replace helpers (per slug/checkin)
# ────────────────────────────────────────────────────────────────────────────
def archive_day_rows(user_id: str, slug: str, checkin: str) -> int:
    """Calls SQL fn fn_archive_day_rows(user_id, slug, checkin). Returns moved row count."""
    try:
        r = http_post(
            "/rest/v1/rpc/fn_archive_day_rows",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
        )
        # Supabase RPC returns either a scalar or a JSON with 'count'
        try:
            val = r.json()
            if isinstance(val, int):
                return val
            if isinstance(val, dict) and "fn_archive_day_rows" in val:
                return int(val["fn_archive_day_rows"])
        except Exception:
            pass
        return 0
    except HTTPError as e:
        print("⚠️ archive_day_rows failed:", e, getattr(e.response, "text", "")[:400])
        return 0


def delete_day_rows(user_id: str, slug: str, checkin: str):
    """Delete current rows for (user, slug, checkin) from room_prices_raw."""
    try:
        http_delete(
            f"/rest/v1/{RAW_TABLE}",
            {"user_id": f"eq.{user_id}", "slug": f"eq.{slug}", "checkin": f"eq.{checkin}"},
        )
    except HTTPError as e:
        txt = getattr(e.response, "text", "")[:200]
        print("⚠️ delete_day_rows failed:", e, txt)


def clear_soldout_marker(user_id: str, slug: str, checkin: str):
    try:
        http_delete(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"user_id": f"eq.{user_id}", "slug": f"eq.{slug}", "checkin": f"eq.{checkin}"},
        )
    except HTTPError:
        pass


def ensure_soldout_marker(user_id: str, slug: str, checkin: str) -> bool:
    """Returns True iff a NEW marker was inserted (i.e., first consecutive miss)."""
    try:
        r = http_post(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"on_conflict": "user_id,slug,checkin"},
            [{"user_id": user_id, "slug": slug, "checkin": checkin}],
            upsert=True,
            prefer="resolution=ignore-duplicates,return=representation",
        )
        rows = r.json() if r.text else []
        return bool(rows)
    except HTTPError as e:
        print("⚠️ ensure_soldout_marker failed:", e, getattr(e.response, "text", "")[:400])
        return False


def insert_soldout_alert(user_id: str, slug: str, checkin: str):
    payload = {
        "type": "AUTO_SOLD_OUT",
        "user_id": user_id,
        "slug": slug,
        "checkin": checkin,
        "price": None,
        "payload": {"source": "worker_poll", "reason": "no rooms parsed for any occupancy"},
    }
    try:
        http_post(f"/rest/v1/{ALERTS_TABLE}", {}, payload, upsert=False)
        print(f"🔔 ALERT: SOLD OUT emitted for user={user_id} slug={slug} date={checkin}")
    except HTTPError as e:
        print("⚠️ insert_soldout_alert failed:", e, getattr(e.response, "text", "")[:400])


# ────────────────────────────────────────────────────────────────────────────
# Upsert helper
# ────────────────────────────────────────────────────────────────────────────
def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in RAW_ALLOWED_KEYS:
        if k in row:
            out[k] = row[k]

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

    if "slug" in out and isinstance(out["slug"], str):
        out["slug"] = out["slug"].lower()

    if out.get("price") is None:
        out.pop("price", None)

    return out


def insert_room_level(user_id: str, rows: List[dict]):
    """Insert rows (no upsert) after we've deleted the old snapshot."""
    if not rows:
        return

    clean_rows: List[Dict[str, Any]] = []
    for r in rows:
        base = {
            "user_id": user_id,
            "slug": (r.get("slug") or ""),
            "checkin": r.get("checkin"),
            "room": r.get("room"),
            "occupancy": r.get("occupancy"),
            "price": r.get("price"),
            "hotel": r.get("hotel") or r.get("hotel_name") or r.get("hotelTitle"),
        }
        if RAW_INCLUDE_JOB_ID and "job_id" in r:
            base["job_id"] = r["job_id"]

        s = _sanitize_row(base)

        required = ("user_id", "slug", "checkin", "room", "occupancy", "price", "hotel")
        if not all(s.get(k) for k in required):
            continue

        clean_rows.append(s)

    if not clean_rows:
        return

    for i in range(0, len(clean_rows), 400):
        batch = clean_rows[i : i + 400]
        http_post(f"/rest/v1/{RAW_TABLE}", {}, batch, upsert=False)
        print(f"✅ Inserted {len(batch)} rows.")


# ────────────────────────────────────────────────────────────────────────────
# Worker processing
# ────────────────────────────────────────────────────────────────────────────
STEP_LOCK = threading.Lock()


def _scrape_task(job_id: str, user_id: str, hotel: Dict[str, Any], checkin: str, checkout: str) -> Dict[str, Any]:
    if is_canceled(job_id):
        return {"slug": hotel["slug"], "checkin": checkin, "rows": []}
    try:
        cc = hotel.get("cc") or (resolve_cc_for_slug(hotel["slug"]) if RESOLVE_CC_IF_MISSING else None) or DEFAULT_CC
        raw = scrape_hotel_for_dates(
            hotel["name"], hotel["slug"], cc, checkin, checkout, should_cancel=lambda: is_canceled(job_id)
        )
        if is_canceled(job_id):
            return {"slug": hotel["slug"], "checkin": checkin, "rows": []}

        room_level = dedupe_min_per_room_and_occupancy(raw)
        for r in room_level:
            r["hotel"] = r.get("hotel") or hotel["name"]
            r["user_id"] = user_id
            if RAW_INCLUDE_JOB_ID:
                r["job_id"] = job_id

        return {"slug": hotel["slug"], "checkin": checkin, "rows": room_level}
    except Exception as e:
        print("⚠️ task error:", e)
        return {"slug": hotel["slug"], "checkin": checkin, "rows": []}


def process_job(initial_job_row: dict):
    job_id = initial_job_row["id"]

    claimed = claim_job(job_id)
    if not claimed:
        print(f"↩️  Job {job_id} is no longer pending; another worker took it. Skipping.")
        return

    job = claimed
    user_id = job["user_id"]
    range_days = int(job.get("range_days") or 0) or 1
    start_date = dt.date.today()

    own = get_own_hotel(user_id)
    own_id = own["hotel_id"] if own else None
    competitors = get_competitor_hotels(user_id, own_hotel_id=own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append({"name": own["name"], "slug": own["slug"], "cc": own.get("cc"), "own": True})
    for c in competitors:
        hotels.append({"name": c["name"], "slug": c["slug"], "cc": c.get("cc"), "own": False})

    try:
        job_slugs = {s.lower() for s in ((job.get("meta") or {}).get("slugs") or []) if isinstance(s, str)}
    except Exception:
        job_slugs = set()
    if job_slugs:
        before = len(hotels)
        hotels = [h for h in hotels if h.get("own") or (h.get("slug") or "").lower() in job_slugs]
        seen = set()
        deduped = []
        for h in hotels:
            s = (h.get("slug") or "").lower()
            if s and s not in seen:
                seen.add(s)
                deduped.append(h)
        hotels = deduped
        print(f"🔎 Slug filter active ({len(job_slugs)}). {before}→{len(hotels)} (own forced in).")

    print(
        f"🚀 Processing job {job_id} | user {user_id} | range_days={range_days}\n"
        f"🏨 Own present: {bool(own)} | Competitors: {len(competitors)} | Scraping: {len(hotels)} hotels"
    )

    total_steps = len(hotels) * range_days
    set_job_fields(job_id, total_steps=(total_steps or None), completed_steps=0)

    completed = 0

    def should_cancel() -> bool:
        return is_canceled(job_id)

    try:
        tasks = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for h in hotels:
                if should_cancel():
                    break
                for i in range(range_days):
                    if should_cancel():
                        break
                    checkin = (start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d")
                    checkout = (start_date + dt.timedelta(days=i + 1)).strftime("%Y-%m-%d")
                    set_job_fields(job_id, meta={"last_hotel": h["name"], "last_checkin": checkin, "own": bool(h.get("own"))})
                    tasks.append(pool.submit(_scrape_task, job_id, user_id, h, checkin, checkout))

            for fut in as_completed(tasks):
                if should_cancel():
                    break
                try:
                    result = fut.result() or {}
                    rows = result.get("rows") or []
                    slug = (result.get("slug") or "").lower()
                    checkin = result.get("checkin")

                    if not slug or not checkin:
                        continue

                    if rows and len(rows) > 0:
                        # Fresh data → archive + delete + insert, clear soldout marker
                        archive_day_rows(user_id, slug, checkin)
                        delete_day_rows(user_id, slug, checkin)
                        insert_room_level(user_id, rows)
                        clear_soldout_marker(user_id, slug, checkin)
                    else:
                        # No rows → debounce SOLD_OUT; on second miss, archive+delete stale rows
                        is_first_miss = ensure_soldout_marker(user_id, slug, checkin)
                        if not is_first_miss:
                            insert_soldout_alert(user_id, slug, checkin)
                            archive_day_rows(user_id, slug, checkin)
                            delete_day_rows(user_id, slug, checkin)

                except Exception as e:
                    print("⚠️ Parallel step error:", e)
                    set_job_fields(job_id, last_error=str(e))
                finally:
                    with STEP_LOCK:
                        completed += 1
                        set_job_fields(job_id, completed_steps=completed)

        if should_cancel():
            set_job_fields(job_id, status="canceled", finished_at=now_iso_z())
            print(f"🟠 Job {job_id} marked canceled.")
        else:
            set_job_fields(job_id, status="done", finished_at=now_iso_z())
            print(f"✅ Job {job_id} done.")

    except HTTPError as http_err:
        print("💥 HTTP error during process_job:", http_err, getattr(http_err.response, "text", "")[:400])
        try:
            set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error=str(http_err))
        except Exception:
            pass
    except Exception as fatal:
        print("💥 Fatal error:", fatal)
        try:
            set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error=str(fatal))
        except Exception:
            pass


# ────────────────────────────────────────────────────────────────────────────
# Main loop
# ────────────────────────────────────────────────────────────────────────────
def main():
    print("⏳ Worker started. Polling for jobs…")
    while True:
        try:
            job = fetch_next_pending_job()
            if not job:
                time.sleep(3)
                continue
            print("🎯 Picked pending job:", job.get("id"))
            process_job(job)
        except HTTPError as http_err:
            print("Worker loop HTTP error:", http_err, getattr(http_err.response, "text", "")[:400])
            time.sleep(3)
        except Exception as e:
            print("Worker loop error:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
