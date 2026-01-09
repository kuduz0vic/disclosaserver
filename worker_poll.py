# worker_poll.py
# Hardened: per-step timeouts, HTTP retries, non-blocking sold-out cleanup,
# watchdog-based self-restart on hangs, and variant-aware via 'rate_key'.

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
# slug helpers (country-aware)
#  - Store slugs in DB as: "<booking_slug>__<cc>" when cc is known.
#  - Matches frontend selection and avoids cross-country collisions.
# ──────────────────────────────────────────────────────────────────────────────
def to_scrape_slug(base_slug: str, cc: str | None) -> str:
    base = (base_slug or "").strip().lower()
    cc_norm = (cc or "").strip().lower()
    return f"{base}__{cc_norm}" if cc_norm else base

def split_scrape_slug(scrape_slug: str) -> tuple[str, str | None]:
    s = (scrape_slug or "").strip().lower()
    if "__" in s:
        base, cc = s.split("__", 1)
        return base, (cc or None)
    return s, None


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
MAX_JOB_DURATION_SEC = int(os.getenv("MAX_JOB_DURATION_SEC", "5400"))
HEARTBEAT_EVERY_STEPS = int(os.getenv("HEARTBEAT_EVERY_STEPS", "8"))
STALE_JOB_MINUTES = int(os.getenv("STALE_JOB_MINUTES", "90"))

HOTEL_STEP_TIMEOUT_SEC = int(os.getenv("HOTEL_STEP_TIMEOUT_SEC", "120"))

# If we don't see ANY step finish for this long, assume Playwright is hung and
# hard-exit the process so the platform restarts the service.
NO_PROGRESS_KILL_SEC = int(os.getenv("NO_PROGRESS_KILL_SEC", "1800"))

HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_BASE_SLEEP = float(os.getenv("RETRY_BASE_SLEEP", "0.6"))

JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
HISTORY_FUNC = "fn_archive_day_rows"
SOLDOUT_TABLE = "soldout_markers"
ALERTS_TABLE = "alert_events"
RAW_ON_CONFLICT = os.getenv("RAW_ON_CONFLICT", "user_id,slug,checkin,room,occupancy,rate_key")

RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"

# IMPORTANT: allow-list the exact columns we upsert (includes 'rate_key' for variants)
_raw_allowed_default = ",".join([
    "user_id","slug","checkin","room","occupancy","price","hotel","job_id",
    "breakfast_included","dinner_included","half_board",
    "free_cancellation","nonrefundable","prepay_required","rate_plan",
    "rate_key",
])
RAW_ALLOWED_KEYS = {
    k.strip()
    for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",")
    if k.strip()
}
if not RAW_INCLUDE_JOB_ID and "job_id" in RAW_ALLOWED_KEYS:
    RAW_ALLOWED_KEYS.remove("job_id")

# 🔒 Ensure rate_key is ALWAYS included, even if env RAW_ALLOWED_KEYS overrides it.
RAW_ALLOWED_KEYS.add("rate_key")

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (short timeouts + retries)
# ──────────────────────────────────────────────────────────────────────────────
def now_iso_z() -> str:
    return (dt.datetime.now(timezone.utc).replace(microsecond=0)
            .isoformat().replace("+00:00", "Z"))

def _req_with_retry(method, path, *, params=None, json_body=None, headers=None, timeout=None):
    url = f"{SUPABASE_URL}{path}"
    for attempt in range(HTTP_RETRIES + 1):
        try:
            r = requests.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers or supabase_headers(),
                timeout=timeout or HTTP_TIMEOUT_SEC,
            )
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt >= HTTP_RETRIES:
                raise
            sleep = RETRY_BASE_SLEEP * (2 ** attempt) * (1 + 0.25 * random.random())
            print(f"🌐 retrying {method} {path} in {sleep:.2f}s (attempt {attempt+1}/{HTTP_RETRIES}) due to {type(e).__name__}")
            time.sleep(sleep)

def http_get(path: str, params: Dict[str, Any]) -> requests.Response:
    return _req_with_retry("GET", path, params=params, headers=supabase_headers(json_pref=False))

def http_patch(path: str, params: Dict[str, Any], json_body: Dict[str, Any], prefer_return=False) -> requests.Response:
    h = supabase_headers()
    if prefer_return:
        h["Prefer"] = "return=representation"
    return _req_with_retry("PATCH", path, params=params, json_body=json_body, headers=h)

def http_post(path: str, params: Dict[str, Any], json_body: Any, upsert=False, prefer: Optional[str] = None, timeout_sec: int = None) -> requests.Response:
    h = supabase_headers(upsert=upsert)
    if prefer:
        h["Prefer"] = prefer
    return _req_with_retry("POST", path, params=params, json_body=json_body, headers=h, timeout=timeout_sec or HTTP_TIMEOUT_SEC)

def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    return _req_with_retry("DELETE", path, params=params, headers=supabase_headers(json_pref=False))

# ──────────────────────────────────────────────────────────────────────────────
# jobs helpers
# ──────────────────────────────────────────────────────────────────────────────
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
            {"status": "running", "started_at": now_iso_z(), "heartbeat_at": now_iso_z()},
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
        rows = r.json()
        if not rows:
            return True
        status = (rows[0].get("status") or "").lower()
        return status in ("canceled", "failed")
    except HTTPError as e:
        print("⚠️ is_canceled check failed:", e, getattr(e.response, "text", "")[:200])
        return True

def reap_stale_jobs():
    try:
        cutoff = (
            dt.datetime.now(timezone.utc)
            - dt.timedelta(minutes=STALE_JOB_MINUTES)
        ).isoformat().replace("+00:00", "Z")
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"status": "eq.running", "or": f"(heartbeat_at.is.null,heartbeat_at.lt.{cutoff})"},
            {"status": "failed", "finished_at": now_iso_z(), "last_error": "stale (reaped)"},
        )
    except HTTPError as e:
        print("⚠️ stale reaper error:", e, getattr(e.response, "text", "")[:400])

# ──────────────────────────────────────────────────────────────────────────────
# archiving + sold-out helpers (each isolated)
# ──────────────────────────────────────────────────────────────────────────────
def archive_previous_snapshot(user_id: str, slug: str, checkin: str):
    try:
        r = http_post(
            f"/rest/v1/rpc/{HISTORY_FUNC}",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=20,
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
    try:
        r = http_post(
            "/rest/v1/rpc/fn_delete_day_rows",
            {},
            {"p_user_id": user_id, "p_slug": slug, "p_checkin": checkin},
            upsert=False,
            timeout_sec=20,
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
    try:
        r = http_post(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"on_conflict": "user_id,slug,checkin"},
            [{"user_id": user_id, "slug": slug.lower(), "checkin": checkin}],
            upsert=True,
            prefer="resolution=ignore-duplicates,return=representation",
            timeout_sec=20,
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
            timeout_sec=20,
        )
        print(f"🔔 ALERT AUTO_SOLD_OUT for {slug} {checkin} (user={user_id})")
    except HTTPError as e:
        print(
            "⚠️ insert_soldout_alert failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )

# ──────────────────────────────────────────────────────────────────────────────
# upsert to RAW (flags included, variant-aware via rate_key)
# ──────────────────────────────────────────────────────────────────────────────
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

    # ✅ rate_key is NOT NULL in DB. Always send a string ("" is fine).
    rk = out.get("rate_key", "")
    if rk is None:
        rk = ""
    out["rate_key"] = str(rk)

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
            # flags:
            "breakfast_included": r.get("breakfast_included"),
            "dinner_included": r.get("dinner_included"),
            "half_board": r.get("half_board"),
            "free_cancellation": r.get("free_cancellation"),
            "nonrefundable": r.get("nonrefundable"),
            "prepay_required": r.get("prepay_required"),
            "rate_plan": r.get("rate_plan"),
            # ✅ always present
            "rate_key": r.get("rate_key") if r.get("rate_key") is not None else "",
        }
        if RAW_INCLUDE_JOB_ID:
            base["job_id"] = job_id
        s = _sanitize_raw_row(base)
        if s:
            clean.append(s)
    if not clean:
        return

    # dedupe within batch by (user, slug, date, room, occ, rate_key) -> keep cheaper
    uniq: Dict[tuple, Dict[str, Any]] = {}
    for r in clean:
        key = (r["user_id"], r["slug"], r["checkin"], r["room"], r["occupancy"], r.get("rate_key") or "")
        if key not in uniq or r["price"] < uniq[key]["price"]:
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
                timeout_sec=30,
            )
            print(f"✅ Upserted {len(batch)} rows into room_prices_raw")
        except HTTPError as e:
            print("❌ upsert_room_prices error:", e, getattr(e.response, "text", "")[:400])
            if batch:
                print("   sample row:", batch[0])
            raise

# ──────────────────────────────────────────────────────────────────────────────
# scrape step wrapper
# ──────────────────────────────────────────────────────────────────────────────
STEP_LOCK = threading.Lock()

def _scrape_once(job_id: str, user_id: str, hotel: Dict[str, Any], checkin: str, checkout: str) -> Dict[str, Any]:
    # hotel['slug'] may be either a plain booking slug or a "<slug>__<cc>" scrape slug
    stored_slug = (hotel.get("scrape_slug") or hotel.get("slug") or "").strip().lower()
    base_slug, cc_from_slug = split_scrape_slug(stored_slug)

    try:
        cc = (
            (hotel.get("cc") or "").strip().lower()
            or (cc_from_slug or "")
            or (resolve_cc_for_slug(base_slug) if RESOLVE_CC_IF_MISSING else None)
            or DEFAULT_CC
        )

        # Persist as "<slug>__<cc>" so selection + DB are consistent
        stored_slug = to_scrape_slug(base_slug, cc)

        raw_rows = scrape_hotel_for_dates(
            hotel["name"],
            base_slug,
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
            r["slug"] = stored_slug
            # ✅ make sure every row has a non-null rate_key
            if r.get("rate_key") is None:
                r["rate_key"] = ""
        return {"slug": stored_slug, "checkin": checkin, "rows": deduped}
    except Exception as e:
        print("⚠️ scrape task error:", e)
        return {"slug": stored_slug, "checkin": checkin, "rows": []}

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
        print(f"🧽 Cleared sold-out marker for {slug} {checkin}")
    except HTTPError as e:
        print(
            "⚠️ clear_soldout_marker failed:",
            e,
            getattr(e.response, "text", "")[:400],
        )
    except Exception as e:
        print("⚠️ clear_soldout_marker error:", e)

# ──────────────────────────────────────────────────────────────────────────────
# main job loop
# ──────────────────────────────────────────────────────────────────────────────
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

    own = get_own_hotel(user_id)
    own_id = own["hotel_id"] if own else None
    competitors = get_competitor_hotels(user_id, own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append(
            {"name": own["name"], "slug": own["slug"], "cc": (own.get("cc") or None), "scrape_slug": to_scrape_slug(own["slug"], (own.get("cc") or DEFAULT_CC)), "own": True}
        )
    for c in competitors:
        hotels.append(
            {"name": c["name"], "slug": c["slug"], "cc": (c.get("cc") or None), "scrape_slug": to_scrape_slug(c["slug"], (c.get("cc") or DEFAULT_CC)), "own": False}
        )

    # optional job slug filter
    # IMPORTANT: be tolerant of old UI values that send just "slug" (no cc)
    # and newer values that send "slug__cc".
    job_slug_base: set[str] = set()
    job_slug_full: set[str] = set()
    try:
        raw_slugs = (job.get("meta") or {}).get("slugs") or []
        if isinstance(raw_slugs, list):
            for s in raw_slugs:
                if not isinstance(s, str):
                    continue
                base, cc = split_scrape_slug(s.strip().lower())
                if base:
                    job_slug_base.add(base)
                    if cc:
                        job_slug_full.add(to_scrape_slug(base, cc))
    except Exception:
        pass

    def _matches_job_slugs(h: Dict[str, Any]) -> bool:
        base = (h.get("slug") or "").strip().lower()
        cc = (h.get("cc") or DEFAULT_CC).strip().lower() if (h.get("cc") or DEFAULT_CC) else ""
        full = to_scrape_slug(base, cc) if base else ""
        # match if either exact full key matches OR base matches (old jobs)
        if full and full in job_slug_full:
            return True
        if base and base in job_slug_base:
            return True
        return False

    if job_slug_base or job_slug_full:
        before = len(hotels)
        seen, deduped_hotels = set(), []
        for h in hotels:
            base = (h.get("slug") or "").strip().lower()
            cc = (h.get("cc") or DEFAULT_CC).strip().lower() if (h.get("cc") or DEFAULT_CC) else ""
            full = to_scrape_slug(base, cc) if base else ""

            if h.get("own") or _matches_job_slugs(h):
                # dedupe by full key (or base if no full)
                key = full or base
                if key and key not in seen:
                    seen.add(key)
                    # keep scrape_slug aligned with cc
                    h["scrape_slug"] = full or base
                    deduped_hotels.append(h)
        hotels = deduped_hotels
        print(f"🔎 job slug filter: had {before}, now {len(hotels)} (own forced in)")

    if not hotels:
        print(f"❌ job {job_id} has no hotels to scrape for user {user_id}")
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
    set_job_fields(job_id, total_steps=(total_steps or None), completed_steps=0)

    completed_steps = 0
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = []
            for h in hotels:
                for d in range(range_days):
                    checkin = (start_date + dt.timedelta(days=d)).strftime("%Y-%m-%d")
                    checkout = (start_date + dt.timedelta(days=d + 1)).strftime(
                        "%Y-%m-%d"
                    )
                    futures.append(
                        pool.submit(
                            _scrape_once, job_id, user_id, h, checkin, checkout
                        )
                    )

            buffer_rows: List[dict] = []
            buffer_lock = threading.Lock()

            pending = set(futures)
            last_progress = time.time()

            while pending:
                if canceled():
                    break
                if over_time_budget():
                    set_job_fields(
                        job_id,
                        status="failed",
                        finished_at=now_iso_z(),
                        last_error="time budget exceeded",
                    )
                    print(f"⏱️ job {job_id} exceeded time budget; failing")
                    return

                done, pending = wait(pending, timeout=15, return_when=FIRST_COMPLETED)
                if not done:
                    if (time.time() - last_progress) > NO_PROGRESS_KILL_SEC:
                        try:
                            set_job_fields(
                                job_id,
                                status="failed",
                                finished_at=now_iso_z(),
                                last_error=f"worker hang: no progress for {NO_PROGRESS_KILL_SEC}s",
                            )
                        except Exception:
                            pass
                        print(f"🧨 No progress for {NO_PROGRESS_KILL_SEC}s -> exiting worker so platform restarts")
                        os._exit(2)
                    continue

                for fut in done:
                    last_progress = time.time()

                    if canceled():
                        break
                    if over_time_budget():
                        set_job_fields(
                            job_id,
                            status="failed",
                            finished_at=now_iso_z(),
                            last_error="time budget exceeded",
                        )
                        print(f"⏱️ job {job_id} exceeded time budget; failing")
                        return

                    try:
                        result = fut.result()
                    except Exception as e:
                        print("⏳ step error:", type(e).__name__, e)
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(
                                job_id,
                                completed_steps=completed_steps,
                                last_error="step error",
                            )
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)
                        continue

                    try:
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
                            if slug and checkin:
                                try:
                                    archive_previous_snapshot(user_id, slug, checkin)
                                except Exception as e:
                                    print("⚠️ archive_previous_snapshot err:", e)
                                try:
                                    delete_current_snapshot(user_id, slug, checkin)
                                except Exception as e:
                                    print("⚠️ delete_current_snapshot err:", e)
                                try:
                                    ensure_soldout_marker(user_id, slug, checkin)
                                except Exception as e:
                                    print("⚠️ ensure_soldout_marker err:", e)
                                try:
                                    insert_soldout_alert(user_id, slug, checkin)
                                except Exception as e:
                                    print("⚠️ insert_soldout_alert err:", e)

                    except Exception as e:
                        print("⚠️ parallel scrape step error:", e)
                        set_job_fields(job_id, last_error=str(e))
                    finally:
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(job_id, completed_steps=completed_steps)
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)

        if buffer_rows:
            try:
                upsert_room_prices(user_id, job_id, buffer_rows)
            except Exception as e:
                print("⚠️ final upsert buffer err:", e)
            seen_pairs = set()
            for rr in buffer_rows:
                key = (rr.get("slug"), rr.get("checkin"))
                if key not in seen_pairs and rr.get("slug") and rr.get("checkin"):
                    clear_soldout_marker(user_id, rr["slug"], rr["checkin"])
                    seen_pairs.add(key)

        set_heartbeat(job_id)
        if canceled():
            set_job_fields(job_id, status="canceled", finished_at=now_iso_z())
        else:
            set_job_fields(job_id, status="done", finished_at=now_iso_z())

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
