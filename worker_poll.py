# worker_poll.py
# Option B: property_id isolation in room_prices_raw (worker stamps property_id on every row)
# Also respects job.meta.slugs selection (no forced own)

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
    _normalize_slug,
    resolve_cc_for_slug,
    RESOLVE_CC_IF_MISSING,
    DEFAULT_CC,
    scrape_hotel_for_dates_with_status,
    dedupe_min_per_room_and_occupancy,
    build_rate_key,
)

# ──────────────────────────────────────────────────────────────────────────────
# slug helpers (country-aware)
# ──────────────────────────────────────────────────────────────────────────────

def to_scrape_slug(base_slug: str, cc: Optional[str]) -> str:
    base = (base_slug or "").strip().lower()
    cc_norm = (cc or "").strip().lower()
    return f"{base}__{cc_norm}" if cc_norm else base


def split_scrape_slug(scrape_slug: str) -> tuple[str, Optional[str]]:
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
NO_PROGRESS_KILL_SEC = int(os.getenv("NO_PROGRESS_KILL_SEC", "1800"))
# Liveness: the main thread touches this file every loop; the Docker healthcheck reads
# it, and the watchdog thread exits the process (-> restart policy) when it goes stale.
LIVENESS_FILE = os.getenv("LIVENESS_FILE", "/tmp/worker-alive")
LIVENESS_MAX_AGE_SEC = int(os.getenv("LIVENESS_MAX_AGE_SEC", "900"))

HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_BASE_SLEEP = float(os.getenv("RETRY_BASE_SLEEP", "0.6"))

JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
HISTORY_FUNC = "fn_archive_day_rows"
SOLDOUT_TABLE = "soldout_markers"
ALERTS_TABLE = "alert_events"

# IMPORTANT: after Option B migration, unique should be:
# (user_id, property_id, slug, checkin, room, occupancy, rate_key)
RAW_ON_CONFLICT = "user_id,property_id,slug,checkin,room,occupancy,rate_key"
RAW_INCLUDE_JOB_ID = os.getenv("RAW_INCLUDE_JOB_ID", "0") == "1"

_raw_allowed_default = ",".join([
    "user_id","property_id","slug","checkin","room","occupancy","price","hotel","job_id",
    "breakfast_included","dinner_included","half_board",
    "free_cancellation","nonrefundable","prepay_required","rate_plan",
    "rate_key",
])
RAW_ALLOWED_KEYS = {k.strip() for k in os.getenv("RAW_ALLOWED_KEYS", _raw_allowed_default).split(",") if k.strip()}
if not RAW_INCLUDE_JOB_ID and "job_id" in RAW_ALLOWED_KEYS:
    RAW_ALLOWED_KEYS.remove("job_id")

# Force-include rate_key because variant storage depends on it.
RAW_ALLOWED_KEYS.add("rate_key")
RAW_ALLOWED_KEYS.add("property_id")

if os.getenv("DEBUG_WORKER_CONFIG", "0") == "1":
    print("🧩 RAW_ON_CONFLICT:", RAW_ON_CONFLICT)
    print("🧩 RAW_ALLOWED_KEYS:", ",".join(sorted(RAW_ALLOWED_KEYS)))


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (short timeouts + retries)
# ──────────────────────────────────────────────────────────────────────────────

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
                headers=headers or supabase_headers(),
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
    return _req_with_retry(
        "GET", path, params=params, headers=supabase_headers(json_pref=False)
    )


def http_patch(
    path: str, params: Dict[str, Any], json_body: Dict[str, Any], prefer_return=False
) -> requests.Response:
    h = supabase_headers()
    if prefer_return:
        h["Prefer"] = "return=representation"
    return _req_with_retry("PATCH", path, params=params, json_body=json_body, headers=h)


def http_post(
    path: str,
    params: Dict[str, Any],
    json_body: Any,
    upsert=False,
    prefer: Optional[str] = None,
    timeout_sec: int = None,
) -> requests.Response:
    h = supabase_headers(upsert=upsert)
    if prefer:
        h["Prefer"] = prefer
    return _req_with_retry(
        "POST",
        path,
        params=params,
        json_body=json_body,
        headers=h,
        timeout=timeout_sec or HTTP_TIMEOUT_SEC,
    )


def http_delete(path: str, params: Dict[str, Any]) -> requests.Response:
    return _req_with_retry(
        "DELETE", path, params=params, headers=supabase_headers(json_pref=False)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Fetch hotels (property-scoped via user_hotels.property_id)
# ──────────────────────────────────────────────────────────────────────────────

def _get_user_links(user_id: str, property_id: str) -> List[dict]:
    # ✅ property-scoped links
    r = http_get(
        "/rest/v1/user_hotels",
        {
            "select": "hotel_id,link_type,inserted_at,property_id",
            "user_id": f"eq.{user_id}",
            "property_id": f"eq.{property_id}",
            "order": "inserted_at.asc",
        },
    )
    links = r.json() or []
    if not links:
        return []

    hotel_ids = [x.get("hotel_id") for x in links if x.get("hotel_id")]
    if not hotel_ids:
        return []

    rh = http_get(
        "/rest/v1/hotels",
        {"select": "id,name,url", "id": f"in.({','.join(hotel_ids)})"},
    )
    hotels = {h["id"]: h for h in (rh.json() or [])}

    profs: Dict[str, dict] = {}
    try:
        rp = http_get(
            "/rest/v1/hotel_profiles",
            {
                "select": "hotel_id,booking_slug,booking_cc,updated_at",
                "hotel_id": f"in.({','.join(hotel_ids)})",
                "order": "updated_at.desc.nullslast",
            },
        )
        for p in (rp.json() or []):
            hid = p.get("hotel_id")
            if hid and hid not in profs:
                profs[hid] = p
    except Exception:
        profs = {}

    out: List[dict] = []
    for row in links:
        hid = row.get("hotel_id")
        h = hotels.get(hid) or {}
        p = profs.get(hid) or {}
        out.append(
            {
                "hotel_id": hid,
                "link_type": (row.get("link_type") or "").strip(),
                "inserted_at": row.get("inserted_at"),
                "name": (h.get("name") or "").strip(),
                "url": (h.get("url") or "").strip(),
                "booking_slug": (p.get("booking_slug") or "").strip(),
                "booking_cc": (p.get("booking_cc") or "").strip().lower() or "",
            }
        )
    return out


def get_own_hotel(user_id: str, property_id: str):
    links = _get_user_links(user_id, property_id)
    if not links:
        return None
    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {
                    "hotel_id": l["hotel_id"],
                    "name": l["name"] or "My Hotel",
                    "slug": slug,
                    "cc": cc,
                }
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {
        "hotel_id": first["hotel_id"],
        "name": first["name"] or "My Hotel",
        "slug": slug,
        "cc": cc,
    }


def get_competitor_hotels(user_id: str, property_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id, property_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l["hotel_id"] == own_hotel_id:
            continue
        if l.get("link_type") != "competitor":
            continue
        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append(
            {
                "hotel_id": l["hotel_id"],
                "name": l["name"] or "",
                "slug": slug,
                "cc": cc,
            }
        )
    return out


# ──────────────────────────────────────────────────────────────────────────────
# jobs helpers
# ──────────────────────────────────────────────────────────────────────────────

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
        print("❌ claim_job error:", e, getattr(e.response, "text", "")[:400])
        return None


def set_job_fields(job_id: str, **patch):
    try:
        http_patch(f"/rest/v1/{JOBS_TABLE}", {"id": f"eq.{job_id}"}, patch, prefer_return=False)
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
            f"/rest/v1/{JOBS_TABLE}", {"id": f"eq.{job_id}"}, {"heartbeat_at": now_iso_z()}
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
    except Exception as e:
        # A transient error while *checking* must not cancel the job (it used to on
        # HTTPError) nor bubble up into a scrape step (non-HTTP errors used to).
        print("⚠️ is_canceled check failed:", type(e).__name__, e)
        return False


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
    except HTTPError:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# archiving + sold-out helpers (NOTE: still user_id-only RPC signatures in your DB)
# TODO later: add property_id to RPC + soldout table for full isolation.
# ──────────────────────────────────────────────────────────────────────────────

def archive_previous_snapshot(user_id: str, property_id: str, slug: str, checkin: str):
    try:
        r = http_post(
            f"/rest/v1/rpc/{HISTORY_FUNC}",
            {},
            {
                "p_user_id": user_id,
                "p_property_id": property_id,
                "p_slug": slug,
                "p_checkin": checkin,
            },
            timeout_sec=20,
        )
        try:
            n = int(r.json() or 0)
        except Exception:
            n = 0
        print(f"📦 Archived {n} old rows for {slug} {checkin} before upsert")
    except HTTPError as e:
        print("⚠️ archive_previous_snapshot failed:", e, getattr(e.response, "text", "")[:200])


def delete_current_snapshot(user_id: str, property_id: str, slug: str, checkin: str):
    try:
        r = http_post(
            "/rest/v1/rpc/fn_delete_day_rows",
            {},
            {
                "p_user_id": user_id,
                "p_property_id": property_id,
                "p_slug": slug,
                "p_checkin": checkin,
            },
            timeout_sec=20,
        )
        try:
            n = int(r.json() or 0)
        except Exception:
            n = 0
        print(f"🧹 Deleted {n} rows for {slug} {checkin} (sold out)")
    except HTTPError as e:
        print("⚠️ delete_current_snapshot failed:", e, getattr(e.response, "text", "")[:200])


def ensure_soldout_marker(user_id: str, property_id: str, slug: str, checkin: str) -> bool:
    try:
        r = http_post(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {"on_conflict": "user_id,property_id,slug,checkin"},
            [{
                "user_id": user_id,
                "property_id": property_id,
                "slug": slug.lower(),
                "checkin": checkin,
            }],
            upsert=True,
            prefer="resolution=ignore-duplicates,return=representation",
            timeout_sec=20,
        )
        rows = r.json() if r.text else []
        return bool(rows)
    except HTTPError:
        return False


def insert_soldout_alert(user_id: str, property_id: str, slug: str, checkin: str):
    payload = {
        "type": "AUTO_SOLD_OUT",
        "user_id": user_id,
        "property_id": property_id,
        "slug": slug.lower(),
        "checkin": checkin,
        "price": None,
        "payload": {"source": "worker_poll", "reason": "no rooms parsed for any occupancy"},
    }
    try:
        http_post(f"/rest/v1/{ALERTS_TABLE}", {}, payload, timeout_sec=20)
        print(f"🔔 ALERT AUTO_SOLD_OUT for {slug} {checkin} (user={user_id}, property={property_id})")
    except HTTPError:
        pass


def clear_soldout_marker(user_id: str, property_id: str, slug: str, checkin: str):
    try:
        http_delete(
            f"/rest/v1/{SOLDOUT_TABLE}",
            {
                "user_id": f"eq.{user_id}",
                "property_id": f"eq.{property_id}",
                "slug": f"eq.{slug.lower()}",
                "checkin": f"eq.{checkin}",
            },
        )
        print(f"🧽 Cleared sold-out marker for {slug} {checkin}")
    except Exception:
        pass



# ──────────────────────────────────────────────────────────────────────────────
# upsert to RAW (property_id included)
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

    # rate_key must be non-null
    if "rate_key" in RAW_ALLOWED_KEYS:
        rk = out.get("rate_key")
        if not isinstance(rk, str):
            rk = ""
        rk = rk.strip()
        if not rk:
            flags = {
                "breakfast_included": out.get("breakfast_included"),
                "dinner_included": out.get("dinner_included"),
                "half_board": out.get("half_board"),
                "free_cancellation": out.get("free_cancellation"),
                "nonrefundable": out.get("nonrefundable"),
                "prepay_required": out.get("prepay_required"),
            }
            try:
                rk = build_rate_key(flags)
            except Exception:
                rk = ""
        out["rate_key"] = rk or ""

    required_keys = ("user_id", "property_id", "slug", "checkin", "room", "occupancy")
    for rk in required_keys:
        if not out.get(rk):
            return None
    if "hotel" in RAW_ALLOWED_KEYS and not out.get("hotel"):
        return None
    if out.get("price") is None:
        return None
    return out


def upsert_room_prices(user_id: str, property_id: str, job_id: str, rows: List[dict]):
    if not rows:
        return

    clean: List[Dict[str, Any]] = []
    for r in rows:
        base = {
            "user_id": user_id,
            "property_id": property_id,
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

    # Deduplicate within batch: keep cheapest per unique key
    uniq: Dict[tuple, Dict[str, Any]] = {}
    for r in clean:
        key = (
            r["user_id"],
            r["property_id"],
            r["slug"],
            r["checkin"],
            r["room"],
            r["occupancy"],
            r.get("rate_key") or "",
        )
        if key not in uniq or r["price"] < uniq[key]["price"]:
            uniq[key] = r
    clean = list(uniq.values())

    for i in range(0, len(clean), 300):
        batch = clean[i : i + 300]
        http_post(
            f"/rest/v1/{RAW_TABLE}",
            {"on_conflict": RAW_ON_CONFLICT},
            batch,
            upsert=True,
            timeout_sec=30,
        )
        print(f"✅ Upserted {len(batch)} rows into room_prices_raw")


# ──────────────────────────────────────────────────────────────────────────────
# scrape step
# ──────────────────────────────────────────────────────────────────────────────

STEP_LOCK = threading.Lock()


def _scrape_once(job_id: str, user_id: str, property_id: str, hotel: Dict[str, Any], checkin: str, checkout: str) -> Dict[str, Any]:
    stored_slug = (hotel.get("scrape_slug") or hotel.get("slug") or "").strip().lower()
    base_slug, cc_from_slug = split_scrape_slug(stored_slug)
    try:
        if is_canceled(job_id):
            return {"slug": stored_slug, "checkin": checkin, "rows": [], "canceled": True}

        cc = (
            (hotel.get("cc") or "").strip().lower()
            or (cc_from_slug or "")
            or (resolve_cc_for_slug(base_slug) if RESOLVE_CC_IF_MISSING else None)
            or DEFAULT_CC
        )
        stored_slug = to_scrape_slug(base_slug, cc)

        raw_rows, status = scrape_hotel_for_dates_with_status(
            hotel["name"],
            base_slug,
            cc,
            checkin,
            checkout,
            should_cancel=lambda: is_canceled(job_id),
        )

        if is_canceled(job_id):
            return {"slug": stored_slug, "checkin": checkin, "rows": [], "canceled": True}

        # Only an empty result from pages that all loaded fine means "sold out".
        # If any page failed (timeout / HTTP 4xx-5xx / parse error) and nothing was
        # parsed, this is a scrape failure: keep existing prices, raise no alert.
        attempted = int(status.get("attempted") or 0)
        loaded = int(status.get("loaded") or 0)
        explicit = int(status.get("no_availability") or 0)
        # Sold out also needs positive evidence: Booking.com's "no availability" message.
        # All pages loaded but nothing parsed and no such message = likely a layout change.
        if not raw_rows and attempted > 0 and loaded == attempted and explicit == 0:
            return {
                "slug": stored_slug,
                "checkin": checkin,
                "rows": [],
                "canceled": False,
                "failed": True,
                "error": "no rooms parsed and no 'no availability' message (page layout change?)",
            }
        if not raw_rows and (attempted == 0 or loaded < attempted):
            return {
                "slug": stored_slug,
                "checkin": checkin,
                "rows": [],
                "canceled": False,
                "failed": True,
                "error": "; ".join(status.get("errors") or []) or "no pages loaded",
            }

        deduped = dedupe_min_per_room_and_occupancy(raw_rows)
        for r in deduped:
            if not r.get("hotel"):
                r["hotel"] = hotel["name"]
            r["user_id"] = user_id
            r["property_id"] = property_id
            r["slug"] = stored_slug  # normalized + cc
        return {"slug": stored_slug, "checkin": checkin, "rows": deduped, "canceled": False}
    except Exception as e:
        print("⚠️ scrape task error:", e)
        # An exception is a failure, never a sold-out signal.
        return {
            "slug": stored_slug,
            "checkin": checkin,
            "rows": [],
            "canceled": False,
            "failed": True,
            "error": f"{type(e).__name__}: {e}",
        }


# ──────────────────────────────────────────────────────────────────────────────
# selection filter (meta.slugs)
# ──────────────────────────────────────────────────────────────────────────────

def apply_job_slug_filter(job_meta: Dict[str, Any], hotels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected = (job_meta or {}).get("slugs")
    if not (isinstance(selected, list) and len(selected) > 0):
        return hotels

    def _norm(s: str) -> str:
        return (s or "").strip().lower()

    def _base(s: str) -> str:
        s = _norm(s)
        return s.split("__", 1)[0] if "__" in s else s

    selected_set = {_norm(x) for x in selected if isinstance(x, str) and _norm(x)}
    selected_bases = {_base(x) for x in selected_set}

    before = len(hotels)

    def _is_selected(h: Dict[str, Any]) -> bool:
        scrape_slug = _norm(h.get("scrape_slug") or "")
        base = _base(scrape_slug)
        return (scrape_slug in selected_set) or (base in selected_set) or (base in selected_bases)

    filtered = [h for h in hotels if _is_selected(h)]
    after = len(filtered)
    print(f"🔎 job slug filter: had {before}, now {after} (no forcing own)")
    return filtered


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
    property_id = job.get("property_id")

    if not property_id:
        print(f"❌ job {job_id} missing property_id")
        set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error="missing property_id")
        return

    property_id = str(property_id)
    range_days = int(job.get("range_days") or 1)

    start_date = dt.date.today()
    job_started = dt.datetime.now(timezone.utc)

    def over_time_budget() -> bool:
        return (dt.datetime.now(timezone.utc) - job_started).total_seconds() > MAX_JOB_DURATION_SEC

    own = get_own_hotel(user_id, property_id)
    own_id = own["hotel_id"] if own else None
    competitors = get_competitor_hotels(user_id, property_id, own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append(
            {
                "name": own["name"],
                "slug": own["slug"],
                "cc": own.get("cc") or None,
                "scrape_slug": to_scrape_slug(own["slug"], own.get("cc") or DEFAULT_CC),
                "own": True,
            }
        )
    for c in competitors:
        hotels.append(
            {
                "name": c["name"],
                "slug": c["slug"],
                "cc": c.get("cc") or None,
                "scrape_slug": to_scrape_slug(c["slug"], c.get("cc") or DEFAULT_CC),
                "own": False,
            }
        )

    hotels = apply_job_slug_filter(job.get("meta") or {}, hotels)

    if not hotels:
        print(f"❌ job {job_id} has no hotels to scrape for user {user_id} (property {property_id})")
        set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error="no hotels configured")
        return

    print(f"🚀 process_job {job_id} (user {user_id}) | property={property_id} | range_days={range_days} | hotels={len(hotels)}")

    total_steps = len(hotels) * range_days
    set_job_fields(job_id, total_steps=(total_steps or None), completed_steps=0)

    completed_steps = 0
    steps_ok = 0
    steps_soldout = 0
    steps_failed = 0
    first_errors: List[str] = []

    def note_failure(msg: str):
        nonlocal steps_failed
        steps_failed += 1
        if len(first_errors) < 3:
            first_errors.append(msg[:200])

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = []
            for h in hotels:
                for d in range(range_days):
                    checkin = (start_date + dt.timedelta(days=d)).strftime("%Y-%m-%d")
                    checkout = (start_date + dt.timedelta(days=d + 1)).strftime("%Y-%m-%d")
                    futures.append(pool.submit(_scrape_once, job_id, user_id, property_id, h, checkin, checkout))

            pending = set(futures)
            last_progress = time.time()
            canceled_detected = False

            while pending:
                touch_liveness()
                if is_canceled(job_id):
                    canceled_detected = True
                    break
                if over_time_budget():
                    set_job_fields(job_id, status="failed", finished_at=now_iso_z(), last_error="time budget exceeded")
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
                    try:
                        result = fut.result()
                    except Exception as e:
                        print("⏳ step error:", type(e).__name__, e)
                        note_failure(f"{type(e).__name__}: {e}")
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(job_id, completed_steps=completed_steps, last_error="step error")
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)
                        continue

                    slug = (result.get("slug") or "").lower()
                    checkin = result.get("checkin")
                    rows = result.get("rows") or []
                    step_canceled = bool(result.get("canceled"))

                    if step_canceled or is_canceled(job_id):
                        canceled_detected = True
                        continue

                    try:
                        if result.get("failed"):
                            # Scrape failure (timeout / blocked / 404 / exception):
                            # keep the last known prices and do NOT raise a sold-out alert.
                            note_failure(f"{slug} {checkin}: {result.get('error') or 'failed'}")
                        elif rows:
                            if slug and checkin:
                                archive_previous_snapshot(user_id, property_id, slug, checkin)
                            # Upsert right away: archiving may move the previous rows out, so
                            # buffering here risked losing the day's data if the job crashed.
                            upsert_room_prices(user_id, property_id, job_id, rows)
                            if slug and checkin:
                                clear_soldout_marker(user_id, property_id, slug, checkin)
                            steps_ok += 1
                        else:
                            if slug and checkin:
                                archive_previous_snapshot(user_id, property_id, slug, checkin)
                                delete_current_snapshot(user_id, property_id, slug, checkin)
                                # alert only when the day *becomes* sold out, not on every scrape
                                if ensure_soldout_marker(user_id, property_id, slug, checkin):
                                    insert_soldout_alert(user_id, property_id, slug, checkin)
                            steps_soldout += 1
                    except Exception as e:
                        print("⚠️ parallel scrape step error:", e)
                        note_failure(f"{slug} {checkin}: {e}")
                        set_job_fields(job_id, last_error=str(e))
                    finally:
                        with STEP_LOCK:
                            completed_steps += 1
                            set_job_fields(job_id, completed_steps=completed_steps)
                            if completed_steps % HEARTBEAT_EVERY_STEPS == 0:
                                set_heartbeat(job_id)

                if canceled_detected:
                    break

        set_heartbeat(job_id)
        summary = f"ok={steps_ok} sold_out={steps_soldout} failed={steps_failed}"
        print(f"📊 job {job_id}: {summary}")
        if is_canceled(job_id):
            set_job_fields(job_id, status="canceled", finished_at=now_iso_z())
        elif steps_failed > 0 and steps_ok == 0 and steps_soldout == 0:
            # Nothing usable came back (e.g. Booking blocked us) — don't report success.
            set_job_fields(
                job_id,
                status="failed",
                finished_at=now_iso_z(),
                last_error=f"all scrape steps failed ({summary}): " + " | ".join(first_errors),
            )
        else:
            patch: Dict[str, Any] = {"status": "done", "finished_at": now_iso_z()}
            if steps_failed > 0:
                patch["last_error"] = f"partial: {summary}: " + " | ".join(first_errors)
            set_job_fields(job_id, **patch)

    except HTTPError as http_err:
        print("💥 HTTP error in process_job:", http_err, getattr(http_err.response, "text", "")[:400])
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


def touch_liveness():
    try:
        with open(LIVENESS_FILE, "w") as f:
            f.write(str(int(time.time())))
    except OSError as e:
        print("⚠️ liveness touch failed:", e)


def liveness_age_sec(now: Optional[float] = None) -> float:
    try:
        return (now if now is not None else time.time()) - os.path.getmtime(LIVENESS_FILE)
    except OSError:
        return float("inf")


def _liveness_watchdog():
    # The main thread can block outside the no-progress check (e.g. the thread pool
    # waiting on a hung Playwright step after a cancel). The process stays alive, so the
    # restart policy never fires; exiting here hands it back to Docker.
    while True:
        time.sleep(30)
        try:
            check_liveness()
        except Exception as e:  # the watchdog itself must never die
            print("⚠️ liveness watchdog error:", type(e).__name__, e)


def check_liveness():
    age = liveness_age_sec()
    if age > LIVENESS_MAX_AGE_SEC:
        try:
            print(f"🧨 main loop stalled for {age:.0f}s -> exiting so the container restarts")
        finally:
            os._exit(3)


def main():
    print("⏳ Worker online. Polling scrape_jobs...")
    touch_liveness()
    threading.Thread(target=_liveness_watchdog, name="liveness-watchdog", daemon=True).start()
    while True:
        touch_liveness()
        try:
            reap_stale_jobs()
            job = fetch_next_pending_job()
            if not job:
                time.sleep(10)
                continue
            print("🎯 picked job", job.get("id"))
            process_job(job)
        except HTTPError as http_err:
            print("worker loop HTTP error:", http_err, getattr(http_err.response, "text", "")[:400])
            time.sleep(10)
        except Exception as e:
            print("worker loop error:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
