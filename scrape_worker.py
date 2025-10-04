# scrape_worker.py
# ------------------------------------------------------------------------------
# Disclosa/Yield - Country-aware, Parallel Booking.com scraper + worker
#
# ENV (required):
#   SUPABASE_URL
#   SUPABASE_SERVICE_ROLE_KEY      # service role preferred (writes jobs/rows)
#
# ENV (optional):
#   MAX_WORKERS=4                  # threadpool size (each task starts its own browser)
#   DEFAULT_CC=si                  # fallback country if we can't resolve
#   RESOLVE_CC_IF_MISSING=1        # try to auto-resolve cc if missing (0 to disable)
#   COMMON_CCS="si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr"
#   PAGE_GOTO_TIMEOUT_MS=20000
#   WAIT_TABLE_TIMEOUT_MS=8000
#   SCROLL_PASSES=12
#   PLAYWRIGHT_BROWSERS_PATH=0     # good default in containers
#
# Tables:
#   - scrape_jobs
#   - user_hotels (join hotels(name,url,hotel_profiles(booking_slug,booking_cc)))
#   - room_prices_raw (unique on: user_id, slug, checkin, room)
#
# Note: conservative concurrency + image/media/font blocking to reduce load.
# ------------------------------------------------------------------------------

import os
import re
import time
import json
import math
import functools
import threading
import datetime as dt
from datetime import timezone
from typing import Optional, Dict, Any, List, Callable
from urllib.parse import urlencode, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests import HTTPError
from playwright.sync_api import sync_playwright, TimeoutError

# ------------------------------ Env & Tunables --------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
)
assert SUPABASE_URL and SUPABASE_KEY, "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
DEFAULT_CC = (os.getenv("DEFAULT_CC", "si") or "si").lower()
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "1") == "1"
COMMON_CCS = (os.getenv("COMMON_CCS") or
              "si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr").split(",")

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "20000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "8000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))

JOBS_TABLE = "scrape_jobs"
RAW_TABLE = "room_prices_raw"
RAW_ON_CONFLICT = "user_id,slug,checkin,room"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ------------------------------ HTTP helpers ----------------------------------

def supabase_headers(json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if json_pref:
        h["Content-Type"] = "application/json"
    if upsert:
        h["Prefer"] = "resolution=merge-duplicates,return=representation"
    return h

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

def http_post(path: str, params: Dict[str, Any], json_body: Any, upsert=False) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    headers = supabase_headers(upsert=upsert)
    r = requests.post(url, params=params, json=json_body, headers=headers, timeout=60)
    r.raise_for_status()
    return r

# ------------------------------ Utilities -------------------------------------

def now_iso_z() -> str:
    return dt.datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def chunked(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [items]
    return [items[i:i+size] for i in range(0, len(items), size)]

# ------------------------------ Jobs ------------------------------------------

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
            {"status": "running", "started_at": now_iso_z()},
            prefer_return=True,
        )
        rows = r.json() if r.text else []
        return rows[0] if rows else None
    except HTTPError as e:
        print("❌ claim_job error:", e, getattr(e.response, "text", "")[:400])
        return None

def set_job_fields(job_id: str, **patch):
    safe_patch: Dict[str, Any] = {}
    for k, v in patch.items():
        if k in ("finished_at", "started_at") and isinstance(v, dt.datetime):
            safe_patch[k] = v.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            safe_patch[k] = v
    try:
        http_patch(
            f"/rest/v1/{JOBS_TABLE}",
            {"id": f"eq.{job_id}"},
            safe_patch,
            prefer_return=False,
        )
    except HTTPError as e:
        print("❌ set_job_fields error:", e, "| payload:", safe_patch, "| resp:", getattr(e.response, "text", "")[:400])
        raise

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

# ------------------------ User hotel links (+cc) -------------------------------

def _normalize_slug(v: str) -> str:
    if not v:
        return ""
    t = v.strip()
    if t.startswith("http"):
        try:
            u = urlparse(t)
            parts = [p for p in u.path.split("/") if p]
            if len(parts) >= 3 and parts[0].lower() == "hotel":
                slug = parts[2]
                return re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", slug, flags=re.I)
        except Exception:
            pass
    t = re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", t, flags=re.I)
    return t.replace(" ", "")

def _get_user_links(user_id: str) -> List[dict]:
    """
    Pull user_hotels + joined hotels + joined hotel_profiles(booking_slug, booking_cc) via hotels table.
    """
    url = f"{SUPABASE_URL}/rest/v1/user_hotels"
    params = {
        "select": "hotel_id,link_type,inserted_at,hotels(name,url,hotel_profiles(booking_slug,booking_cc))",
        "user_id": f"eq.{user_id}",
        "order": "inserted_at.asc",
    }
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=20)
    r.raise_for_status()
    rows = r.json() or []

    out: List[dict] = []
    for row in rows:
        h = (row.get("hotels") or {})
        prof = h.get("hotel_profiles")

        if isinstance(prof, list) and prof:
            bslug = (prof[0].get("booking_slug") or "").strip()
            bcc   = (prof[0].get("booking_cc") or "").strip().lower() or ""
        elif isinstance(prof, dict) and prof:
            bslug = (prof.get("booking_slug") or "").strip()
            bcc   = (prof.get("booking_cc") or "").strip().lower() or ""
        else:
            bslug, bcc = "", ""

        out.append({
            "hotel_id": row.get("hotel_id"),
            "link_type": (row.get("link_type") or "").strip(),
            "inserted_at": row.get("inserted_at"),
            "name": (h.get("name") or "").strip(),
            "url": (h.get("url") or "").strip(),
            "booking_slug": bslug,
            "booking_cc": bcc,
        })
    return out

def get_own_hotel(user_id: str) -> Optional[Dict[str, Any]]:
    links = _get_user_links(user_id)
    if not links:
        return None

    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {"hotel_id": l["hotel_id"], "name": l["name"] or "My Hotel", "slug": slug, "cc": cc}
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {"hotel_id": first["hotel_id"], "name": first["name"] or "My Hotel", "slug": slug, "cc": cc}

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l["hotel_id"] == own_hotel_id:
            continue
        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({"hotel_id": l["hotel_id"], "name": l["name"] or "", "slug": slug, "cc": cc})
    return out

# ------------------------ Country resolver (optional) --------------------------

@functools.lru_cache(maxsize=512)
def resolve_cc_for_slug(slug: str, timeout: float = 4.0) -> Optional[str]:
    if not slug:
        return None
    s = requests.Session()
    headers = {"User-Agent": USER_AGENT}
    for cc in COMMON_CCS:
        try:
            url = f"https://www.booking.com/hotel/{cc}/{slug}.html?lang=en-gb"
            r = s.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code != 200:
                continue
            parts = [p for p in urlparse(r.url).path.lower().split("/") if p]
            if len(parts) >= 3 and parts[0] == "hotel":
                final_cc = parts[1]
                final_slug = parts[2].split(".")[0]
                if final_slug == slug.lower():
                    return final_cc
        except requests.RequestException:
            continue
    return None

# ------------------------------ Scraper ---------------------------------------

EXPAND_SELECTORS = [
    "button:has-text('Show all')","button:has-text('Show more')",
    "button:has-text('See all rooms')","a:has-text('Show all')",
    "a:has-text('Show more')","a:has-text('See all rooms')",
    "button:has-text('Prikaži več')","button:has-text('Prikaži vse')",
    "a:has-text('Prikaži več')","a:has-text('Prikaži vse')",
]
ROOM_TABLE_SELECTORS = [
    "tr.js-rt-block-row",
    "table.hprt-table tr",
    "[data-testid='room-row']",
]
PRICE_SELECTORS = [
    ".prco-valign-middle-helper",
    ".bui-price-display__value",
    "[data-testid='price-and-discounted-price']",
]

def get_occupancy_from_name(name: str) -> int:
    n = (name or "").lower()
    if any(x in n for x in ["triposteljna", "troposteljna", "triple"]):
        return 3
    if any(x in n for x in ["štiriposteljna", "stiriposteljna", "quadruple", "družinska", "familij"]):
        return 4
    if any(x in n for x in ["enoposteljna", "single"]):
        return 1
    return 2

def is_valid_room(name: str) -> bool:
    n = (name or "").lower()
    return not any(x in n for x in ["dnevna soba", "review", "rezultati", "ocena"])

def clean_price(text: str) -> Optional[float]:
    if not text:
        return None
    txt = text.replace("\u00A0", " ").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", txt)
    return float(m.group(1)) if m else None

def build_hotel_url(cc: Optional[str], slug: str, checkin: str, checkout: str, adults: int, lang="en-gb"):
    cc_eff = (cc or DEFAULT_CC).lower()
    base = f"https://www.booking.com/hotel/{cc_eff}/{slug}.html"
    qs = {
        "checkin": checkin,
        "checkout": checkout,
        "group_adults": adults,
        "group_children": 0,
        "no_rooms": 1,
        "selected_currency": "EUR",
        "lang": lang,
    }
    return f"{base}?{urlencode(qs)}"

def aggressively_expand_and_scroll(page, should_cancel: Optional[Callable[[], bool]] = None):
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())
    try:
        if cancelled(): return
        page.wait_for_selector("#onetrust-accept-btn-handler", timeout=3000)
        page.click("#onetrust-accept-btn-handler")
    except TimeoutError:
        pass

    for _ in range(3):
        if cancelled(): return
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=400)
                page.wait_for_timeout(120)
            except Exception:
                pass

    last_h = 0
    for _ in range(SCROLL_PASSES):
        if cancelled(): return
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(500)
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=300)
            except Exception:
                pass
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

def collect_room_rows(page) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    for table_sel in ROOM_TABLE_SELECTORS:
        rows = page.query_selector_all(table_sel)
        for row in rows:
            try:
                name_el = (
                    row.query_selector(".hprt-roomtype-icon-link")
                    or row.query_selector(".hprt-roomtype-name")
                    or row.query_selector("[data-testid='room-name']")
                )
                room_name = (name_el.inner_text().strip() if name_el else None)
                if not room_name or not is_valid_room(room_name):
                    continue

                price = None
                for td in row.query_selector_all("td,div,section"):
                    for psel in PRICE_SELECTORS:
                        el = td.query_selector(psel)
                        if el:
                            price = clean_price(el.inner_text())
                            if price is not None:
                                break
                    if price is not None:
                        break

                if price is not None:
                    results.append({"room": re.sub(r"\s+", " ", room_name), "price": price})
            except Exception:
                continue
    return results

def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: str,
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    """
    For adults=1..4:
      - Load /hotel/{cc}/{slug}.html with given checkin/checkout
      - Expand/scroll
      - Collect room rows
      - Keep ONLY rows where inferred occupancy == adults
    Return: [{hotel, slug, checkin, room, occupancy, price}]
    """
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    out: List[dict] = []
    with sync_playwright() as p:
        extra_args = ["--no-sandbox"] if os.getenv("NO_SANDBOX") == "1" else []
        browser = p.chromium.launch(headless=True, args=extra_args)
        context = browser.new_context(
            locale="en-GB",
            viewport={"width": 1400, "height": 900},
            user_agent=USER_AGENT,
        )
        # Block heavy resources
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        page = context.new_page()

        for adults in (1, 2, 3, 4):
            if cancelled(): break
            url = build_hotel_url(cc, slug, checkin, checkout, adults, lang="en-gb")
            print(f"🏨 {name} | {checkin}→{checkout} | adults={adults}\n   {url}")
            try:
                page.goto(url, timeout=PAGE_GOTO_TIMEOUT_MS)
            except TimeoutError:
                print("⚠️ Timeout loading hotel page.")
                continue

            if cancelled(): break

            try:
                aggressively_expand_and_scroll(page, should_cancel=should_cancel)
                if cancelled(): break

                try:
                    page.wait_for_selector(",".join(ROOM_TABLE_SELECTORS), timeout=WAIT_TABLE_TIMEOUT_MS)
                except TimeoutError:
                    print("⚠️ No room table found.")
                    continue

                if cancelled(): break

                rows = collect_room_rows(page)
                for r in rows:
                    if cancelled(): break
                    occ_inferred = get_occupancy_from_name(r["room"])
                    if occ_inferred != adults:
                        continue
                    out.append({
                        "hotel": name,
                        "slug": slug.lower(),
                        "checkin": checkin,
                        "room": r["room"],
                        "occupancy": occ_inferred,
                        "price": r["price"],
                    })
            except Exception as e:
                print("⚠️ Page error:", e)
                continue

        browser.close()
    return out

# ------------------------------ Upserts ---------------------------------------

def upsert_rows(table: str, rows: List[dict], on_conflict: str):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {"on_conflict": on_conflict}
    headers = supabase_headers(upsert=True)
    # Chunk to avoid large payloads (approx size safe ~500 rows)
    for batch in chunked(rows, 400):
        r = requests.post(url, params=params, json=batch, headers=headers, timeout=60)
        if r.status_code not in (200, 201):
            print(f"❌ Upsert into {table} failed:", r.status_code, r.text[:400])
        else:
            print(f"✅ Upserted {len(batch)} rows into {table}")

def dedupe_min_per_room(rows: List[dict]) -> List[dict]:
    best: Dict[tuple, dict] = {}
    for r in rows:
        slug_lower = (r["slug"] or "").lower()
        key = (slug_lower, r["checkin"], r["room"])
        price = r["price"]
        if price is None:
            continue
        if key not in best or price < best[key]["price"]:
            best[key] = {
                "hotel": r["hotel"],
                "slug": slug_lower,
                "checkin": r["checkin"],
                "room": r["room"],
                "occupancy": r["occupancy"],
                "price": price,
            }
    return list(best.values())

def upsert_room_level(user_id: str, room_rows: List[dict]):
    if not room_rows:
        return
    for r in room_rows:
        r["user_id"] = user_id
    upsert_rows(RAW_TABLE, room_rows, RAW_ON_CONFLICT)

# ------------------------------ Worker core -----------------------------------

STEP_LOCK = threading.Lock()

def _scrape_task(job_id: str, user_id: str, hotel: Dict[str, Any], checkin: str, checkout: str) -> List[dict]:
    if is_canceled(job_id):
        return []
    try:
        raw = scrape_hotel_for_dates(
            hotel["name"], hotel["slug"], hotel["cc"], checkin, checkout,
            should_cancel=lambda: is_canceled(job_id)
        )
        if is_canceled(job_id):
            return []
        room_level = dedupe_min_per_room(raw)
        for r in room_level:
            r["user_id"] = user_id
            r["job_id"] = job_id
        return room_level
    except Exception as e:
        print("⚠️ task error:", e)
        return []

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

    print(f"🚀 Processing job {job_id} for user {user_id} range_days={range_days}")
    print(f"🏨 Own: {bool(own)} | Competitors: {len(competitors)}")

    total_steps = len(hotels) * range_days
    set_job_fields(job_id, total_steps=(total_steps or None), completed_steps=0)

    completed = 0

    def should_cancel() -> bool:
        return is_canceled(job_id)

    try:
        # Resolve missing CCs if requested
        for h in hotels:
            if not h.get("cc") and RESOLVE_CC_IF_MISSING:
                resolved = resolve_cc_for_slug(h["slug"])
                h["cc"] = resolved or DEFAULT_CC

        tasks = []
        results_buffer: List[dict] = []
        buffer_lock = threading.Lock()

        def submit_one(pool, h, ci, co):
            return pool.submit(_scrape_task, job_id, user_id, h, ci, co)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for h in hotels:
                if should_cancel():
                    print(f"🛑 Job {job_id} canceled before scheduling.")
                    break
                name, slug = h["name"], h["slug"]
                is_own = bool(h.get("own"))

                for i in range(range_days):
                    if should_cancel():
                        print(f"🛑 Job {job_id} canceled during scheduling.")
                        break

                    checkin = (start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d")
                    checkout = (start_date + dt.timedelta(days=i + 1)).strftime("%Y-%m-%d")
                    set_job_fields(job_id, meta={"last_hotel": name, "last_checkin": checkin, "own": is_own})
                    tasks.append(submit_one(pool, h, checkin, checkout))

            for fut in as_completed(tasks):
                if should_cancel():
                    break
                try:
                    rows = fut.result() or []
                    if rows:
                        with buffer_lock:
                            results_buffer.extend(rows)
                            # Flush in batches to reduce DB calls but keep memory in check
                            if len(results_buffer) >= 300:
                                upsert_room_level(user_id, results_buffer)
                                results_buffer.clear()
                except Exception as e:
                    print("⚠️ Parallel step error:", e)
                    set_job_fields(job_id, last_error=str(e))
                finally:
                    nonlocal_completed = None
                    with STEP_LOCK:
                        completed += 1
                        nonlocal_completed = completed
                    set_job_fields(job_id, completed_steps=nonlocal_completed)

        # flush remaining
        if results_buffer:
            upsert_room_level(user_id, results_buffer)
            results_buffer.clear()

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

# ------------------------------ Manual runner ---------------------------------

def run_for_user(user_id: str, start_date: str, days: int):
    own = get_own_hotel(user_id)
    own_id = own["hotel_id"] if own else None
    comps = get_competitor_hotels(user_id, own_hotel_id=own_id)

    hotels: List[Dict[str, Any]] = []
    if own:
        hotels.append({"name": own["name"], "slug": own["slug"], "cc": own.get("cc"), "own": True})
    hotels.extend([{"name": c["name"], "slug": c["slug"], "cc": c.get("cc"), "own": False} for c in comps])

    if not hotels:
        print("No selected hotels for user.")
        return

    # resolve CCs
    for h in hotels:
        if not h.get("cc") and RESOLVE_CC_IF_MISSING:
            h["cc"] = resolve_cc_for_slug(h["slug"]) or DEFAULT_CC

    start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    print(f"🏨 Own: {bool(own)} | Competitors: {len(comps)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = []
        for h in hotels:
            for i in range(days):
                checkin = (start + dt.timedelta(days=i)).strftime("%Y-%m-%d")
                checkout = (start + dt.timedelta(days=i + 1)).strftime("%Y-%m-%d")
                futures.append(pool.submit(_scrape_task, "manual", user_id, h, checkin, checkout))

        buffer: List[dict] = []
        for f in as_completed(futures):
            rows = f.result() or []
            if rows:
                buffer.extend(rows)
                if len(buffer) >= 300:
                    upsert_room_level(user_id, buffer)
                    buffer.clear()
        if buffer:
            upsert_room_level(user_id, buffer)
            buffer.clear()

# ------------------------------ Main loop -------------------------------------

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
    # If you want a quick manual run, set RUN_FOR_USER_ID=... in env
    run_uid = os.getenv("RUN_FOR_USER_ID")
    if run_uid:
        start = os.getenv("RUN_START_DATE") or dt.datetime.now(timezone.utc).strftime("%Y-%m-%d")
        days = int(os.getenv("RUN_DAYS", "3"))
        run_for_user(run_uid, start, days)
    else:
        main()
