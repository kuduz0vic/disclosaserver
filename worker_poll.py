import os
import re
import json
import time
import asyncio
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

POLL_INTERVAL_SEC = 2.0
MAX_DAYS_DEFAULT = 7
MAX_ADULTS = 4

# ---------------------------
# Small rate-limited logger
# ---------------------------
class RLLogger:
    def __init__(self, every_sec=1.5):
        self.every_sec = every_sec
        self._last = 0.0

    def log(self, msg: str):
        now = time.time()
        if now - self._last >= self.every_sec:
            print(msg, flush=True)
            self._last = now

rl = RLLogger()

# ---------------------------
# Meal-plan detection helpers
# ---------------------------

# We only set flags TRUE if text explicitly says so.
# Otherwise we keep None (unknown) to avoid false positives like for NOX.
BREAKFAST_PATTERNS = [
    r"\bbreakfast included\b",
    r"\bbreakfast is included\b",
    r"\bincludes breakfast\b",
    r"\bwith breakfast\b",
    r"\bzajtrk vključen\b",
    r"\bzajtrk je vključen\b",
    r"\bangebots mit frühstück\b",  # DE variants
    r"\bcolazione inclusa\b",       # IT
]

DINNER_PATTERNS = [
    r"\bdinner included\b",
    r"\bincludes dinner\b",
    r"\bwith dinner\b",
    r"\bvečerja vključen[oa]?\b",
    r"\babendessen inklusive\b",
    r"\bcena vključuje večerjo\b",
]

HALF_BOARD_PATTERNS = [
    r"\bhalf board\b",
    r"\bpolpenzion\b",
    r"\bhalbpension\b",
    r"\bmezza pensione\b",
]

FULL_BOARD_PATTERNS = [
    r"\bfull board\b",
    r"\bpolni penzion\b",
    r"\bvollpension\b",
    r"\bpensione completa\b",
]

ALL_INCLUSIVE_PATTERNS = [
    r"\ball inclusive\b",
    r"\bvse vključeno\b",
    r"\ball-inclusive\b",
]

def detect_meal_flags(rate_text: str) -> Dict[str, Optional[bool]]:
    """
    Return flags:
      breakfast_included, dinner_included, half_board
    True only if explicit match found; else None.
    half_board True if "half board/polpenzion/halbpension/mezza pensione" etc.
    dinner True if dinner explicitly included OR fullboard/allinclusive implies dinner.
    """
    t = re.sub(r"\s+", " ", (rate_text or "").lower())

    def any_match(patterns):
        return any(re.search(p, t) for p in patterns)

    breakfast = True if any_match(BREAKFAST_PATTERNS) else None

    half_board = True if any_match(HALF_BOARD_PATTERNS) else None
    full_board = True if any_match(FULL_BOARD_PATTERNS) else None
    all_incl = True if any_match(ALL_INCLUSIVE_PATTERNS) else None

    dinner = True if any_match(DINNER_PATTERNS) else None
    # If it is full board or all inclusive, dinner is implied
    if dinner is None and (full_board or all_incl):
        dinner = True

    # If half-board explicitly present, dinner is implied
    if dinner is None and half_board:
        dinner = True

    return {
        "breakfast_included": breakfast,
        "dinner_included": dinner,
        "half_board": half_board,
    }

# ---------------------------
# Scrape models
# ---------------------------

@dataclass
class HotelToScrape:
    slug: str          # e.g. "nox__si"
    name: str
    cc: Optional[str]  # "si" etc

def parse_slug_cc(scrape_slug: str) -> Tuple[str, Optional[str]]:
    """
    "nox__si" -> ("nox", "si")
    "nox" -> ("nox", None)
    """
    if "__" in scrape_slug:
        base, cc = scrape_slug.split("__", 1)
        return base, (cc or None)
    return scrape_slug, None

def build_booking_url(base_slug: str, cc: Optional[str], checkin: str, checkout: str, adults: int) -> str:
    # cc is Booking country code for domain; if missing, default .com + locale by lang
    if cc:
        domain = f"www.booking.com/hotel/{cc}/{base_slug}.html"
    else:
        domain = f"www.booking.com/hotel/{base_slug}.html"

    return (
        f"https://{domain}"
        f"?checkin={checkin}"
        f"&checkout={checkout}"
        f"&group_adults={adults}"
        f"&group_children=0"
        f"&no_rooms=1"
        f"&selected_currency=EUR"
        f"&lang=en-gb"
        f"&sb_price_type=total"
    )

# ---------------------------
# Supabase helpers
# ---------------------------

def supa() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

async def claim_next_job(sb: Client) -> Optional[Dict[str, Any]]:
    """
    Claim one pending job.
    We do a select then update; in practice fine with 1 worker.
    If you scale workers, replace with RPC or advisory lock.
    """
    res = sb.table("scrape_jobs") \
        .select("*") \
        .eq("status", "pending") \
        .order("created_at", desc=False) \
        .limit(1) \
        .execute()

    row = (res.data or [None])[0]
    if not row:
        return None

    job_id = row["id"]
    # try to set running (only if still pending)
    upd = sb.table("scrape_jobs") \
        .update({"status": "running", "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}) \
        .eq("id", job_id) \
        .eq("status", "pending") \
        .execute()

    if not upd.data:
        return None

    return row

def load_job_hotels(sb: Client, user_id: str, slugs_filter: Optional[List[str]]) -> List[HotelToScrape]:
    """
    Pull own + competitors for user, filtered by selected slugs from job if provided.
    """
    links = sb.table("user_hotels") \
        .select("hotel_id, link_type, inserted_at, hotels(name,url,id)") \
        .eq("user_id", user_id) \
        .order("inserted_at") \
        .execute().data or []

    hotels: List[HotelToScrape] = []
    for l in links:
        h = l.get("hotels") or {}
        name = h.get("name")
        url_slug = (h.get("url") or "").strip().lower()
        if not url_slug:
            continue
        base, cc = parse_slug_cc(url_slug)
        scrape_slug = f"{base}__{cc}" if cc else base
        hotels.append(HotelToScrape(slug=scrape_slug, name=name or base, cc=cc))

    if slugs_filter:
        sset = set(slugs_filter)
        hotels = [h for h in hotels if h.slug in sset]

    return hotels

def job_is_canceled(sb: Client, job_id: str) -> bool:
    r = sb.table("scrape_jobs").select("status").eq("id", job_id).limit(1).execute()
    st = (r.data or [{}])[0].get("status")
    return st == "canceled"

def mark_done(sb: Client, job_id: str, meta: Dict[str, Any]):
    sb.table("scrape_jobs").update({
        "status": "done",
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "meta": meta
    }).eq("id", job_id).execute()

def mark_failed(sb: Client, job_id: str, err: str, meta: Dict[str, Any]):
    sb.table("scrape_jobs").update({
        "status": "failed",
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "last_error": err[:1200],
        "meta": meta
    }).eq("id", job_id).execute()

def mark_canceled(sb: Client, job_id: str, meta: Dict[str, Any]):
    sb.table("scrape_jobs").update({
        "status": "canceled",
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "meta": meta
    }).eq("id", job_id).execute()

def upsert_raw_rows(sb: Client, rows: List[Dict[str, Any]]):
    if not rows:
        return
    sb.table("room_prices_raw").insert(rows).execute()

# ---------------------------
# Playwright scrape
# ---------------------------

async def safe_route_handler(route, request):
    try:
        if request.resource_type in ("image", "font", "media"):
            await route.abort()
        else:
            await route.continue_()
    except asyncio.CancelledError:
        return
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass

async def scrape_one_url(page, url: str) -> List[Dict[str, Any]]:
    """
    Returns list of raw price rows found on that page.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(800)

    # Booking.com room blocks are messy; we grab text from each room card
    room_cards = await page.query_selector_all('[data-testid="property-card"], [data-testid="room-card"], .hprt-table')

    results = []
    full_page_text = (await page.inner_text("body")).lower()

    # Fallback meal detection from full page if needed
    global_flags = detect_meal_flags(full_page_text)

    for card in room_cards:
        try:
            text = (await card.inner_text()).strip()
        except Exception:
            continue

        low = text.lower()
        flags = detect_meal_flags(low)

        # If card doesn't say anything but page does, inherit
        for k, v in flags.items():
            if v is None and global_flags.get(k) is True:
                flags[k] = True

        # price extraction (take EUR amounts)
        prices = re.findall(r"€\s?([0-9]+(?:\.[0-9]+)?)", text.replace(",", ""))
        if not prices:
            continue

        price_num = float(prices[0])

        # occupancy guess: look for "x guests" or icons etc. keep as None if unknown
        occ = None
        m = re.search(r"(\d+)\s+guests?", low)
        if m:
            occ = int(m.group(1))

        # room name best effort
        room_name = None
        rm = re.search(r"^(.{3,80})\n", text)
        if rm:
            room_name = rm.group(1).strip()

        results.append({
            "price": price_num,
            "room": room_name,
            "occupancy": occ,
            **flags,
            "rate_plan": text[:400],  # store snippet for debugging
        })

    return results

async def scrape_hotel_dates(sb: Client, job_id: str, user_id: str, hotel: HotelToScrape,
                            start_date: str, range_days: int):
    """
    Scrape a hotel for 1..4 adults for each date in range.
    Inserts into room_prices_raw.
    """
    base_slug, cc = parse_slug_cc(hotel.slug)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context()
        await context.route("**/*", safe_route_handler)
        page = await context.new_page()

        inserted_rows = []
        errors = 0

        for d_offset in range(range_days):
            if job_is_canceled(sb, job_id):
                await context.unroute("**/*")
                await context.close()
                await browser.close()
                raise asyncio.CancelledError()

            checkin = time.strftime("%Y-%m-%d", time.gmtime(time.time() + d_offset * 86400))
            checkout = time.strftime("%Y-%m-%d", time.gmtime(time.time() + (d_offset + 1) * 86400))

            for adults in range(1, MAX_ADULTS + 1):
                if job_is_canceled(sb, job_id):
                    raise asyncio.CancelledError()

                url = build_booking_url(base_slug, cc, checkin, checkout, adults)
                rl.log(f"🏨 {hotel.name} | {checkin}→{checkout} | adults={adults}")

                try:
                    rows = await scrape_one_url(page, url)
                except PWTimeout:
                    errors += 1
                    continue
                except Exception as e:
                    errors += 1
                    continue

                for r in rows:
                    inserted_rows.append({
                        "user_id": user_id,
                        "hotel": hotel.name,
                        "slug": hotel.slug,
                        "checkin": checkin,
                        "occupancy": r.get("occupancy") or adults,  # fallback to adults
                        "price": r["price"],
                        "room": r.get("room"),
                        "inserted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "breakfast_included": r.get("breakfast_included"),
                        "dinner_included": r.get("dinner_included"),
                        "half_board": r.get("half_board"),
                        "free_cancellation": None,   # keep placeholder, add later if you want
                        "nonrefundable": None,
                        "prepay_required": None,
                        "rate_plan": r.get("rate_plan"),
                    })

        try:
            upsert_raw_rows(sb, inserted_rows)
        finally:
            try:
                await context.unroute("**/*")
            except Exception:
                pass
            await context.close()
            await browser.close()

        return {"rows": len(inserted_rows), "errors": errors}

# ---------------------------
# Job processor
# ---------------------------

async def process_job(sb: Client, job: Dict[str, Any]):
    job_id = job["id"]
    user_id = job["user_id"]
    range_days = int(job.get("range_days") or MAX_DAYS_DEFAULT)
    slugs_filter = job.get("slugs") or job.get("meta", {}).get("slugs")

    hotels = load_job_hotels(sb, user_id, slugs_filter)
    if not hotels:
        mark_failed(sb, job_id, "No hotels to scrape", {"reason": "empty_hotels"})
        return

    meta = {"hotels": len(hotels), "range_days": range_days, "inserted": 0, "errors": 0}

    try:
        for h in hotels:
            if job_is_canceled(sb, job_id):
                mark_canceled(sb, job_id, meta)
                return

            res = await scrape_hotel_dates(sb, job_id, user_id, h, start_date=None, range_days=range_days)
            meta["inserted"] += res["rows"]
            meta["errors"] += res["errors"]
            meta["last_hotel"] = h.name

        mark_done(sb, job_id, meta)

    except asyncio.CancelledError:
        mark_canceled(sb, job_id, meta)
        return
    except Exception as e:
        mark_failed(sb, job_id, str(e), meta)
        return

# ---------------------------
# Main worker loop
# ---------------------------

async def worker_loop():
    sb = supa()
    print("👷 worker started", flush=True)

    while True:
        try:
            job = await claim_next_job(sb)
            if not job:
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            print(f"🎯 picked job {job['id']}", flush=True)
            await process_job(sb, job)

        except Exception as e:
            print(f"[worker] loop error: {e}", flush=True)
            await asyncio.sleep(2.0)

if __name__ == "__main__":
    asyncio.run(worker_loop())
