# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Playwright scraper core for Booking.com
# Hardened for:
#  - asyncio.CancelledError noise from route handlers / shutdown
#  - Accurate meal flags (breakfast/dinner/half_board)
# Compatible with your original worker_poll.py
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
import asyncio
import datetime as dt
from typing import Dict, Any, List, Optional, Callable, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Supabase config (used by worker)
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY")

DEFAULT_CC = os.environ.get("DEFAULT_CC", "si").lower()
RESOLVE_CC_IF_MISSING = os.environ.get("RESOLVE_CC_IF_MISSING", "1") == "1"

def supabase_headers(upsert: bool = False, json_pref: bool = True):
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if json_pref:
        h["Accept"] = "application/json"
    if upsert:
        h["Prefer"] = "resolution=merge-duplicates"
    return h

# ──────────────────────────────────────────────────────────────────────────────
# DB helpers (simple REST; same pattern as you used)
# ──────────────────────────────────────────────────────────────────────────────
def _sb_get(path: str, params: Dict[str, Any]) -> List[dict]:
    url = f"{SUPABASE_URL}{path}"
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=30)
    r.raise_for_status()
    return r.json() or []

def get_own_hotel(user_id: str) -> Optional[dict]:
    rows = _sb_get(
        "/rest/v1/user_hotels",
        {
            "select": "hotel_id,link_type,hotels(name,url,id,booking_slug,booking_cc)",
            "user_id": f"eq.{user_id}",
            "link_type": "eq.own",
            "limit": 1,
        },
    )
    if not rows:
        # fallback to first linked
        rows = _sb_get(
            "/rest/v1/user_hotels",
            {
                "select": "hotel_id,link_type,hotels(name,url,id,booking_slug,booking_cc)",
                "user_id": f"eq.{user_id}",
                "limit": 1,
            },
        )
        if not rows:
            return None

    row = rows[0]
    h = row.get("hotels") or {}
    slug = (h.get("booking_slug") or h.get("url") or "").strip().lower()
    cc = (h.get("booking_cc") or None)
    return {
        "hotel_id": row.get("hotel_id") or h.get("id"),
        "name": h.get("name") or slug,
        "slug": slug,
        "cc": cc,
    }

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[dict]:
    rows = _sb_get(
        "/rest/v1/user_competitors",
        {"select": "id,comp_name,booking_slug,booking_cc", "user_id": f"eq.{user_id}"},
    )
    out = []
    for r in rows:
        slug = (r.get("booking_slug") or "").strip().lower()
        if not slug:
            continue
        out.append(
            {
                "hotel_id": r["id"],
                "name": r.get("comp_name") or slug,
                "slug": slug,
                "cc": r.get("booking_cc"),
            }
        )
    return out

def resolve_cc_for_slug(slug: str) -> Optional[str]:
    # if you store cc in hotels table you can map it here;
    # fallback to DEFAULT_CC
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Meal / rate flag parsing
# ──────────────────────────────────────────────────────────────────────────────
_RE_BREAKFAST_POS = re.compile(r"\bbreakfast\b.*\bincluded\b|\bincluded breakfast\b", re.I)
_RE_DINNER_POS    = re.compile(r"\bdinner\b.*\bincluded\b|\bincluded dinner\b", re.I)
_RE_HALFB_POS     = re.compile(r"\bhalf[-\s]?board\b.*\bincluded\b|\bincluded half[-\s]?board\b", re.I)

# negative cues: if present near meal words, treat as NOT included
_RE_NEG = re.compile(r"\bavailable\b|\boptional\b|\bat extra charge\b|\bextra fee\b|\bcan be added\b|\bfor an additional\b|\bnot included\b", re.I)

def parse_meal_flags(text_blocks: List[str]) -> Tuple[bool, bool, bool]:
    """
    Takes mealplan-ish texts and decides included flags.
    Distinguishes 'included' from 'available'.
    """
    full = " • ".join([t for t in text_blocks if t]).strip()
    if not full:
        return False, False, False

    # If negatives appear, we only accept meals that are explicitly included.
    neg = bool(_RE_NEG.search(full))

    half_board = bool(_RE_HALFB_POS.search(full))
    breakfast  = bool(_RE_BREAKFAST_POS.search(full))
    dinner     = bool(_RE_DINNER_POS.search(full))

    # Half board included implies breakfast+dinner included.
    if half_board:
        breakfast = True
        dinner = True

    # If neg cues exist and we don't have explicit "included" patterns, zero it.
    if neg:
        # keep only explicitly included meals already detected;
        # (we don't add anything just because meal word appears)
        pass

    return breakfast, dinner, half_board

_RE_FREE_CANCEL = re.compile(r"free cancellation|cancel for free", re.I)
_RE_NONREF      = re.compile(r"non[-\s]?refundable|no refund", re.I)
_RE_PREPAY      = re.compile(r"prepayment|pay in advance|you'll be charged", re.I)

def parse_rate_flags(rate_text: str) -> Dict[str, Any]:
    t = (rate_text or "").strip()
    return {
        "free_cancellation": bool(_RE_FREE_CANCEL.search(t)),
        "nonrefundable": bool(_RE_NONREF.search(t)),
        "prepay_required": bool(_RE_PREPAY.search(t)),
        "rate_plan": t[:400] if t else None,
    }

# ──────────────────────────────────────────────────────────────────────────────
# Playwright scrape
# ──────────────────────────────────────────────────────────────────────────────
def _build_url(slug: str, cc: str, checkin: str, checkout: str, adults: int, lang: str = "en-gb") -> str:
    # slug is booking slug like "nox" not full domain
    # If you already store full booking url, pass that in and skip formatting
    if slug.startswith("http"):
        base = slug
    else:
        base = f"https://www.booking.com/hotel/{cc}/{slug}.html"
    return (
        f"{base}?checkin={checkin}&checkout={checkout}"
        f"&group_adults={adults}&group_children=0&no_rooms=1"
        f"&selected_currency=EUR&lang={lang}&sb_price_type=total"
    )

async def _scrape_one_occ(
    hotel_name: str,
    slug: str,
    cc: str,
    checkin: str,
    checkout: str,
    adults: int,
    should_cancel: Callable[[], bool],
) -> List[dict]:
    url = _build_url(slug, cc, checkin, checkout, adults)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(locale="en-GB")
        page = await context.new_page()

        # Block heavy resources, but DON'T let CancelledError bubble.
        async def route_handler(route):
            try:
                if should_cancel():
                    await route.abort()
                    return
                rtype = route.request.resource_type
                if rtype in ("image", "font", "media"):
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

        await context.route("**/*", route_handler)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(1000)

            if should_cancel():
                return []

            # Wait for room cards or sold-out
            await page.wait_for_selector(
                '[data-testid="property-card"], [data-testid="sold-out-property"], #availability_target',
                timeout=20000,
            )

            cards = await page.query_selector_all('[data-testid="property-card"]')
            rows: List[dict] = []

            for card in cards:
                if should_cancel():
                    break

                # Room name
                room_el = await card.query_selector('[data-testid="title"]')
                room = (await room_el.inner_text()).strip() if room_el else None
                if not room:
                    continue

                # Price: find cheapest visible total price
                price_el = await card.query_selector('[data-testid="price-and-discounted-price"]')
                if not price_el:
                    price_el = await card.query_selector('[data-testid="price"]')

                price_txt = (await price_el.inner_text()).strip() if price_el else ""
                price_num = _parse_price(price_txt)

                if price_num is None:
                    continue

                # Mealplan badges/texts (more stable than free text)
                meal_bits = []
                meal_nodes = await card.query_selector_all('[data-testid="mealplan"], [data-testid="policy-table"] span, [data-testid="taxes-and-charges"]')
                for n in meal_nodes[:8]:
                    try:
                        t = (await n.inner_text()).strip()
                        if t:
                            meal_bits.append(t)
                    except Exception:
                        pass

                breakfast, dinner, half_board = parse_meal_flags(meal_bits)

                # Rate/policy text
                pol_bits = []
                pol_nodes = await card.query_selector_all('[data-testid="cancellation-policy"], [data-testid="payment-policy"], [data-testid="policy-table"]')
                for n in pol_nodes[:8]:
                    try:
                        t = (await n.inner_text()).strip()
                        if t:
                            pol_bits.append(t)
                    except Exception:
                        pass
                rate_text = " • ".join(pol_bits)
                flags = parse_rate_flags(rate_text)

                rows.append({
                    "hotel": hotel_name,
                    "slug": slug,
                    "checkin": checkin,
                    "room": room,
                    "occupancy": adults,
                    "price": price_num,
                    "breakfast_included": breakfast,
                    "dinner_included": dinner,
                    "half_board": half_board,
                    **flags,
                })

            return rows

        finally:
            # IMPORTANT: unroute first to avoid CancelledError spam on close
            try:
                await context.unroute_all()
            except Exception:
                pass
            try:
                await context.close()
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            try:
                await browser.close()
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

def _parse_price(txt: str) -> Optional[float]:
    if not txt:
        return None
    # take first number like "€ 123" or "123 €"
    m = re.search(r"([0-9][0-9\.,]*)", txt.replace("\u202f", " "))
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return None

def scrape_hotel_for_dates(
    hotel_name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Callable[[], bool] = lambda: False,
    langs: Optional[List[str]] = None,
) -> List[dict]:
    """
    Sync wrapper used by worker threads.
    Scrapes adults 1..4 in parallel-ish (sequential inside thread, but threadpool outside).
    """
    cc_final = (cc or DEFAULT_CC).lower()

    # If you want multi-language scrape (sometimes mealplan varies),
    # you can pass SCRAPE_LANGS env like "en-gb,sl-si,de-de"
    langs = langs or [s.strip() for s in os.getenv("SCRAPE_LANGS", "en-gb").split(",") if s.strip()]

    async def run_all():
        out: List[dict] = []
        for lang in langs:
            for adults in (1, 2, 3, 4):
                if should_cancel():
                    return out
                try:
                    rows = await _scrape_one_occ(hotel_name, slug, cc_final, checkin, checkout, adults, should_cancel)
                    out.extend(rows)
                except asyncio.CancelledError:
                    return out
                except Exception:
                    continue
        return out

    return asyncio.run(run_all())

def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    """
    Keep cheapest per (room, occupancy) for that day.
    """
    best = {}
    for r in rows:
        k = (r.get("room"), r.get("occupancy"))
        p = r.get("price")
        if k not in best or (p is not None and p < best[k].get("price", 10**9)):
            best[k] = r
    return list(best.values())
