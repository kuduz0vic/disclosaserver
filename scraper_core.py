# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright)
# - Country-aware (booking_cc)
# - Occupancy strict filtering (Max persons / Only for X guest)
# - Price extraction hardened (testids + currency-anchored parsing)
# - Rate attributes parsing: breakfast/dinner/half_board/refund/prepay
# - Variant-aware rate_key
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
import time
import datetime as dt
from datetime import timezone
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode

import requests
from requests import HTTPError
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ──────────────────────────────────────────────────────────────────────────────
# Env / Supabase basics
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY") or ""

DEFAULT_CC = os.getenv("DEFAULT_CC", "si").strip().lower()
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "0") == "1"

# Make sure you set this TRUE once you’re happy:
HEADLESS = os.getenv("HEADLESS", "1") == "1"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def supabase_headers(upsert: bool = False, json_pref: bool = True) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if json_pref:
        h["Accept"] = "application/json"
        h["Content-Type"] = "application/json"
    if upsert:
        # PostgREST upsert uses Prefer header; your worker adds it too
        h["Prefer"] = "resolution=merge-duplicates"
    return h

def _normalize_slug(s: str) -> str:
    t = (s or "").strip().lower()
    # accept full URL, hotel/<cc>/<slug>.html
    try:
        if t.startswith("http"):
            # cheap parse without urlparse to keep dependency light
            # examples:
            # https://www.booking.com/hotel/si/city-hotel-ljubljana.sl.html
            parts = t.split("/")
            if "hotel" in parts:
                i = parts.index("hotel")
                if i + 2 < len(parts):
                    slug_raw = parts[i + 2]
                    slug_raw = slug_raw.split("?")[0]
                    slug_raw = re.sub(r"\.[a-z]{2}(?:-[a-z]{2})?\.html?$", "", slug_raw, flags=re.I)
                    return slug_raw.strip().lower()
    except Exception:
        pass

    # "slug.xx.html"
    t = re.sub(r"\.[a-z]{2}(?:-[a-z]{2})?\.html?$", "", t, flags=re.I)
    t = t.replace(" ", "")
    return t

# Optional: if you ever want to resolve cc by probing Booking (not required if you store cc)
def resolve_cc_for_slug(slug: str) -> Optional[str]:
    # In production you should *not* do heavy probing here.
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Parsing rules
# ──────────────────────────────────────────────────────────────────────────────

MAX_PERSONS_RE = re.compile(r"max persons:\s*(\d+)", re.IGNORECASE)
ONLY_FOR_GUEST_RE = re.compile(r"only for\s+(\d+)\s+guest", re.IGNORECASE)

def parse_max_persons(text: str) -> Optional[int]:
    if not text:
        return None
    m = MAX_PERSONS_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def parse_only_for_guest(text: str) -> Optional[int]:
    if not text:
        return None
    m = ONLY_FOR_GUEST_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

# Currency-anchored parsing ONLY (prevents “1124” bugs from random numbers in cards)
CURRENCY_RE = re.compile(r"(€|\$|£)\s*([0-9][0-9\.,\s]*)")

def parse_price_str(s: str) -> Optional[float]:
    if not s:
        return None

    m = CURRENCY_RE.search(s)
    if not m:
        return None

    raw = m.group(2)
    raw = raw.replace("\u00a0", " ").strip()
    raw = raw.replace(" ", "")

    if "," in raw and "." in raw:
        # choose decimal by last occurrence
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "")
            raw = raw.replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        parts = raw.split(",")
        if len(parts[-1]) in (1, 2):
            raw = raw.replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "." in raw:
        parts = raw.split(".")
        if len(parts[-1]) not in (1, 2):
            raw = raw.replace(".", "")

    try:
        val = float(raw)
    except Exception:
        return None

    # sanity: avoids parsing “1.0” / “0.0” / “999999”
    if val < 5 or val > 50000:
        return None

    return val

def text_has_breakfast(text: str) -> bool:
    t = (text or "").lower()
    return "breakfast included" in t or "includes breakfast" in t

def text_has_dinner(text: str) -> bool:
    t = (text or "").lower()
    return "dinner included" in t or "includes dinner" in t

def text_has_half_board(text: str) -> bool:
    t = (text or "").lower()
    # Booking may say "Half board"
    return "half board" in t or "half-board" in t

def text_is_nonrefundable(text: str) -> bool:
    t = (text or "").lower()
    return "non-refundable" in t or "non refundable" in t or "nrf" in t

def text_has_free_cancellation(text: str) -> bool:
    t = (text or "").lower()
    return "free cancellation" in t or "cancel for free" in t

def text_has_prepay(text: str) -> bool:
    t = (text or "").lower()
    return "prepayment" in t or "pay now" in t or "you'll be charged" in t

def build_rate_key(
    breakfast: bool,
    free_cancel: Optional[bool],
    nonref: Optional[bool],
    prepay: Optional[bool],
    dinner: Optional[bool],
    half_board: Optional[bool],
) -> str:
    # small stable key used for variant uniqueness
    # Keep it deterministic + compact
    b = "b1" if breakfast else "b0"
    rc = "rc1" if free_cancel else "rc0" if free_cancel is False else "rc?"
    nrf = "nrf1" if nonref else "nrf0" if nonref is False else "nrf?"
    pp = "pp1" if prepay else "pp0" if prepay is False else "pp?"
    din = "din1" if dinner else "din0" if dinner is False else "din?"
    hb = "hb1" if half_board else "hb0" if half_board is False else "hb?"
    return f"{b}|{rc}|{nrf}|{pp}|{din}|{hb}"

# ──────────────────────────────────────────────────────────────────────────────
# Booking URL builder
# ──────────────────────────────────────────────────────────────────────────────

def build_booking_url(slug: str, cc: str, checkin: str, checkout: str, adults: int) -> str:
    base = f"https://www.booking.com/hotel/{cc}/{slug}.html"
    q = {
        "checkin": checkin,
        "checkout": checkout,
        "group_adults": adults,
        "group_children": 0,
        "no_rooms": 1,
        "selected_currency": "EUR",
        "lang": "en-gb",
        "sb_price_type": "total",
    }
    return f"{base}?{urlencode(q)}"

# ──────────────────────────────────────────────────────────────────────────────
# Core scrape
# ──────────────────────────────────────────────────────────────────────────────

# IMPORTANT:
# Booking’s DOM changes. We use multiple selectors and fallback strategies.
PRICE_TESTIDS = [
    "price-and-discounted-price",
    "recommended-price",
    "price-and-discounted-price--no-discount",
    "price-and-discounted-price--pay-now",
]

def _extract_price_from_card(card) -> Optional[float]:
    # 1) Try price testids first (most stable)
    for tid in PRICE_TESTIDS:
        els = card.query_selector_all(f"[data-testid='{tid}']")
        for el in els:
            p = parse_price_str(el.inner_text())
            if p is not None:
                return p

    # 2) fallback: look for any currency anchored text but from a smaller scope
    # Avoid scraping full page text.
    try:
        txt = card.inner_text()
    except Exception:
        txt = ""
    return parse_price_str(txt)

def _card_text(card) -> str:
    try:
        return (card.inner_text() or "").strip()
    except Exception:
        return ""

def scrape_hotel_for_dates(
    hotel_name: str,
    booking_slug: str,
    booking_cc: str,
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    """
    Returns list of rows with:
      hotel, slug, checkin, room, occupancy, price, flags..., rate_key
    """
    slug = _normalize_slug(booking_slug)
    cc = (booking_cc or DEFAULT_CC).strip().lower()

    out: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(
            locale="en-GB",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
        )
        page = ctx.new_page()

        try:
            for adults in (1, 2, 3, 4):
                if should_cancel and should_cancel():
                    break

                url = build_booking_url(slug, cc, checkin, checkout, adults)
                print(f"🏨 {hotel_name} | {checkin}→{checkout} | adults={adults}\n   {url}")

                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # wait for results area; booking can lazy load
                try:
                    page.wait_for_timeout(1200)
                except Exception:
                    pass

                # Try multiple likely containers
                cards = []
                for sel in [
                    "[data-testid='property-card']",
                    "[data-testid='property-card-container']",
                    "div[data-testid='availability-table']",
                    "div:has-text('Select a room')",
                ]:
                    try:
                        cards = page.query_selector_all(sel)
                        if cards:
                            break
                    except Exception:
                        continue

                # Fallback: find “room row-ish” blocks
                if not cards:
                    try:
                        cards = page.query_selector_all("table tbody tr")
                    except Exception:
                        cards = []

                if not cards:
                    continue

                # Parse room offers from each card
                for card in cards:
                    if should_cancel and should_cancel():
                        break

                    text = _card_text(card)
                    if not text:
                        continue

                    # Find room name: prefer explicit headings; fallback to first line
                    room_name = None
                    for rsel in [
                        "[data-testid='title']",
                        "h3",
                        "h2",
                        "div[data-testid='room-name']",
                    ]:
                        try:
                            el = card.query_selector(rsel)
                            if el:
                                t = (el.inner_text() or "").strip()
                                if t:
                                    room_name = t
                                    break
                        except Exception:
                            continue
                    if not room_name:
                        # fallback: first non-empty line
                        lines = [x.strip() for x in text.splitlines() if x.strip()]
                        if lines:
                            room_name = lines[0][:200]
                    if not room_name:
                        continue

                    # ✅ STRICT OCCUPANCY FILTER
                    only_for = parse_only_for_guest(text)
                    if only_for is not None and only_for != adults:
                        continue

                    max_p = parse_max_persons(text)
                    if max_p is not None and max_p < adults:
                        continue

                    # ✅ PRICE EXTRACTION (currency anchored only)
                    price = _extract_price_from_card(card)
                    if price is None:
                        continue

                    # Flags (best-effort from card text)
                    breakfast = text_has_breakfast(text)
                    dinner = text_has_dinner(text)
                    half_board = text_has_half_board(text)

                    free_cancel = True if text_has_free_cancellation(text) else None
                    nonref = True if text_is_nonrefundable(text) else None
                    prepay = True if text_has_prepay(text) else None

                    rate_plan_parts = []
                    if nonref:
                        rate_plan_parts.append("NRF")
                    if breakfast:
                        rate_plan_parts.append("w/ breakfast")
                    if dinner:
                        rate_plan_parts.append("w/ dinner")
                    if half_board:
                        rate_plan_parts.append("half board")
                    if free_cancel:
                        rate_plan_parts.append("free cancel")
                    if prepay:
                        rate_plan_parts.append("prepay")

                    rate_plan = " ".join(rate_plan_parts).strip() or None
                    rate_key = build_rate_key(
                        breakfast=breakfast,
                        free_cancel=free_cancel,
                        nonref=nonref,
                        prepay=prepay,
                        dinner=dinner,
                        half_board=half_board,
                    )

                    out.append(
                        {
                            "hotel": hotel_name,
                            "slug": f"{slug}__{cc}" if cc else slug,
                            "checkin": checkin,
                            "room": room_name,
                            "occupancy": adults,
                            "price": price,
                            "breakfast_included": breakfast or None,
                            "dinner_included": dinner or None,
                            "half_board": half_board or None,
                            "free_cancellation": free_cancel,
                            "nonrefundable": nonref,
                            "prepay_required": prepay,
                            "rate_plan": rate_plan,
                            "rate_key": rate_key,  # ✅ ALWAYS NON-NULL
                        }
                    )

        finally:
            try:
                ctx.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Deduping helpers
# ──────────────────────────────────────────────────────────────────────────────

def dedupe_min_per_room_and_occupancy(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep the cheapest row per (slug, checkin, room, occupancy, rate_key).
    """
    best: Dict[Tuple, Dict[str, Any]] = {}
    for r in rows or []:
        key = (
            (r.get("slug") or "").lower(),
            (r.get("checkin") or "")[:10],
            (r.get("room") or "").strip(),
            int(r.get("occupancy") or 0),
            (r.get("rate_key") or ""),
        )
        if not key[0] or not key[1] or not key[2] or not key[3]:
            continue
        try:
            price = float(r.get("price"))
        except Exception:
            continue

        if key not in best or price < float(best[key].get("price", 1e18)):
            best[key] = r
    return list(best.values())

# ──────────────────────────────────────────────────────────────────────────────
# These are used by the worker to fetch hotels. Leave as-is if your worker already works.
# If you want, we can later unify this with your RPCs, but you said selection works now.
# ──────────────────────────────────────────────────────────────────────────────

def _get_user_links(user_id: str) -> List[Dict[str, Any]]:
    """
    Expected API response rows should provide at least:
      link_type, hotel_id, name, booking_slug, booking_cc
    In your project you already have working worker logic; we keep this minimal.
    """
    # You likely have a working endpoint already – keep your existing version if needed.
    # This fallback tries a very simple PostgREST select that works ONLY if your relations exist.
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/user_hotels",
            params={"select": "hotel_id,link_type,hotels(name),hotel_profiles(booking_slug,booking_cc)", "user_id": f"eq.{user_id}"},
            headers=supabase_headers(json_pref=False),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or []
        out = []
        for row in data:
            out.append({
                "hotel_id": row.get("hotel_id"),
                "link_type": row.get("link_type"),
                "name": (row.get("hotels") or {}).get("name"),
                "booking_slug": (row.get("hotel_profiles") or {}).get("booking_slug"),
                "booking_cc": (row.get("hotel_profiles") or {}).get("booking_cc"),
                "url": None,
            })
        return out
    except Exception:
        return []

def get_own_hotel(user_id: str) -> Optional[Dict[str, Any]]:
    links = _get_user_links(user_id)
    if not links:
        return None
    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {"hotel_id": l["hotel_id"], "name": l.get("name") or "My Hotel", "slug": slug, "cc": cc}
    # fallback: first link
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {"hotel_id": first["hotel_id"], "name": first.get("name") or "My Hotel", "slug": slug, "cc": cc}

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l.get("hotel_id") == own_hotel_id:
            continue
        if l.get("link_type") not in ("competitor", "own"):
            continue
        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({"hotel_id": l.get("hotel_id"), "name": l.get("name") or "", "slug": slug, "cc": cc})
    # ensure competitors only if you want:
    return [x for x in out if x.get("hotel_id") != own_hotel_id]
