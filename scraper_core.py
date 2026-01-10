# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) + Supabase helpers
#
# Goals (for Rativa/Yield):
# - Scrape Booking availability/price table reliably (classic hprt table layout)
# - Strictly scrape per occupancy (adults=1..4)
# - Extract rate attributes per *variant row* (breakfast/dinner/half-board/refund/prepay)
# - Preserve multiple variants via rate_key
# - Be robust across Booking country codes (booking_cc) + store keyed slug: "<slug>__<cc>"
#
# IMPORTANT DB NOTE
# - If your table room_prices_raw still has UNIQUE(user_id,slug,checkin,room,occupancy)
#   you CANNOT store multiple variants per room/occupancy.
#   Use the provided SQL migration to switch unique constraint to include rate_key.
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode, urlparse

import requests
from requests import HTTPError
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ──────────────────────────────────────────────────────────────────────────────
# Env
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
)
assert SUPABASE_URL and SUPABASE_KEY, "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"

DEFAULT_CC = (os.getenv("DEFAULT_CC", "si") or "si").lower()
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "0") == "1"
COMMON_CCS = (
    os.getenv("COMMON_CCS")
    or "si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr"
).split(",")

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "30000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "10000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))
HEADLESS = os.getenv("HEADLESS", "1") == "1"

USER_AGENT = (
    os.getenv("USER_AGENT")
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────────────────────────────────────────────────────────
# Slug helpers (country-aware selection)
# ──────────────────────────────────────────────────────────────────────────────
def to_scrape_slug(base_slug: str, cc: str | None) -> str:
    base = (base_slug or "").strip().lower()
    cc_norm = (cc or "").strip().lower()
    return f"{base}__{cc_norm}" if (base and cc_norm) else base

def split_scrape_slug(scrape_slug: str) -> tuple[str, str | None]:
    s = (scrape_slug or "").strip().lower()
    if "__" in s:
        base, cc = s.split("__", 1)
        return base, (cc or None)
    return s, None

# ──────────────────────────────────────────────────────────────────────────────
# Supabase headers
# ──────────────────────────────────────────────────────────────────────────────
def supabase_headers(json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if json_pref:
        h["Content-Type"] = "application/json"
        h["Accept"] = "application/json"
    if upsert:
        # PostgREST upsert
        h["Prefer"] = "resolution=merge-duplicates,return=representation"
    return h

# ──────────────────────────────────────────────────────────────────────────────
# User hotel discovery (own + competitors)
# IMPORTANT: Avoid schema-cache relationship errors by NOT using nested embeds.
# We query in 2-3 simple calls: user_hotels -> hotels -> hotel_profiles.
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_slug(v: str) -> str:
    if not v:
        return ""
    t = v.strip()
    if t.startswith("http"):
        try:
            u = urlparse(t)
            parts = [p for p in u.path.split("/") if p]
            # /hotel/<cc>/<slug>.html
            if len(parts) >= 3 and parts[0].lower() == "hotel":
                slug = parts[2]
                return re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", slug, flags=re.I).replace(" ", "")
        except Exception:
            pass
    t = re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", t, flags=re.I)
    return t.replace(" ", "")

def _rest_get(path: str, params: Dict[str, Any], timeout: float = 20.0) -> List[dict]:
    url = f"{SUPABASE_URL}{path}"
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=timeout)
    r.raise_for_status()
    return r.json() or []

def _get_user_links(user_id: str) -> List[dict]:
    """
    Returns list of:
      {
        hotel_id, link_type, inserted_at,
        name, booking_slug, booking_cc
      }
    """
    # 1) user_hotels
    links = _rest_get(
        "/rest/v1/user_hotels",
        {
            "select": "hotel_id,link_type,inserted_at",
            "user_id": f"eq.{user_id}",
            "order": "inserted_at.asc",
        },
    )
    if not links:
        return []

    hotel_ids = [l.get("hotel_id") for l in links if l.get("hotel_id")]
    hotel_ids = [hid for hid in hotel_ids if isinstance(hid, str)]
    if not hotel_ids:
        return []

    # 2) hotels
    hotels = _rest_get(
        "/rest/v1/hotels",
        {
            "select": "id,name",
            "id": f"in.({','.join(hotel_ids)})",
        },
    )
    hotels_by_id = {h["id"]: h for h in hotels if h.get("id")}

    # 3) hotel_profiles (can be 0 or >1; we choose the newest inserted_at if present)
    profiles = _rest_get(
        "/rest/v1/hotel_profiles",
        {
            "select": "hotel_id,booking_slug,booking_cc,inserted_at",
            "hotel_id": f"in.({','.join(hotel_ids)})",
            "order": "inserted_at.desc.nullslast",
        },
    )
    prof_by_hotel: Dict[str, dict] = {}
    for p in profiles:
        hid = p.get("hotel_id")
        if hid and hid not in prof_by_hotel:
            prof_by_hotel[hid] = p

    out: List[dict] = []
    for l in links:
        hid = l.get("hotel_id")
        h = hotels_by_id.get(hid, {}) if hid else {}
        p = prof_by_hotel.get(hid, {}) if hid else {}
        out.append({
            "hotel_id": hid,
            "link_type": (l.get("link_type") or "").strip(),
            "inserted_at": l.get("inserted_at"),
            "name": (h.get("name") or "").strip(),
            "booking_slug": (p.get("booking_slug") or "").strip(),
            "booking_cc": (p.get("booking_cc") or "").strip().lower() or "",
        })
    return out

def get_own_hotel(user_id: str):
    links = _get_user_links(user_id)
    if not links:
        return None
    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {
                    "hotel_id": l["hotel_id"],
                    "name": l["name"] or "My Hotel",
                    "slug": slug,
                    "cc": cc,
                }
    # fallback: first link
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {"hotel_id": first["hotel_id"], "name": first["name"] or "My Hotel", "slug": slug, "cc": cc}

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l.get("hotel_id") == own_hotel_id:
            continue
        if (l.get("link_type") or "").strip().lower() != "competitor":
            continue
        slug = _normalize_slug(l.get("booking_slug") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({"hotel_id": l["hotel_id"], "name": l["name"] or "", "slug": slug, "cc": cc})
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Country resolver (optional)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=512)
def resolve_cc_for_slug(slug: str, timeout: float = 4.0) -> Optional[str]:
    if not slug:
        return None
    s = requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-GB,en;q=0.9"}
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

# ──────────────────────────────────────────────────────────────────────────────
# Scraper helpers
# ──────────────────────────────────────────────────────────────────────────────
EXPAND_SELECTORS = [
    "button:has-text('Show all')", "button:has-text('Show more')",
    "button:has-text('See all rooms')", "a:has-text('Show all')",
    "a:has-text('Show more')", "a:has-text('See all rooms')",
]
ROOM_TABLE_SELECTORS = ["#hprt-table", "table.hprt-table", "[data-testid='hprt-table']"]
PRICE_SELECTORS = [
    ".prco-valign-middle-helper",
    ".bui-price-display__value",
    "[data-testid='price-and-discounted-price']",
    "[data-testid='recommended-price']",
    ".prco-inline-price",
]
OCC_PATTERNS = [
    r"\bfor\s+(\d+)\s+adults?\b",
    r"\bprice\s+for\s+(\d+)\s+adults?\b",
    r"\b(\d+)\s+adults?\b",
    r"\bfor\s+(\d+)\s+people\b",
    r"\b(\d+)\s+guests?\b",
    r"\bsleeps\s+(\d+)\b",
]

# Currency anchored extraction to avoid "1124" from random numbers
_CURRENCY_PATTERNS = [
    r"(?:€|eur)\s*([0-9][0-9\s\.,]+)",
    r"([0-9][0-9\s\.,]+)\s*(?:€|eur)",
    r"(?:\$|usd)\s*([0-9][0-9\s\.,]+)",
    r"([0-9][0-9\s\.,]+)\s*(?:\$|usd)",
    r"(?:£|gbp)\s*([0-9][0-9\s\.,]+)",
    r"([0-9][0-9\s\.,]+)\s*(?:£|gbp)",
]

def _parse_price_number(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = raw.strip()
    s = re.sub(r"[^0-9,\.]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
        # decimal is the last separator
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        parts = s.split(",")
        if len(parts[-1]) == 2:
            s = ".".join(parts)
        else:
            s = "".join(parts)
    else:
        if s.count(".") > 1:
            s = s.replace(".", "")
    try:
        return float(s)
    except Exception:
        return None

def clean_price(text: str) -> Optional[float]:
    """
    Extract a monetary price from text, avoiding unrelated numbers (dates, room size, etc.).
    - Only accepts currency-anchored candidates.
    - If multiple candidates exist, chooses the LAST one in the string (usually "Price € 163").
    """
    if not text:
        return None
    txt = text.replace("\xa0", " ").strip()
    candidates: List[float] = []
    for pat in _CURRENCY_PATTERNS:
        for mm in re.finditer(pat, txt, flags=re.I):
            val = _parse_price_number(mm.group(1))
            if val is not None:
                candidates.append(val)
    if not candidates:
        return None
    price = candidates[-1]
    if price < 5 or price > 50000:
        return None
    return price

def find_adults_in_text(text: str) -> Optional[int]:
    if not text:
        return None
    t = " ".join(text.split()).lower()
    for pat in OCC_PATTERNS:
        m = re.search(pat, t)
        if m:
            try:
                n = int(m.group(1))
                if 1 <= n <= 8:
                    return n
            except Exception:
                pass
    return None

def row_level_occupancy_hint(row) -> Optional[int]:
    try:
        txt = row.inner_text() or ""
        return find_adults_in_text(txt)
    except Exception:
        return None

def get_occupancy_from_name(name: str) -> int:
    n = (name or "").lower()
    if "quadruple" in n or "family" in n:
        return 4
    if "triple" in n:
        return 3
    if "single" in n:
        return 1
    return 2

def is_valid_room_name(name: str) -> bool:
    n = (name or "").lower()
    return not any(x in n for x in ["review", "score", "rating", "availability"])

def build_hotel_url(cc: Optional[str], slug: str, checkin: str, checkout: str, adults: int, lang="en-gb") -> str:
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
        "sb_price_type": "total",
    }
    return f"{base}?{urlencode(qs)}"

# ──────────────────────────────────────────────────────────────────────────────
# Attribute parsing (per variant row)
# ──────────────────────────────────────────────────────────────────────────────
def parse_rate_attributes_from_row(row) -> Dict[str, Optional[bool]]:
    flags: Dict[str, Optional[bool]] = {
        "breakfast_included": None,
        "dinner_included": None,
        "half_board": None,
        "free_cancellation": None,
        "nonrefundable": None,
        "prepay_required": None,
        "rate_plan": None,
    }

    text_chunks: List[str] = []

    # primary conditions cell
    try:
        cond_el = row.query_selector("td.hprt-table-cell-conditions")
        if cond_el:
            txt = cond_el.inner_text() or ""
            if txt.strip():
                text_chunks.append(txt)
    except Exception:
        pass

    # extra likely sources
    for sel in [
        "[data-testid='cancellation-policy']",
        "[data-testid='pricing-subtitle']",
        "[data-testid='meal-plan']",
        "[data-testid*='meal']",
    ]:
        try:
            el = row.query_selector(sel)
            if el:
                txt = el.inner_text() or ""
                if txt.strip():
                    text_chunks.append(txt)
        except Exception:
            pass

    blob = " ".join(text_chunks).strip()

    if not blob or not re.search(
        r"breakfast|dinner|half[-\s]?board|all[-\s]?inclusive|cancellation|refundable|prepay|pay in advance",
        blob,
        re.IGNORECASE,
    ):
        try:
            fallback = (row.inner_text() or "").strip()
        except Exception:
            fallback = ""
        if fallback:
            blob = (blob + " " + fallback).strip() if blob else fallback

    t = " ".join(blob.split()).lower()

    # meals
    if re.search(r"breakfast.{0,20}included", t) or re.search(r"\bwith breakfast\b", t):
        flags["breakfast_included"] = True
    elif re.search(r"\bbreakfast\b", t) and re.search(r"not included|extra charge|fee|surcharge|per person|pp\b", t):
        flags["breakfast_included"] = False

    breakfast_and_dinner = bool(
        re.search(r"breakfast.{0,40}dinner.{0,20}included", t)
        or re.search(r"dinner.{0,40}breakfast.{0,20}included", t)
    )
    dinner_included = bool(
        re.search(r"dinner.{0,10}included", t)
        or re.search(r"evening meal.{0,10}included", t)
    )
    half_board = bool(re.search(r"\bhalf[-\s]?board\b|\bhb\b", t))
    all_inclusive = bool(re.search(r"\ball[-\s]?inclusive\b", t))

    if dinner_included or breakfast_and_dinner or all_inclusive or half_board:
        flags["dinner_included"] = True
    if half_board or breakfast_and_dinner:
        flags["half_board"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        if flags["dinner_included"] is None:
            flags["dinner_included"] = True

    # cancellation
    nonref = bool(
        re.search(r"\bnon[-\s]?refundable\b|\bno refund\b|\btotal cost to cancel\b|\bnrf\b", t)
    )
    free_canc = bool(re.search(r"\bfree cancellation\b|\bfully refundable\b|\bfree to cancel\b", t) and not nonref)

    if nonref:
        flags["nonrefundable"] = True
    if free_canc:
        flags["free_cancellation"] = True
        if flags["nonrefundable"] is None:
            flags["nonrefundable"] = False

    # prepay
    prepay = bool(re.search(r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bpay now\b|\bcharged in advance\b", t))
    if prepay:
        flags["prepay_required"] = True

    # rate plan label
    if flags["nonrefundable"]:
        if flags["half_board"] or flags["dinner_included"]:
            flags["rate_plan"] = "NRF half-board"
        elif flags["breakfast_included"]:
            flags["rate_plan"] = "NRF w/ breakfast"
        else:
            flags["rate_plan"] = "Non-refundable"
    elif flags["free_cancellation"]:
        if flags["half_board"] or flags["dinner_included"]:
            flags["rate_plan"] = "Free cancel + half-board"
        elif flags["breakfast_included"]:
            flags["rate_plan"] = "Free cancel + breakfast"
        else:
            flags["rate_plan"] = "Free cancellation"
    elif flags["half_board"] or flags["dinner_included"]:
        flags["rate_plan"] = "Half board"
    elif flags["breakfast_included"]:
        flags["rate_plan"] = "Breakfast"

    return flags

def build_rate_key(flags: dict) -> str:
    """
    Stable fingerprint for a rate variant (ternary state so unknowns don't collide).
    """
    def tri(v, t):
        return f"{t}1" if v is True else (f"{t}0" if v is False else f"{t}?")
    return "|".join([
        tri(flags.get("breakfast_included"), "b"),
        tri(flags.get("free_cancellation"), "rc"),
        tri(flags.get("nonrefundable"), "nrf"),
        tri(flags.get("prepay_required"), "pp"),
        tri(flags.get("dinner_included"), "din"),
        tri(flags.get("half_board"), "hb"),
    ])

def extract_price_with_context(price_el) -> Tuple[Optional[float], Optional[int]]:
    try:
        price = clean_price(price_el.inner_text())
    except Exception:
        return None, None
    if price is None:
        return None, None

    # small DOM context for occupancy hints
    try:
        context_text = price_el.evaluate(
            """(el) => {
                let txt = '';
                let node = el;
                for (let i=0; i<3 && node; i++){
                  txt += ' ' + (node.innerText || '');
                  node = node.parentElement;
                }
                return txt;
            }"""
        )
    except Exception:
        context_text = ""
    occ = find_adults_in_text(context_text)
    return price, occ

def _is_room_name_row(row) -> Optional[str]:
    """Return room name if the row contains a room-type/name element."""
    try:
        name_el = (
            row.query_selector(".hprt-roomtype-icon-link")
            or row.query_selector(".hprt-roomtype-name")
            or row.query_selector("[data-testid='room-name']")
        )
        if not name_el:
            return None
        nm = (name_el.inner_text() or "").strip()
        if not nm or not is_valid_room_name(nm):
            return None
        return re.sub(r"\s+", " ", nm)
    except Exception:
        return None

def collect_room_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    """
    Classic Booking table logic:
      - a room-name row
      - followed by 1..N variant rows
    We parse flags per variant row and collect prices that correspond to the current adults.
    """
    results: List[Dict[str, Any]] = []

    table = None
    for table_sel in ROOM_TABLE_SELECTORS:
        table = page.query_selector(table_sel)
        if table:
            break
    if not table:
        return results

    trs = table.query_selector_all("tr")
    current_room: Optional[str] = None
    current_room_occ_hint: Optional[int] = None

    for tr in trs:
        try:
            nm = _is_room_name_row(tr)
            if nm:
                current_room = nm
                current_room_occ_hint = row_level_occupancy_hint(tr)
                continue

            if not current_room:
                continue

            attrs = parse_rate_attributes_from_row(tr)
            rate_key = build_rate_key(attrs)

            row_occ = row_level_occupancy_hint(tr) or current_room_occ_hint

            found_any_price = False
            for psel in PRICE_SELECTORS:
                for pel in tr.query_selector_all(psel):
                    price, occ_ctx = extract_price_with_context(pel)
                    if price is None:
                        continue

                    if occ_ctx is not None:
                        occ_final = occ_ctx
                    elif row_occ is not None:
                        occ_final = row_occ
                    else:
                        occ_final = get_occupancy_from_name(current_room)

                    if occ_final != adults:
                        continue

                    results.append({
                        "room": current_room,
                        "price": price,
                        "occupancy": adults,
                        "breakfast_included": attrs.get("breakfast_included"),
                        "dinner_included": attrs.get("dinner_included"),
                        "half_board": attrs.get("half_board"),
                        "free_cancellation": attrs.get("free_cancellation"),
                        "nonrefundable": attrs.get("nonrefundable"),
                        "prepay_required": attrs.get("prepay_required"),
                        "rate_plan": attrs.get("rate_plan"),
                        "rate_key": rate_key,
                    })
                    found_any_price = True

            # fallback: row inner_text price (still currency-anchored)
            if not found_any_price:
                inferred = get_occupancy_from_name(current_room)
                if inferred != adults:
                    continue
                txt = ""
                try:
                    txt = tr.inner_text() or ""
                except Exception:
                    txt = ""
                first_price = clean_price(txt)
                if first_price is not None:
                    results.append({
                        "room": current_room,
                        "price": first_price,
                        "occupancy": adults,
                        "breakfast_included": attrs.get("breakfast_included"),
                        "dinner_included": attrs.get("dinner_included"),
                        "half_board": attrs.get("half_board"),
                        "free_cancellation": attrs.get("free_cancellation"),
                        "nonrefundable": attrs.get("nonrefundable"),
                        "prepay_required": attrs.get("prepay_required"),
                        "rate_plan": attrs.get("rate_plan"),
                        "rate_key": rate_key,
                    })
        except Exception:
            continue

    return results

def aggressively_expand_and_scroll(page, should_cancel: Optional[Callable[[], bool]] = None):
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    # cookie accept (best-effort)
    try:
        if cancelled():
            return
        page.wait_for_selector("#onetrust-accept-btn-handler", timeout=3000)
        page.click("#onetrust-accept-btn-handler")
    except Exception:
        pass

    # click expanders
    for _ in range(3):
        if cancelled():
            return
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=400)
                page.wait_for_timeout(120)
            except Exception:
                pass

    # scroll
    last_h = 0
    for _ in range(SCROLL_PASSES):
        if cancelled():
            return
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(500)
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=300)
            except Exception:
                pass
        try:
            h = page.evaluate("document.body.scrollHeight")
        except Exception:
            h = last_h
        if h == last_h:
            break
        last_h = h

def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    """
    Returns raw rows (may contain multiple variants), keyed slug is applied by the worker.
    """
    out: List[dict] = []

    with sync_playwright() as p:
        extra_args = ["--no-sandbox"] if os.getenv("NO_SANDBOX") == "1" else []
        browser = p.chromium.launch(headless=HEADLESS, args=extra_args)
        context = browser.new_context(
            locale="en-GB",
            viewport={"width": 1400, "height": 900},
            user_agent=USER_AGENT,
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
        )
        context.add_init_script(
            "Object.defineProperty(navigator,'language',{get:()=> 'en-GB'});"
            "Object.defineProperty(navigator,'languages',{get:()=> ['en-GB','en']});"
        )

        # block heavy assets
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        page = context.new_page()

        def cancelled() -> bool:
            return bool(should_cancel and should_cancel())

        for adults in (1, 2, 3, 4):
            if cancelled():
                break
            cc_eff = (cc or "").strip().lower() or None
            if not cc_eff and RESOLVE_CC_IF_MISSING:
                cc_eff = resolve_cc_for_slug(slug) or DEFAULT_CC
            if not cc_eff:
                cc_eff = DEFAULT_CC

            url = build_hotel_url(cc_eff, slug, checkin, checkout, adults, lang="en-gb")
            print(f"🏨 {name} | {checkin}→{checkout} | adults={adults}\n   {url}")
            try:
                page.goto(url, timeout=PAGE_GOTO_TIMEOUT_MS)
            except PWTimeoutError:
                print("⚠️ Timeout loading hotel page.")
                continue

            if cancelled():
                break

            try:
                aggressively_expand_and_scroll(page, should_cancel=should_cancel)
                if cancelled():
                    break

                try:
                    page.wait_for_selector(",".join(ROOM_TABLE_SELECTORS), timeout=WAIT_TABLE_TIMEOUT_MS)
                except PWTimeoutError:
                    print("⚠️ No room table found.")
                    continue

                rows = collect_room_rows_for_adults(page, adults)
                for r in rows:
                    out.append({
                        "hotel": name,
                        "slug": slug.lower(),  # keyed slug is done in worker (slug__cc)
                        "checkin": checkin,
                        "room": r["room"],
                        "occupancy": r["occupancy"],
                        "price": r["price"],
                        "breakfast_included": r.get("breakfast_included"),
                        "dinner_included": r.get("dinner_included"),
                        "half_board": r.get("half_board"),
                        "free_cancellation": r.get("free_cancellation"),
                        "nonrefundable": r.get("nonrefundable"),
                        "prepay_required": r.get("prepay_required"),
                        "rate_plan": r.get("rate_plan"),
                        "rate_key": r.get("rate_key") or "b?|rc?|nrf?|pp?|din?|hb?",  # never null
                    })
            except Exception as e:
                print("⚠️ Page error:", e)
                continue

        try:
            browser.close()
        except Exception:
            pass

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Post-processing: keep min per (slug, checkin, room, occupancy, rate_key)
# ──────────────────────────────────────────────────────────────────────────────
def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    best: Dict[tuple, dict] = {}
    for r in rows:
        slug_lower = (r.get("slug") or "").lower()
        rate_key = (r.get("rate_key") or "").strip() or "b?|rc?|nrf?|pp?|din?|hb?"
        key = (slug_lower, (r.get("checkin") or "")[:10], r.get("room"), int(r.get("occupancy") or 0), rate_key)
        price = r.get("price")
        if not key[0] or not key[1] or not key[2] or key[3] <= 0:
            continue
        try:
            p = float(price)
        except Exception:
            continue
        if p < 5 or p > 50000:
            continue
        if key not in best or p < float(best[key]["price"]):
            best[key] = {
                "hotel": r.get("hotel"),
                "slug": slug_lower,
                "checkin": key[1],
                "room": str(key[2]),
                "occupancy": key[3],
                "price": p,
                "breakfast_included": r.get("breakfast_included"),
                "dinner_included": r.get("dinner_included"),
                "half_board": r.get("half_board"),
                "free_cancellation": r.get("free_cancellation"),
                "nonrefundable": r.get("nonrefundable"),
                "prepay_required": r.get("prepay_required"),
                "rate_plan": r.get("rate_plan"),
                "rate_key": rate_key,
            }
    return list(best.values())
