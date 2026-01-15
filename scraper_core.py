# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) — Variant-aware + strict occupancy
#
# Fixes in this version:
#  1) Meal flags (breakfast / dinner / half_board) are parsed from a *per-offer*
#     blob gathered around each price element (not only from row.inner_text()).
#     This reliably captures "Breakfast & dinner included" offers on Occidental.
#  2) STRICT OCCUPANCY is enforced per offer using (in order):
#        - "Only for X guest" (offer text)
#        - occupancy icons in the offer row (occupancy cell)
#        - "Sleeps: A - B guests" / "Sleeps: B guests"
#        - "Max persons: X"
#     We require capacity == adults (exact) to avoid 2-room combos and to keep
#     1-person results strictly from true single-guest offers (no double rooms).
#  3) "Breakfast & dinner included" is treated as half_board=True (and dinner=True).
#
# Optional debug:
#   - set MEAL_DEBUG=1 to print offer blobs that contain meal keywords
#   - set OFFER_DEBUG=1 to print capacity + flags decisions per offer
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError

# ──────────────────────────────────────────────────────────────────────────────
# Env / Supabase basics (used by worker; keep here)
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

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "45000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "12000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))
HEADLESS = os.getenv("HEADLESS", "1") == "1"

MEAL_DEBUG = os.getenv("MEAL_DEBUG", "0") == "1"
OFFER_DEBUG = os.getenv("OFFER_DEBUG", "0") == "1"

USER_AGENT = (
    os.getenv("USER_AGENT")
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────────────────────────────────────────────────────────
# Supabase headers (kept for worker compatibility)
# ──────────────────────────────────────────────────────────────────────────────
def supabase_headers(json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if json_pref:
        h["Content-Type"] = "application/json"
        h["Accept"] = "application/json"
    if upsert:
        h["Prefer"] = "resolution=merge-duplicates,return=representation"
    return h

# ──────────────────────────────────────────────────────────────────────────────
# Slug normalization
# ──────────────────────────────────────────────────────────────────────────────
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
                slug = slug.split("?")[0]
                return re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", slug, flags=re.I).replace(" ", "")
        except Exception:
            pass
    t = re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", t, flags=re.I)
    return t.replace(" ", "")

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
# Booking URL builder
# ──────────────────────────────────────────────────────────────────────────────
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
# Robust price parsing (currency-anchored)
# ──────────────────────────────────────────────────────────────────────────────
_CURRENCY_RE = re.compile(r"(€|\$|£)\s*([0-9][0-9\s\.,]+)")
def _parse_price_number(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = raw.replace("\xa0", " ").strip()
    s = re.sub(r"[^0-9,\. ]", "", s)
    s = s.replace(" ", "")
    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        parts = s.split(",")
        if len(parts[-1]) in (1, 2):
            s = ".".join(parts)
        else:
            s = "".join(parts)
    else:
        if s.count(".") > 1:
            s = s.replace(".", "")

    try:
        v = float(s)
    except Exception:
        return None

    if v < 5 or v > 50000:
        return None
    return v

def clean_price(text: str) -> Optional[float]:
    if not text:
        return None
    txt = text.replace("\xa0", " ").strip()
    m = _CURRENCY_RE.search(txt)
    if not m:
        return None
    return _parse_price_number(m.group(2))

# ──────────────────────────────────────────────────────────────────────────────
# Variant attribute parsing (EN-first, tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def parse_rate_attributes(text: str) -> Dict[str, Optional[bool]]:
    """
    Returns flags (True/False/None). Note:
    - We treat "Breakfast & dinner included" as half_board=True.
    - We do NOT attempt to distinguish "dinner only" vs half-board reliably.
    """
    t = " ".join((text or "").split()).lower()

    flags: Dict[str, Optional[bool]] = {
        "breakfast_included": None,
        "dinner_included": None,
        "half_board": None,
        "free_cancellation": None,
        "nonrefundable": None,
        "prepay_required": None,
        "rate_plan": None,
    }

    # Breakfast included vs paid
    if re.search(r"\bbreakfast\b.{0,30}\bincluded\b", t) or re.search(r"\bwith breakfast\b", t) or "includes breakfast" in t:
        flags["breakfast_included"] = True
    elif "breakfast" in t and re.search(
        r"not included|extra charge|for an extra fee|surcharge|per person|pp\b|optional|\beur\b|€|\bgood breakfast\b",
        t,
    ):
        flags["breakfast_included"] = False

    # Dinner / half-board
    # IMPORTANT: treat "breakfast & dinner included" as half-board.
    has_bd = ("breakfast & dinner included" in t) or ("breakfast and dinner included" in t) or bool(re.search(r"\bbreakfast\b.{0,80}\bdinner\b.{0,40}\bincluded\b", t))
    has_hb_word = bool(re.search(r"\bhalf[-\s]?board\b|\bhb\b|halvpension|halbpension|polpenzion|polpansion", t))
    has_ai = bool(re.search(r"\ball[-\s]?inclusive\b", t))
    has_dinner_included = bool(re.search(r"\bdinner\b.{0,40}\bincluded\b", t)) or bool(re.search(r"\bincludes\b.{0,40}\bdinner\b", t))

    if has_hb_word or has_bd:
        flags["half_board"] = True
        flags["dinner_included"] = True
        # breakfast is part of HB in practice; if not explicitly false, set true
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
    elif has_ai or has_dinner_included:
        flags["dinner_included"] = True

    # Cancellation
    nonref = bool(re.search(r"\bnon[-\s]?refundable\b|\bno refund\b|\btotal cost to cancel\b|\bnrf\b", t))
    free_canc = bool(re.search(r"\bfree cancellation\b|\bfully refundable\b|\bfree to cancel\b|\bcancel for free\b", t)) and not nonref
    if nonref:
        flags["nonrefundable"] = True
        if flags["free_cancellation"] is None:
            flags["free_cancellation"] = False
    if free_canc:
        flags["free_cancellation"] = True
        if flags["nonrefundable"] is None:
            flags["nonrefundable"] = False

    # Prepay
    if re.search(r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bpay now\b|\bcharged in advance\b|payment before arrival|pay the property before arrival", t):
        flags["prepay_required"] = True
    if "no prepayment needed" in t or "no prepayment" in t or "pay at the property" in t:
        if flags["prepay_required"] is None:
            flags["prepay_required"] = False

    # Rate plan label
    parts = []
    if flags["nonrefundable"] is True:
        parts.append("NRF")
    if flags["free_cancellation"] is True:
        parts.append("Free cancel")
    if flags["prepay_required"] is True:
        parts.append("Prepay")
    if flags["half_board"] is True:
        parts.append("Half-board")
    else:
        if flags["breakfast_included"] is True:
            parts.append("Breakfast")
        if flags["dinner_included"] is True:
            parts.append("Dinner")
    flags["rate_plan"] = " + ".join(parts) if parts else None

    return flags

def build_rate_key(flags: dict) -> str:
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

# ──────────────────────────────────────────────────────────────────────────────
# DOM helpers
# ──────────────────────────────────────────────────────────────────────────────
EXPAND_SELECTORS = [
    "button:has-text('Show all')","button:has-text('Show more')",
    "button:has-text('See all rooms')","a:has-text('Show all')",
    "a:has-text('Show more')","a:has-text('See all rooms')",
]
ROOM_TABLE_SELECTORS = [
    "#hprt-table",
    "table.hprt-table",
    "[data-testid='hprt-table']",
    "[data-testid='availability-table']",
]

PRICE_SELECTORS = [
    "[data-testid='price-and-discounted-price']",
    "[data-testid='price-and-discounted-price--no-discount']",
    "[data-testid='recommended-price']",
    "[data-testid='price-and-discounted-price--pay-now']",
    ".prco-valign-middle-helper",
    ".bui-price-display__value",
    ".prco-inline-price",
]

def aggressively_expand_and_scroll(page, should_cancel: Optional[Callable[[], bool]] = None):
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    for sel in ["#onetrust-accept-btn-handler", "button#onetrust-accept-btn-handler"]:
        try:
            if cancelled(): return
            page.locator(sel).first.click(timeout=2000)
        except Exception:
            pass

    for _ in range(3):
        if cancelled(): return
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=500)
                page.wait_for_timeout(150)
            except Exception:
                pass

    last_h = 0
    for _ in range(SCROLL_PASSES):
        if cancelled(): return
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(600)
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=400)
            except Exception:
                pass
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

# ──────────────────────────────────────────────────────────────────────────────
# Strict occupancy helpers
# ──────────────────────────────────────────────────────────────────────────────
_MAX_PERSONS_RE = re.compile(r"max persons:\s*(\d+)", re.IGNORECASE)
_ONLY_FOR_GUEST_RE = re.compile(r"only for\s+(\d+)\s+guest", re.IGNORECASE)
# Booking sometimes uses "Sleeps: 1 - 2 guests" or "Sleeps: 2 guests"
_SLEEPS_RANGE_RE = re.compile(r"sleeps:\s*(\d+)\s*[-–]\s*(\d+)\s*guests?", re.IGNORECASE)
_SLEEPS_EXACT_RE = re.compile(r"sleeps:\s*(\d+)\s*guests?", re.IGNORECASE)

def _extract_max_persons_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    m = _MAX_PERSONS_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _extract_only_for_guest(text: str) -> Optional[int]:
    if not text:
        return None
    m = _ONLY_FOR_GUEST_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _extract_sleeps_capacity(text: str) -> Optional[int]:
    if not text:
        return None
    m = _SLEEPS_RANGE_RE.search(text)
    if m:
        try:
            lo = int(m.group(1)); hi = int(m.group(2))
            # For our strict rules, the *offer* is usually for the max in that row.
            # (e.g. "Sleeps: 1 - 2 guests" is a 2-capacity offer)
            return max(lo, hi)
        except Exception:
            pass
    m2 = _SLEEPS_EXACT_RE.search(text)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            pass
    return None

def _count_person_icons_in_node(node) -> Optional[int]:
    try:
        return node.evaluate(
            """(el) => {
              const sels = [
                "i.bicon-occupancy", "i.bicon-person", "span.bicon-occupancy",
                "svg[aria-label*='person']", "svg[aria-label*='guest']",
                "[data-testid*='occupancy'] svg", "[data-testid*='occupancy'] i",
                "[data-testid='occupancy-icon']",
              ];
              let c = 0;
              for (const sel of sels){
                const els = el.querySelectorAll(sel);
                if (els && els.length) c = Math.max(c, els.length);
              }
              return c || null;
            }"""
        )
    except Exception:
        return None

def _variant_row_occupancy_icons(variant_row) -> Optional[int]:
    """Prefer occupancy icons in the variant row itself (not the room header row)."""
    try:
        occ_cell = (
            variant_row.query_selector("td.hprt-table-cell-occupancy")
            or variant_row.query_selector("[data-testid*='occupancy']")
        )
        if occ_cell:
            c = _count_person_icons_in_node(occ_cell)
            if c:
                return c
    except Exception:
        pass
    return None

def _offer_blob_near_price(price_el) -> str:
    """
    Build a tight text blob around ONE offer (price cell and its nearest row/cell),
    so we capture meal/cancel text without swallowing other offers.
    """
    try:
        return (price_el.evaluate(
            """(el) => {
              function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
              // 1) prefer the nearest table row (offer row)
              const tr = el.closest('tr');
              const chunks = [];
              if (tr){
                // include the "your choices" cell if present
                const choices = tr.querySelector('td.hprt-table-cell-conditions') || tr;
                chunks.push(clean(choices.innerText || choices.textContent || ''));
                // include occupancy cell and any max persons / only-for labels
                const occ = tr.querySelector('td.hprt-table-cell-occupancy') || tr;
                chunks.push(clean(occ.innerText || occ.textContent || ''));
                // include the price cell text
                chunks.push(clean(el.innerText || el.textContent || ''));
                // include any nearby "Sleeps" labels within the row
                chunks.push(clean(tr.innerText || tr.textContent || ''));
                return chunks.filter(Boolean).join(' ');
              }
              // 2) fallback: climb a few parents
              let node = el;
              for (let i=0;i<6 && node;i++){
                const t = clean(node.innerText || node.textContent || '');
                if (t) chunks.push(t);
                node = node.parentElement;
              }
              return chunks.filter(Boolean).join(' ');
            }"""
        ) or "").strip()
    except Exception:
        return ""

def _capacity_for_offer(adults: int, room_name_row, variant_row, offer_blob: str) -> Optional[int]:
    """
    Determine the offer capacity (exact) using the most reliable signals.
    """
    # Only-for is exact per-offer
    only_for = _extract_only_for_guest(offer_blob)
    if only_for is not None:
        return only_for

    # Occupancy icons in this offer row
    icons = _variant_row_occupancy_icons(variant_row)
    if icons:
        return icons

    # Sleeps (some locales)
    sleeps = _extract_sleeps_capacity(offer_blob)
    if sleeps:
        return sleeps

    # Max persons inside offer blob or header row
    mp = _extract_max_persons_from_text(offer_blob)
    if mp is not None:
        return mp

    if room_name_row is not None:
        try:
            hdr = (room_name_row.inner_text() or "")
        except Exception:
            hdr = ""
        mp2 = _extract_max_persons_from_text(hdr)
        if mp2 is not None:
            return mp2
        try:
            ic = _count_person_icons_in_node(room_name_row)
            if ic:
                return ic
        except Exception:
            pass

    return None

# ──────────────────────────────────────────────────────────────────────────────
# Classic table extraction (preferred)
# ──────────────────────────────────────────────────────────────────────────────
def _is_room_name_row(row) -> Optional[str]:
    try:
        name_el = (
            row.query_selector(".hprt-roomtype-icon-link")
            or row.query_selector(".hprt-roomtype-name")
            or row.query_selector("[data-testid='room-name']")
        )
        if not name_el:
            return None
        nm = (name_el.inner_text() or "").strip()
        if not nm:
            return None
        nm = re.sub(r"\s+", " ", nm)
        low = nm.lower()
        if any(x in low for x in ["review", "score", "rating"]):
            return None
        return nm
    except Exception:
        return None

def collect_room_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    """
    For a given adults=N, collect offers from the classic hprt table.

    IMPORTANT:
    We attach flags/capacity PER PRICE ELEMENT (offer), because a single room type
    can have many offers/variants.
    """
    results: List[Dict[str, Any]] = []

    table = None
    for sel in ROOM_TABLE_SELECTORS:
        try:
            table = page.query_selector(sel)
        except Exception:
            table = None
        if table:
            break
    if not table:
        return results

    try:
        trs = table.query_selector_all("tr")
    except Exception:
        trs = []

    current_room: Optional[str] = None
    current_room_row = None

    # Dedup within page
    seen = set()

    for tr in trs:
        try:
            nm = _is_room_name_row(tr)
            if nm:
                current_room = nm
                current_room_row = tr
                continue

            if not current_room or not current_room_row:
                continue

            # Find price elements in this offer row
            price_els = []
            for psel in PRICE_SELECTORS:
                try:
                    price_els.extend(tr.query_selector_all(psel))
                except Exception:
                    pass
            if not price_els:
                continue

            for pel in price_els:
                try:
                    ptxt = (pel.inner_text() or "").strip()
                except Exception:
                    ptxt = ""
                price = clean_price(ptxt)
                if price is None:
                    # fallback to offer blob (still currency anchored)
                    offer_blob = _offer_blob_near_price(pel)
                    price = clean_price(offer_blob)
                else:
                    offer_blob = _offer_blob_near_price(pel)

                if not offer_blob:
                    continue

                cap = _capacity_for_offer(adults, current_room_row, tr, offer_blob)
                if cap is None or cap != adults:
                    continue

                attrs = parse_rate_attributes(offer_blob)
                rate_key = build_rate_key(attrs)

                if MEAL_DEBUG and ("breakfast" in offer_blob.lower() or "dinner" in offer_blob.lower() or "half board" in offer_blob.lower()):
                    print("🧪 MEAL_BLOB:", offer_blob[:520])

                if OFFER_DEBUG:
                    print(f"🧩 OFFER adults={adults} cap={cap} price={price} room='{current_room[:40]}' hb={attrs.get('half_board')} din={attrs.get('dinner_included')} b={attrs.get('breakfast_included')} | blob={offer_blob[:180]}")

                if price is None:
                    continue

                key = (current_room.lower(), adults, float(price), rate_key)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "room": current_room,
                    "price": float(price),
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

# ──────────────────────────────────────────────────────────────────────────────
# Card layout fallback (when no table)
# ──────────────────────────────────────────────────────────────────────────────
def _card_candidates(page):
    for sel in [
        "[data-testid='property-card']",
        "[data-testid='property-card-container']",
        "[data-testid='availability-table']",
        "table tbody tr",
    ]:
        try:
            els = page.query_selector_all(sel)
            if els:
                return els
        except Exception:
            pass
    return []

def _extract_room_name_from_card(card) -> Optional[str]:
    for sel in ["[data-testid='room-name']", "h3", "h2", "[data-testid='title']"]:
        try:
            el = card.query_selector(sel)
            if el:
                t = (el.inner_text() or "").strip()
                if t and len(t) < 220:
                    return re.sub(r"\s+", " ", t)
        except Exception:
            continue
    try:
        lines = [x.strip() for x in (card.inner_text() or "").splitlines() if x.strip()]
        return lines[0][:200] if lines else None
    except Exception:
        return None

def _extract_price_from_card(card) -> Optional[float]:
    for psel in [
        "[data-testid='price-and-discounted-price']",
        "[data-testid='recommended-price']",
        "[data-testid='price-and-discounted-price--no-discount']",
        "[data-testid='price-and-discounted-price--pay-now']",
        ".bui-price-display__value",
    ]:
        try:
            for el in card.query_selector_all(psel):
                p = clean_price((el.inner_text() or ""))
                if p is not None:
                    return p
        except Exception:
            continue
    try:
        return clean_price(card.inner_text() or "")
    except Exception:
        return None

def collect_card_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cards = _card_candidates(page)
    for c in cards:
        try:
            txt = (c.inner_text() or "").strip()
        except Exception:
            txt = ""
        if not txt:
            continue

        room_name = _extract_room_name_from_card(c)
        if not room_name:
            continue

        # strict occupancy: exact capacity
        only_for = _extract_only_for_guest(txt)
        if only_for is not None and only_for != adults:
            continue

        cap = _extract_sleeps_capacity(txt) or _extract_max_persons_from_text(txt)
        if cap is None or cap != adults:
            continue

        price = _extract_price_from_card(c)
        if price is None:
            continue

        flags = parse_rate_attributes(txt)
        rate_key = build_rate_key(flags)

        out.append({
            "room": room_name,
            "price": float(price),
            "occupancy": adults,
            "breakfast_included": flags.get("breakfast_included"),
            "dinner_included": flags.get("dinner_included"),
            "half_board": flags.get("half_board"),
            "free_cancellation": flags.get("free_cancellation"),
            "nonrefundable": flags.get("nonrefundable"),
            "prepay_required": flags.get("prepay_required"),
            "rate_plan": flags.get("rate_plan"),
            "rate_key": rate_key,
        })
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Public scrape API (used by worker)
# ──────────────────────────────────────────────────────────────────────────────
def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    out: List[dict] = []
    booking_slug = _normalize_slug(slug)
    cc_eff = (cc or DEFAULT_CC).lower()

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
            """Object.defineProperty(navigator, 'language', {get: ()=>'en-GB'});
               Object.defineProperty(navigator, 'languages', {get: ()=>['en-GB','en']});"""
        )
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

            url = build_hotel_url(cc_eff, booking_slug, checkin, checkout, adults, lang="en-gb")
            print(f"🏨 {name} | {checkin}→{checkout} | adults={adults}\n   {url}")

            try:
                page.goto(url, timeout=PAGE_GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
            except TimeoutError:
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
                except Exception:
                    pass

                rows = collect_room_rows_for_adults(page, adults)
                if not rows:
                    rows = collect_card_rows_for_adults(page, adults)

                for r in rows:
                    out.append({
                        "hotel": name,
                        "slug": booking_slug.lower(),   # worker rewrites to "<slug>__<cc>"
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
                        "rate_key": r.get("rate_key"),
                    })

            except Exception as e:
                print("⚠️ Page error:", e)
                continue

        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Post-processing: keep min per (slug, checkin, room, occupancy, rate_key)
# ──────────────────────────────────────────────────────────────────────────────
def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    best: Dict[Tuple, dict] = {}
    for r in rows or []:
        slug_lower = (r.get("slug") or "").lower()
        rate_key = r.get("rate_key") or ""
        key = (slug_lower, r.get("checkin"), r.get("room"), int(r.get("occupancy") or 0), rate_key)
        try:
            price = float(r.get("price"))
        except Exception:
            continue
        if not slug_lower or not r.get("checkin") or not r.get("room") or not r.get("occupancy"):
            continue
        if key not in best or price < float(best[key]["price"]):
            best[key] = {**r, "slug": slug_lower, "price": price, "occupancy": int(r["occupancy"]), "rate_key": rate_key}
    return list(best.values())
