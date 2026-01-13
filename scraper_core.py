# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) — Variant-aware + strict occupancy
#
# Fixes in this version:
#  1) "Breakfast & dinner included" MUST set half_board=True even if breakfast_included is already True
#     (previous logic only set half_board when breakfast_included was None).
#  2) Strict occupancy for singles: support Booking's "Sleeps: 1 - 1 guests" text as a max-persons signal.
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

USER_AGENT = (
    os.getenv("USER_AGENT")
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/124.0.0.0 Safari/537.36"
)

# Debug helper: if set to "1", prints the offer blobs we parse flags from
DEBUG_MEAL_BLOBS = os.getenv("DEBUG_MEAL_BLOBS", "0") == "1"

# ──────────────────────────────────────────────────────────────────────────────
# Supabase headers
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
# Variant attribute parsing (EN-first, but tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def parse_rate_attributes(text: str) -> Dict[str, Optional[bool]]:
    """
    Best-effort variant parsing from a small text blob near the price/offer.

    IMPORTANT FIX:
      "Breakfast & dinner included" now ALWAYS sets half_board=True.
      Previously, half_board was only set if breakfast_included was None, which
      fails when we already set breakfast_included=True via "breakfast...included".
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

    # ── Meals ─────────────────────────────────────────────────────────────
    if re.search(r"breakfast.{0,30}included", t) or re.search(r"\bwith breakfast\b", t) or "includes breakfast" in t:
        flags["breakfast_included"] = True
    elif "breakfast" in t and re.search(
        r"not included|extra charge|for an extra fee|surcharge|per person|pp\b|optional|€|\beur\b|\bgood breakfast\b",
        t,
    ):
        # "Good breakfast € 18" = breakfast NOT included
        flags["breakfast_included"] = False

    dinner_kw = r"(dinner|evening meal|supper|abendessen|večerj|cena\b)"
    hb_kw = r"(half[-\s]?board|\bhb\b|halvpension|halbpension|polpenzion|polpansion)"

    has_hb = bool(re.search(hb_kw, t))
    has_ai = bool(re.search(r"\ball[-\s]?inclusive\b", t))
    has_dinner_included = bool(re.search(dinner_kw + r".{0,30}included", t)) or bool(re.search(r"includes.{0,30}" + dinner_kw, t))
    has_breakfast_and_dinner = bool(
        re.search(r"breakfast.{0,80}" + dinner_kw + r".{0,40}included", t)
        or re.search(dinner_kw + r".{0,80}breakfast.{0,40}included", t)
        or "breakfast & dinner included" in t
        or "breakfast and dinner included" in t
        or "breakfast & dinner" in t
        or "breakfast and dinner" in t
    )

    if has_hb:
        flags["half_board"] = True
        flags["dinner_included"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
    elif has_breakfast_and_dinner:
        # ✅ treat as half-board as well
        flags["half_board"] = True
        flags["dinner_included"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        # If breakfast was previously set False because of "Good breakfast € 18",
        # but we now see breakfast+dinner included, trust the "included" variant.
        if flags["breakfast_included"] is False and "breakfast" in t and "included" in t:
            flags["breakfast_included"] = True
    elif has_ai:
        flags["dinner_included"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
    elif has_dinner_included:
        flags["dinner_included"] = True

    # ── Cancellation ──────────────────────────────────────────────────────
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

    # ── Prepay ────────────────────────────────────────────────────────────
    if re.search(r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bpay now\b|\bcharged in advance\b|payment before arrival", t):
        flags["prepay_required"] = True
    if "no prepayment needed" in t or "no prepayment" in t or "pay at the property" in t:
        if flags["prepay_required"] is None:
            flags["prepay_required"] = False

    # ── Rate plan label ───────────────────────────────────────────────────
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
ROOM_TABLE_SELECTORS = ["#hprt-table", "table.hprt-table", "[data-testid='hprt-table']", "[data-testid='availability-table']"]

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
# Booking (some layouts): "Sleeps: 1 - 2 guests"
_SLEEPS_RANGE_RE = re.compile(r"sleeps:\s*(\d+)\s*-\s*(\d+)\s*guests?", re.IGNORECASE)
# Also seen: "Sleeps 2"
_SLEEPS_SINGLE_RE = re.compile(r"\bsleeps\s+(\d+)\b", re.IGNORECASE)

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

def _extract_sleeps_max(text: str) -> Optional[int]:
    if not text:
        return None
    m = _SLEEPS_RANGE_RE.search(text)
    if m:
        try:
            return int(m.group(2))
        except Exception:
            return None
    m2 = _SLEEPS_SINGLE_RE.search(text)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            return None
    return None

def _count_person_icons_in_node(node) -> Optional[int]:
    """
    Count visible person icons inside the node (best-effort).
    """
    try:
        return node.evaluate(
            """(el) => {
              const sels = [
                "i.bicon-occupancy", "i.bicon-person", "span.bicon-occupancy",
                "svg[aria-label*='person']", "svg[aria-label*='guest']",
                "[data-testid*='occupancy'] svg", "[data-testid*='occupancy'] i"
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

def parse_rate_attributes_from_row(row, room_name_row=None) -> Dict[str, Optional[bool]]:
    text_chunks: List[str] = []
    for sel in [
        "td.hprt-table-cell-conditions",
        "td:has([data-testid='cancellation-policy'])",
        "[data-testid='cancellation-policy']",
        "[data-testid='pricing-subtitle']",
        "[data-testid='meal-plan']",
        "[data-testid*='meal']",
    ]:
        try:
            el = row.query_selector(sel)
            if el:
                txt = (el.inner_text() or "").strip()
                if txt:
                    text_chunks.append(txt)
        except Exception:
            pass

    blob = " ".join(text_chunks).strip()

    try:
        vtxt = (row.inner_text() or "").strip()
    except Exception:
        vtxt = ""
    if vtxt:
        blob = (blob + " " + vtxt).strip() if blob else vtxt

    try:
        vtc = (row.evaluate("(el) => el.textContent") or "").strip()
    except Exception:
        vtc = ""
    if vtc and vtc != vtxt:
        blob = (blob + " " + vtc).strip() if blob else vtc

    if room_name_row is not None:
        try:
            htxt = (room_name_row.inner_text() or "").strip()
        except Exception:
            htxt = ""
        if htxt:
            blob = (blob + " " + htxt).strip() if blob else htxt

    if DEBUG_MEAL_BLOBS and blob:
        print("🧪 MEAL_BLOB:", blob[:900])

    return parse_rate_attributes(blob)

def _extract_variant_max_persons(room_name_row, variant_row_text: str) -> Optional[int]:
    """
    For strict occupancy:
      - Only for X guest => max = X for that offer
      - "Sleeps: A - B guests" => max = B
      - Prefer icon count on the room-name row (when present)
      - Else parse 'Max persons: X'
    """
    only_for = _extract_only_for_guest(variant_row_text or "")
    if only_for is not None:
        return only_for

    sleeps_max = _extract_sleeps_max(variant_row_text or "")
    if sleeps_max is not None:
        return sleeps_max

    icons = _count_person_icons_in_node(room_name_row)
    if icons:
        return icons

    try:
        txt = (room_name_row.inner_text() or "")
    except Exception:
        txt = ""
    mp = _extract_max_persons_from_text(txt)
    if mp is not None:
        return mp

    mp2 = _extract_max_persons_from_text(variant_row_text or "")
    if mp2 is not None:
        return mp2

    return None

def collect_room_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
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
    for tr in trs:
        try:
            nm = _is_room_name_row(tr)
            if nm:
                current_room = nm
                current_room_row = tr
                continue

            if not current_room or not current_room_row:
                continue

            attrs = parse_rate_attributes_from_row(tr, room_name_row=current_room_row)
            rate_key = build_rate_key(attrs)

            try:
                vtxt = (tr.inner_text() or "")
            except Exception:
                vtxt = ""

            max_p = _extract_variant_max_persons(current_room_row, vtxt)
            if max_p is None or max_p != adults:
                continue

            found_price = False
            for psel in PRICE_SELECTORS:
                for pel in tr.query_selector_all(psel):
                    try:
                        ptxt = (pel.inner_text() or "").strip()
                    except Exception:
                        ptxt = ""
                    price = clean_price(ptxt) or clean_price(vtxt)
                    if price is None:
                        continue

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
                    found_price = True

            if not found_price:
                price = clean_price(vtxt)
                if price is not None:
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

        # strict occupancy
        only_for = _extract_only_for_guest(txt)
        if only_for is not None and only_for != adults:
            continue

        # Use Sleeps and Max persons
        mp = _extract_max_persons_from_text(txt)
        if mp is None:
            mp = _extract_sleeps_max(txt)

        if mp is None or mp != adults:
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
