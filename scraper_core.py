# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Shared scraper/core:
#  - English-only rendering (locale + Accept-Language)
#  - Occupancy-aware scrape (adults 1..4)
#  - Extract rate attributes from the "conditions" cell:
#       breakfast_included, dinner_included, half_board,
#       free_cancellation, nonrefundable, prepay_required,
#       rate_plan (simple text tag)
#  - Helpers for Supabase.
#  - Variant-aware via rate_key  # <<< ADDED >>>
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
from typing import Optional, Dict, Any, List, Callable
from urllib.parse import urlencode, urlparse

import requests
from requests import HTTPError
from playwright.sync_api import sync_playwright, TimeoutError
from functools import lru_cache

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
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "1") == "1"
COMMON_CCS = (
    os.getenv("COMMON_CCS")
    or "si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr"
).split(",")

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "20000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "8000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────────────────────────────────────────────────────────
# Supabase headers
# ──────────────────────────────────────────────────────────────────────────────
def supabase_headers(json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if json_pref:
        h["Content-Type"] = "application/json"
    if upsert:
        h["Prefer"] = "resolution=merge-duplicates,return=representation"
    return h

# ──────────────────────────────────────────────────────────────────────────────
# User hotel discovery (own + competitors)
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
                return re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", slug, flags=re.I)
        except Exception:
            pass
    t = re.sub(r"\.([a-z]{2}(?:-[a-z]{2})?)?\.html?$", "", t, flags=re.I)
    return t.replace(" ", "")

def _get_user_links(user_id: str) -> List[dict]:
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

def get_own_hotel(user_id: str):
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

# ──────────────────────────────────────────────────────────────────────────────
# Country resolver (when CC missing)
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
    "button:has-text('Show all')","button:has-text('Show more')",
    "button:has-text('See all rooms')","a:has-text('Show all')",
    "a:has-text('Show more')","a:has-text('See all rooms')",
]
ROOM_TABLE_SELECTORS = ["#hprt-table", "table.hprt-table", "[data-testid='hprt-table']"]
ROW_SELECTORS = ["tr.js-rt-block-row", "table.hprt-table tr", "[data-testid='room-row']"]
PRICE_SELECTORS = [
    ".prco-valign-middle-helper",
    ".bui-price-display__value",
    "[data-testid='price-and-discounted-price']",
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

def _parse_price_number(raw: str) -> Optional[float]:
    """Parse a number that may contain spaces, thousand separators, and comma decimals."""
    if not raw:
        return None
    s = raw.strip()
    s = re.sub(r"[^0-9,\.]", "", s)
    if not s:
        return None
    if "," in s and "." in s:
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
    """Extract a monetary price from text, avoiding unrelated numbers like '1 night'."""
    if not text:
        return None
    txt = text.replace("\xa0", " ").strip()

    patterns = [
        r"(?:€|eur)\s*([0-9][0-9\s\.,]+)",
        r"([0-9][0-9\s\.,]+)\s*(?:€|eur)",
    ]
    candidates: list[float] = []
    for pat in patterns:
        for mm in re.finditer(pat, txt, flags=re.I):
            val = _parse_price_number(mm.group(1))
            if val is not None:
                candidates.append(val)

    if not candidates:
        for mm in re.finditer(r"([0-9][0-9\s\.,]+)", txt):
            val = _parse_price_number(mm.group(1))
            if val is not None:
                candidates.append(val)

    if not candidates:
        return None
    price = max(candidates)
    if price < 5:
        return None
    return price
    txt = text.replace("\u00A0", " ").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", txt)
    return float(m.group(1)) if m else None

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
    if "triple" in n: return 3
    if "quadruple" in n or "family" in n: return 4
    if "single" in n: return 1
    return 2

def is_valid_row(name: str) -> bool:
    n = (name or "").lower()
    return not any(x in n for x in ["review", "score", "rating"])

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
        "sb_price_type": "total",
    }
    return f"{base}?{urlencode(qs)}"

# ──────────────────────────────────────────────────────────────────────────────
# Attribute parsing (mostly English)
#
# IMPORTANT:
# Booking's "room table" often contains multiple *rate variants* per room type
# (refundable vs non-refundable, breakfast vs half-board, etc.). In those cases,
# the flags live next to the *specific price* (or on the variant row), not
# necessarily on the room-type header row.
#
# To be accurate (and make your DayPopup filters work), we parse attributes
# **per variant row** and preserve variants via `rate_key`.
# ──────────────────────────────────────────────────────────────────────────────
def parse_rate_attributes_from_row(row) -> Dict[str, Optional[bool]]:
    """
    Look into the row (conditions + nearby policy text) and extract flags
    about meals, cancellation and prepayment.

    Returns a dict of booleans or None:
      - breakfast_included
      - dinner_included
      - half_board
      - free_cancellation
      - nonrefundable
      - prepay_required
      - rate_plan (short textual label)
    """
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

    # 1) Primary conditions cell (classic Booking layout)
    try:
        cond_el = row.query_selector(
            "td.hprt-table-cell-conditions, "
            "[data-testid='room-row'] td:has([data-testid*='policy'])"
        )
        if cond_el:
            txt = cond_el.inner_text() or ""
            if txt.strip():
                text_chunks.append(txt)
    except Exception:
        pass

    # 2) Additional likely sources (meal plan / price subtitle etc.)
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

    # 3) Fallback – if we still don't see anything useful, include the whole row.
    #    This makes the parser more robust for weird layouts.
    if not blob or not re.search(
        r"breakfast|dinner|supper|half[-\s]?board|all[-\s]?inclusive|"
        r"cancellation|refundable|prepay|pay in advance",
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

    # ── Meals ─────────────────────────────────────────────

    # Breakfast:
    # - "breakfast included", "with breakfast", "very good breakfast included"
    # - Any "breakfast ... included" within 20 chars
    # - Negative hints like "breakfast not included"
    if re.search(r"breakfast.{0,20}included", t):
        flags["breakfast_included"] = True
    elif re.search(r"\bwith breakfast\b", t):
        flags["breakfast_included"] = True
    elif re.search(r"\bbreakfast\b", t) and re.search(
        r"not included|extra charge|for an extra fee|surcharge|\b€\b|eur\b|per person|pp\b",
        t,
    ):
        # If breakfast is mentioned alongside a fee, treat it as NOT included.
        flags["breakfast_included"] = False

    # Dinner / half-board / all-inclusive:
    # "Breakfast & dinner included" or "breakfast and dinner included"
    breakfast_and_dinner = bool(
        re.search(r"breakfast.{0,40}dinner.{0,20}included", t)
        or re.search(r"dinner.{0,40}breakfast.{0,20}included", t)
    )

    # Explicit "dinner included", "buffet dinner included", etc.
    dinner_included = bool(
        re.search(r"dinner.{0,10}included", t)
        or re.search(r"evening meal.{0,10}included", t)
        or re.search(r"buffet dinner", t)
    )

    # "half board" / "HB" / "half-board"
    half_board = bool(
        re.search(r"\bhalf[-\s]?board\b", t)
        or re.search(r"\bhb\b", t)
        or re.search(r"\bhalf-board\b", t)
    )

    # All-inclusive is basically "has dinner" for our purposes
    all_inclusive = bool(
        re.search(r"\ball[-\s]?inclusive\b", t)
        or re.search(r"\bai board\b", t)
    )

    # Negatives (rare, but let's guard)
    no_dinner = bool(re.search(r"\bno dinner\b|\bwithout dinner\b", t))
    no_half_board = bool(
        re.search(r"\bno half[-\s]?board\b|\bwithout half board\b", t)
    )

    if (dinner_included or breakfast_and_dinner or all_inclusive or half_board) and not no_dinner:
        flags["dinner_included"] = True

    if half_board and not no_half_board:
        flags["half_board"] = True

        # In practice half-board means breakfast + dinner
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        if flags["dinner_included"] is None:
            flags["dinner_included"] = True

    # If we explicitly see "breakfast & dinner included" but haven't decided half_board yet,
    # treat that as half-board as well.
    if breakfast_and_dinner and flags["half_board"] is None:
        flags["half_board"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        if flags["dinner_included"] is None:
            flags["dinner_included"] = True

    # ── Cancellation ─────────────────────────────────────

    nonref = bool(
        re.search(r"\bnon[-\s]?refundable\b|\bnon refundable\b", t)
        or re.search(r"\bno refund\b|\bno refunds\b", t)
        or re.search(r"\btotal cost to cancel\b", t)
        or re.search(r"\bnonref\b", t)
    )

    free_canc = bool(
        re.search(
            r"\bfree cancellation\b|\bfully refundable\b|\bfree to cancel\b",
            t,
        )
        and not nonref
    )

    flags["nonrefundable"] = True if nonref else flags["nonrefundable"]
    flags["free_cancellation"] = True if free_canc else flags["free_cancellation"]

    # ── Prepayment ───────────────────────────────────────

    prepay = bool(
        re.search(
            r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bcharged in advance\b|"
            r"\bpayment before arrival\b|\bpay now\b",
            t,
        )
    )
    if prepay:
        flags["prepay_required"] = True

    # ── Rate Plan label (short tag used in UI) ───────────

    if flags["nonrefundable"]:
        if flags["breakfast_included"]:
            flags["rate_plan"] = "NRF w/ breakfast"
        elif flags["half_board"] or flags["dinner_included"]:
            flags["rate_plan"] = "NRF half-board"
        else:
            flags["rate_plan"] = "Non-refundable"
    elif flags["free_cancellation"]:
        if flags["breakfast_included"]:
            flags["rate_plan"] = "Free cancel + breakfast"
        elif flags["half_board"] or flags["dinner_included"]:
            flags["rate_plan"] = "Free cancel + half-board"
        else:
            flags["rate_plan"] = "Free cancellation"
    elif flags["half_board"] or flags["dinner_included"]:
        flags["rate_plan"] = "Half board"
    elif flags["breakfast_included"]:
        flags["rate_plan"] = "Breakfast"
    else:
        flags["rate_plan"] = None

    # 4) Final tighten: explicit cancellation snippet element (if present)
    try:
        canc_el = row.query_selector("[data-testid='cancellation-policy']")
        if canc_el:
            ctext = (canc_el.inner_text() or "").lower()
            if "free cancellation" in ctext or "fully refundable" in ctext:
                flags["free_cancellation"] = True
                if flags["nonrefundable"] is None:
                    flags["nonrefundable"] = False
            if "non-refundable" in ctext or "non refundable" in ctext:
                flags["nonrefundable"] = True
    except Exception:
        pass

    return flags

def build_rate_key(flags: dict) -> str:
    """
    Stable fingerprint for a rate variant (ternary state so unknowns don't collide).
    Order matters—keep it consistent with DB consumers.
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

def extract_price_with_context(price_el) -> (Optional[float], Optional[int]):
    price = None
    try:
        price = clean_price(price_el.inner_text())
    except Exception:
        return None, None
    if price is None:
        return None, None
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
        if not nm or not is_valid_row(nm):
            return None
        return re.sub(r"\s+", " ", nm)
    except Exception:
        return None


def collect_room_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    """Collect rate variants for a given adults count.

    Booking often structures the table like:
      - a "room name" row
      - followed by 1..N "variant" rows (different cancellation/meal/payment)
        where the room name is NOT repeated.

    Older logic assumed all flags lived on the room-name row, which caused
    dinner/non-refundable variants to be missed or overwritten.
    """
    results: List[Dict[str, Any]] = []

    for table_sel in ROOM_TABLE_SELECTORS:
        table = page.query_selector(table_sel)
        if not table:
            continue

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

                # Variant row: only meaningful if we have a current room name
                if not current_room:
                    continue

                # Parse flags *per variant row*
                attrs = parse_rate_attributes_from_row(tr)
                rate_key = build_rate_key(attrs)

                row_occ = row_level_occupancy_hint(tr) or current_room_occ_hint

                found_any_price = False
                # Find all price elements in this row (often one)
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

                # Fallback: if no price selectors matched in the variant row,
                # try first visible price text inside the row.
                if not found_any_price:
                    inferred = get_occupancy_from_name(current_room)
                    if inferred != adults:
                        continue
                    first_price = None
                    try:
                        txt = (tr.inner_text() or "")
                        first_price = clean_price(txt)
                    except Exception:
                        first_price = None
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

def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    out: List[dict] = []
    with sync_playwright() as p:
        extra_args = ["--no-sandbox"] if os.getenv("NO_SANDBOX") == "1" else []
        browser = p.chromium.launch(headless=True, args=extra_args)
        context = browser.new_context(
            locale="en-GB",
            viewport={"width": 1400, "height": 900},
            user_agent=USER_AGENT,
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
        )
        context.add_init_script("""Object.defineProperty(navigator, 'language', {get: ()=>'en-GB'}); Object.defineProperty(navigator, 'languages', {get: ()=>['en-GB','en']});""")

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

                rows = collect_room_rows_for_adults(page, adults)
                for r in rows:
                    out.append({
                        "hotel": name,
                        "slug": slug.lower(),
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
                        "rate_key": r.get("rate_key"),  # <<< ADDED >>>
                    })

            except Exception as e:
                print("⚠️ Page error:", e)
                continue

        browser.close()
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Post-processing: keep min per (slug, checkin, room, occupancy, rate_key)
# ──────────────────────────────────────────────────────────────────────────────
def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    # <<< CHANGED >>> include rate_key in the key so variants are preserved
    best: Dict[tuple, dict] = {}
    for r in rows:
        slug_lower = (r["slug"] or "").lower()
        rate_key = r.get("rate_key") or ""
        key = (slug_lower, r["checkin"], r["room"], int(r["occupancy"]), rate_key)
        price = r["price"]
        if price is None:
            continue
        if key not in best or price < best[key]["price"]:
            best[key] = {
                "hotel": r["hotel"],
                "slug": slug_lower,
                "checkin": r["checkin"],
                "room": r["room"],
                "occupancy": int(r["occupancy"]),
                "price": float(price),
                "breakfast_included": r.get("breakfast_included"),
                "dinner_included": r.get("dinner_included"),
                "half_board": r.get("half_board"),
                "free_cancellation": r.get("free_cancellation"),
                "nonrefundable": r.get("nonrefundable"),
                "prepay_required": r.get("prepay_required"),
                "rate_plan": r.get("rate_plan"),
                "rate_key": r.get("rate_key"),
            }
    return list(best.values())
