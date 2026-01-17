# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) — Variant-aware + strict occupancy
#
# Key behaviors:
#  - Scrape adults 1..4 by setting group_adults in URL (one pass per adults)
#  - STRICT occupancy:
#      * adults=N only if the OFFER itself has capacity == N (icons / sleeps / max persons / only-for)
#      * prevents “2 rooms for 4 people” combos being treated as a 4-person room
#      * prevents 2-person rooms being shown under 1-person (unless it is explicitly a 1-person offer)
#  - Variant aware:
#      * rate_key fingerprint for meal/refund/prepay flags
#  - Meal parsing:
#      * breakfast_included True when “included”
#      * breakfast_included False when “Good breakfast € 18” / fee/extra charge detected
#      * half_board True when “Breakfast & dinner included” / half-board tokens detected
#  - CRITICAL FIX (v5→v6):
#      * Meals are extracted PER PRICE element.
#      * Additionally, we “nearest-meal” search within the same row by bounding-box distance,
#        to catch cases where meal-plan text is not in the direct ancestor chain (common for adults=1/3).
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError

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

# Debug toggles (Railway env vars)
DEBUG_MEALS_PER_PRICE = os.getenv("DEBUG_MEALS_PER_PRICE", "0") == "1"   # prints per-price blob + flags (limited)
DEBUG_MEALS_MISSES    = os.getenv("DEBUG_MEALS_MISSES", "0") == "1"      # prints only when breakfast line exists but flags stay None
DEBUG_OFFER_CONTEXT   = os.getenv("DEBUG_OFFER_CONTEXT", "0") == "1"     # prints offer context for adults=1/3 when detected

# ──────────────────────────────────────────────────────────────────────────────
# Supabase headers (used by worker; keep here)
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
# Price parsing (currency anchored)
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
# Variant attribute parsing
# ──────────────────────────────────────────────────────────────────────────────
def parse_rate_attributes(text: str) -> Dict[str, Optional[bool]]:
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

    # Breakfast included signals
    if re.search(r"breakfast.{0,30}included", t) or re.search(r"\bwith breakfast\b", t) or "includes breakfast" in t:
        flags["breakfast_included"] = True

    # Breakfast fee / upsell signals (this should force False)
    # examples: "Good breakfast € 18", "Breakfast €18", "Breakfast for an extra fee"
    if "breakfast" in t and re.search(
        r"(€\s*\d+|\d+\s*€|eur\s*\d+|\bextra charge\b|\bextra fee\b|\bfor an extra fee\b|\boptional\b|\bsurcharge\b|\bper person\b|\bgood breakfast\b)",
        t,
    ):
        # If we did not explicitly see "...included", treat as NOT included
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = False

    # Half-board / dinner
    dinner_kw = r"(dinner|evening meal|supper|abendessen|večerj|vecerj|večerja|vecerja)"
    hb_kw = r"(half[-\s]?board|\bhb\b|halvpension|halbpension|polpenzion|polpansion)"
    has_hb = bool(re.search(hb_kw, t))
    has_breakfast_and_dinner = (
        "breakfast & dinner included" in t
        or "breakfast and dinner included" in t
        or bool(re.search(r"breakfast.{0,60}" + dinner_kw + r".{0,30}included", t))
        or bool(re.search(dinner_kw + r".{0,60}breakfast.{0,30}included", t))
    )
    has_dinner_included = bool(re.search(dinner_kw + r".{0,30}included", t)) or bool(re.search(r"includes.{0,30}" + dinner_kw, t))

    if has_hb or has_breakfast_and_dinner:
        flags["half_board"] = True
        flags["dinner_included"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
    elif has_dinner_included:
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
    if re.search(r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bpay now\b|\bcharged in advance\b|payment before arrival", t):
        flags["prepay_required"] = True
    if "no prepayment needed" in t or "no prepayment" in t or "pay at the property" in t:
        if flags["prepay_required"] is None:
            flags["prepay_required"] = False

    # Label
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

ROOM_NAME_SELECTORS = [
    "[data-testid='room-name']",
    ".hprt-roomtype-icon-link",
    ".hprt-roomtype-name",
    "h3",
    "h2",
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
# Strict capacity parsing
# ──────────────────────────────────────────────────────────────────────────────
_MAX_PERSONS_RE = re.compile(r"max persons:\s*(\d+)", re.IGNORECASE)
_ONLY_FOR_GUEST_RE = re.compile(r"only for\s+(\d+)\s+guest", re.IGNORECASE)
_SLEEPS_RE = re.compile(r"sleeps:\s*(\d+)\s*(?:-|to)?\s*(\d+)?\s*guests?", re.IGNORECASE)

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
    m = _SLEEPS_RE.search(text)
    if not m:
        return None
    a = m.group(1)
    b = m.group(2)
    try:
        lo = int(a) if a else None
        hi = int(b) if b else None
    except Exception:
        return None
    # strict: if range exists, use upper bound
    if hi is not None:
        return hi
    return lo

def _count_person_icons_in_node(node) -> Optional[int]:
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

def _closest_room_name(price_el) -> Optional[str]:
    try:
        return price_el.evaluate(
            """(el, sels) => {
                function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
                let node = el;
                for (let depth=0; depth<8 && node; depth++){
                    for (const sel of sels){
                        const cand = node.querySelector(sel);
                        if (cand){
                            const t = clean(cand.innerText || cand.textContent || '');
                            if (t && t.length < 220) return t;
                        }
                    }
                    node = node.parentElement;
                }
                const tr = el.closest('tr');
                if (tr){
                    for (const sel of sels){
                        const cand = tr.querySelector(sel);
                        if (cand){
                            const t = clean(cand.innerText || cand.textContent || '');
                            if (t && t.length < 220) return t;
                        }
                    }
                }
                return null;
            }""",
            ROOM_NAME_SELECTORS,
        )
    except Exception:
        return None

def _per_price_offer_blob(price_el) -> str:
    """
    Build the smallest reasonable text blob for THIS price.
    Steps:
      1) gather text along ancestor chain (limited depth)
      2) within the nearest <tr> (or nearest block), find the closest element that mentions meals
         using bounding-box distance, and append that text.
    This is the key fix for adults=1/3 where meal text is not in the ancestor chain.
    """
    try:
        blob = price_el.evaluate(
            """(el) => {
              function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
              let node = el;
              const chunks = [];
              for (let depth=0; depth<7 && node; depth++){
                const t = clean(node.innerText || node.textContent || '');
                if (t) chunks.push(t);
                node = node.parentElement;
              }
              return chunks.join(' ');
            }"""
        ) or ""
    except Exception:
        blob = ""

    # Append nearest meal-plan-ish text from within the same row
    try:
        extra = price_el.evaluate(
            """(el) => {
              function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
              function hasMeal(t){
                t = (t||'').toLowerCase();
                return t.includes('breakfast') || t.includes('half board') || t.includes('half-board')
                  || t.includes('dinner') || t.includes('breakfast & dinner') || t.includes('breakfast and dinner')
                  || t.includes('good breakfast');
              }
              const root = el.closest('tr') || el.closest("[data-testid='hprt-table']") || el.parentElement;
              if (!root) return '';
              const elRect = el.getBoundingClientRect();
              let best = null;
              let bestD = 1e18;
              const nodes = root.querySelectorAll("div,span,td,p,li");
              for (const n of nodes){
                const t = clean(n.innerText || n.textContent || '');
                if (!t) continue;
                if (!hasMeal(t)) continue;
                const r = n.getBoundingClientRect();
                const dx = (r.left + r.width/2) - (elRect.left + elRect.width/2);
                const dy = (r.top + r.height/2) - (elRect.top + elRect.height/2);
                const d = Math.abs(dy) * 2 + Math.abs(dx); // prioritize vertical closeness
                if (d < bestD){
                  bestD = d;
                  best = t;
                }
              }
              return best ? best : '';
            }"""
        ) or ""
    except Exception:
        extra = ""

    if extra and extra not in blob:
        blob = (blob + " " + extra).strip()

    return " ".join(blob.split())

def _offer_capacity_for_price(price_el, room_header_row, variant_row_text: str) -> Optional[int]:
    """
    Determine capacity for THIS price/offer (strict).
    Priority:
      - Only for X guest (in offer/variant text blob)
      - Sleeps: a-b guests (upper bound)
      - Icon count on room header row
      - Max persons
    """
    # first check local offer blob (because adults=1 offers have "Only for 1 guest" near the price)
    local = ""
    try:
        local = _per_price_offer_blob(price_el)
    except Exception:
        local = variant_row_text or ""

    only_for = _extract_only_for_guest(local)
    if only_for is not None:
        return only_for

    sleeps = _extract_sleeps_capacity(local)
    if sleeps is not None:
        return sleeps

    if room_header_row is not None:
        icons = _count_person_icons_in_node(room_header_row)
        if icons:
            return icons
        try:
            htxt = (room_header_row.inner_text() or "")
        except Exception:
            htxt = ""
        mp = _extract_max_persons_from_text(htxt)
        if mp is not None:
            return mp

    mp2 = _extract_max_persons_from_text(local) or _extract_max_persons_from_text(variant_row_text or "")
    return mp2

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

            try:
                vtxt = (tr.inner_text() or "")
            except Exception:
                vtxt = ""

            # Find all price elements in this variant row
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
                price = clean_price(ptxt) or clean_price(vtxt)
                if price is None:
                    continue

                # strict capacity for THIS price
                cap = _offer_capacity_for_price(pel, current_room_row, vtxt)
                if cap is None or cap != adults:
                    continue

                room_name = current_room

                # meal flags from per-price offer blob
                blob = _per_price_offer_blob(pel)
                flags = parse_rate_attributes(blob)
                rate_key = build_rate_key(flags)

                if DEBUG_MEALS_PER_PRICE and ("breakfast" in blob.lower() or "half" in blob.lower() or "dinner" in blob.lower()):
                    print(f"🧪 MEAL_PRICE(adults={adults}) room={room_name[:45]} price={price} | blob={blob[:240]}")
                    print(f"   -> flags: b={flags.get('breakfast_included')} hb={flags.get('half_board')} din={flags.get('dinner_included')}")

                results.append({
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
        except Exception:
            continue

    return results

# ──────────────────────────────────────────────────────────────────────────────
# Card fallback (rare now, but keep)
# ──────────────────────────────────────────────────────────────────────────────
def _card_candidates(page):
    for sel in [
        "[data-testid='availability-table']",
        "table tbody tr",
        "[data-testid='property-card']",
        "[data-testid='property-card-container']",
    ]:
        try:
            els = page.query_selector_all(sel)
            if els:
                return els
        except Exception:
            pass
    return []

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

        # strict capacity
        only_for = _extract_only_for_guest(txt) or _extract_only_for_guest((c.evaluate("(el)=>el.textContent") or "") if True else "")
        if only_for is not None and only_for != adults:
            continue

        cap = _extract_sleeps_capacity(txt) or _extract_max_persons_from_text(txt)
        if cap is None or cap != adults:
            continue

        # price
        price = None
        for psel in PRICE_SELECTORS:
            try:
                for el in c.query_selector_all(psel):
                    price = clean_price((el.inner_text() or ""))
                    if price is not None:
                        break
            except Exception:
                pass
            if price is not None:
                break
        if price is None:
            price = clean_price(txt)
        if price is None:
            continue

        # room name (best-effort)
        room_name = None
        for sel in ["[data-testid='room-name']", "h3", "h2", "[data-testid='title']"]:
            try:
                el = c.query_selector(sel)
                if el:
                    t = (el.inner_text() or "").strip()
                    if t and len(t) < 220:
                        room_name = re.sub(r"\s+", " ", t)
                        break
            except Exception:
                pass
        if not room_name:
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
# Public scrape API
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
