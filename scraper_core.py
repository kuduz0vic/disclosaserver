# scraper_core.py
# ------------------------------------------------------------------------------
# Core helpers + Booking scraper (English-only) with occupancy-aware passes.
# Extracts from the main #hprt-table and parses rate conditions (meal plan,
# refundability, payment mode) so UI filters are non-null.
#
# ENV (required):
#   SUPABASE_URL
#   SUPABASE_SERVICE_ROLE_KEY
#
# ENV (optional):
#   DEFAULT_CC=si
#   RESOLVE_CC_IF_MISSING=1
#   COMMON_CCS="si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr"
#   PAGE_GOTO_TIMEOUT_MS=20000
#   WAIT_TABLE_TIMEOUT_MS=9000
#   SCROLL_PASSES=12
#   NO_SANDBOX=1
# ------------------------------------------------------------------------------

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
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "9000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────────────────────────────────────────────────────────
# Supabase helpers
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

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[dict]:
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

# ──────────────────────────────────────────────────────────────────────────────
# Scraper (English-only) from #hprt-table with conditions parsing
# ──────────────────────────────────────────────────────────────────────────────

HPRT_TABLE_SEL = "#hprt-table"
ROW_SEL = f"{HPRT_TABLE_SEL} tr"

NAME_SELS = [
    ".hprt-roomtype-icon-link",
    ".hprt-roomtype-name",
    "[data-testid='room-name']",
]
PRICE_SELS = [
    "[data-testid='price-and-discounted-price']",
    ".bui-price-display__value",
    ".prco-valign-middle-helper",
    ".prco-inline-price",
]
COND_CELL_SELS = [
    ".hprt-table-cell-conditions",
    ".hprt-table-cell.hprt-table-cell-conditions",
]
CANCELLATION_BLOCK_SEL = "[data-testid='cancellation-policy']"
PREPAYMENT_BLOCK_SEL   = "[data-testid='prepayment-policy']"
VALUE_ADDS_BLOCK_SEL   = "[data-testid='RoomTableValueAdds-wrapper']"

MEAL_KEYWORDS = {
    "FB": ["full board"],
    "HB": [
        "half board",
        "breakfast & dinner included",
        "breakfast and dinner included",
        "dinner included",
    ],
    "BB": [
        "breakfast included",
        "with breakfast",
    ],
    "RO_NEG": [
        "room only",
        "no breakfast",
    ],
}

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
        "lang": lang,  # FORCE ENGLISH
        "sb_price_type": "total",
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

    for _ in range(2):
        if cancelled(): return
        for sel in ["button:has-text('Show all')","button:has-text('See all rooms')","a:has-text('Show all')","a:has-text('See all rooms')"]:
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
        for sel in ["button:has-text('Show all')","a:has-text('Show all')"]:
            try:
                page.locator(sel).first.click(timeout=300)
            except Exception:
                pass
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

def _text(el) -> str:
    try:
        return (el.inner_text() or "").strip()
    except Exception:
        return ""

def _find_first_text(row, selectors: List[str]) -> str:
    for sel in selectors:
        el = row.query_selector(sel)
        if el:
            t = _text(el)
            if t:
                return t
    return ""

def _detect_meal_from_text(t: str) -> Optional[str]:
    tl = t.lower()
    for code, keys in MEAL_KEYWORDS.items():
        if code == "RO_NEG":  # handled later
            continue
        if any(k in tl for k in keys):
            return code
    if any(k in tl for k in MEAL_KEYWORDS["RO_NEG"]):
        return "RO"
    return None

def _detect_refundable_from_text(t: str) -> Optional[bool]:
    tl = t.lower()
    if "free cancellation" in tl:
        return True
    if "non-refundable" in tl or "total cost to cancel" in tl:
        return False
    return None

def _detect_pay_mode_from_text(t: str) -> Optional[str]:
    tl = t.lower()
    if "no prepayment needed" in tl or "pay at the property" in tl:
        return "PAY_AT_PROPERTY"
    if "prepayment" in tl or "deposit required" in tl:
        return "PREPAY"
    return None

def _short_title_from_text(t: str) -> Optional[str]:
    pats = [
        r"(Breakfast\s*&\s*dinner included)",
        r"(Breakfast and dinner included)",
        r"(Breakfast included)",
        r"(Free cancellation)",
        r"(Non[- ]?refundable)",
        r"(No prepayment needed)",
        r"(Pay at the property)",
        r"(Half board)",
        r"(Full board)",
    ]
    s = " ".join(t.split())
    for p in pats:
        m = re.search(p, s, flags=re.I)
        if m:
            return m.group(1)
    return None

def _coerce_price_from_any(el) -> Optional[float]:
    t = _text(el)
    if not t:
        return None
    t = t.replace("\u00A0", " ").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    return float(m.group(1)) if m else None

def _row_is_room_content(row) -> bool:
    if row.query_selector(", ".join(PRICE_SELS)):
        return True
    if row.query_selector(", ".join(COND_CELL_SELS)):
        return True
    name = _find_first_text(row, NAME_SELS)
    return bool(name)

def _occupancy_hint_from_row(row) -> Optional[int]:
    try:
        txt = (row.inner_text() or "").lower()
    except Exception:
        txt = ""
    for pat in [r"\bfor\s+(\d+)\s+adults?\b", r"\b(\d+)\s+adults?\b", r"\bsleeps\s+(\d+)\b", r"\b(\d+)\s+guests?\b"]:
        m = re.search(pat, txt)
        if m:
            try:
                n = int(m.group(1))
                if 1 <= n <= 8:
                    return n
            except Exception:
                pass
    return None

def _parse_conditions_blob(row) -> dict:
    parts: List[str] = []
    for sel in COND_CELL_SELS:
        cell = row.query_selector(sel)
        if cell:
            parts.append(_text(cell))
            for sub in (VALUE_ADDS_BLOCK_SEL, CANCELLATION_BLOCK_SEL, PREPAYMENT_BLOCK_SEL):
                subel = cell.query_selector(sub)
                if subel:
                    parts.append(_text(subel))
    blob = "\n".join([p for p in parts if p]).strip()
    if not blob:
        return {"rate_meal": None, "is_refundable": None, "pay_mode": None, "rate_title": None, "rate_notes": None}
    meal = _detect_meal_from_text(blob)
    refundable = _detect_refundable_from_text(blob)
    pay_mode = _detect_pay_mode_from_text(blob)
    title = _short_title_from_text(blob)
    return {
        "rate_meal": meal,
        "is_refundable": refundable,
        "pay_mode": pay_mode,
        "rate_title": title,
        "rate_notes": blob[:1000],  # keep a short excerpt for debugging/search
    }

def collect_room_rows_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    """
    Parse the main #hprt-table rows. Emit one row per distinct price cell found.
    Returns [{room, price, occupancy, rate_meal, is_refundable, pay_mode, rate_title, rate_notes}]
    """
    out: List[dict] = []
    table = page.query_selector(HPRT_TABLE_SEL)
    if not table:
        return out

    rows = page.query_selector_all(ROW_SEL)
    rows = [r for r in rows if _row_is_room_content(r)]
    if not rows:
        return out

    last_cond = {"rate_meal": None, "is_refundable": None, "pay_mode": None, "rate_title": None, "rate_notes": None}
    last_room: Optional[str] = None

    for r in rows:
        try:
            cond = _parse_conditions_blob(r)
            if any(v for v in cond.values() if v not in (None, "")):
                last_cond = cond

            room_name = _find_first_text(r, NAME_SELS) or last_room
            if room_name:
                last_room = room_name

            price_els = []
            for psel in PRICE_SELS:
                price_els.extend(r.query_selector_all(psel))
            if not price_els:
                continue

            occ = _occupancy_hint_from_row(r) or adults

            seen_prices: set[float] = set()
            for pel in price_els:
                price = _coerce_price_from_any(pel)
                if price is None or price in seen_prices:
                    continue
                seen_prices.add(price)

                out.append({
                    "room": (room_name or "Room").strip(),
                    "price": float(price),
                    "occupancy": int(occ),
                    "rate_meal": last_cond.get("rate_meal"),
                    "is_refundable": last_cond.get("is_refundable"),
                    "pay_mode": last_cond.get("pay_mode"),
                    "rate_title": last_cond.get("rate_title"),
                    "rate_notes": last_cond.get("rate_notes"),
                    "rate_source": "hprt-table",
                })
        except Exception:
            continue

    return out

def scrape_hotel_for_dates(
    name: str, slug: str, cc: Optional[str], checkin: str, checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    """
    For adults=1..4:
      - Load hotel with lang=en-gb, checkin/checkout & group_adults
      - Expand & scroll
      - Parse #hprt-table and collect prices + conditions
    """
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    out: List[dict] = []
    with sync_playwright() as p:
        extra_args = ["--no-sandbox"] if os.getenv("NO_SANDBOX") == "1" else []
        browser = p.chromium.launch(headless=True, args=extra_args)
        context = browser.new_context(
            locale="en-GB",  # FORCE ENGLISH
            viewport={"width": 1400, "height": 900},
            user_agent=USER_AGENT,
        )
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        page = context.new_page()

        for adults in (1, 2, 3, 4):
            if cancelled():
                break
            url = build_hotel_url(cc, slug, checkin, checkout, adults, lang="en-gb")
            print(f"🏨 {name} | {checkin}→{checkout} | adults={adults}\n   {url}")
            try:
                page.goto(url, timeout=PAGE_GOTO_TIMEOUT_MS)
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
                    page.wait_for_selector(HPRT_TABLE_SEL, timeout=WAIT_TABLE_TIMEOUT_MS)
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
                        "rate_meal": r.get("rate_meal"),
                        "is_refundable": r.get("is_refundable"),
                        "pay_mode": r.get("pay_mode"),
                        "rate_title": r.get("rate_title"),
                        "rate_notes": r.get("rate_notes"),
                        "rate_source": r.get("rate_source"),
                    })

            except Exception as e:
                print("⚠️ Page error:", e)
                continue

        browser.close()
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Post-processing
# ──────────────────────────────────────────────────────────────────────────────
def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    """
    Keep the minimum price per (slug, checkin, room, occupancy).
    Preserves extra fields (rate_*) from the cheapest option seen.
    """
    best: Dict[tuple, dict] = {}
    for r in rows:
        slug_lower = (r["slug"] or "").lower()
        key = (slug_lower, r["checkin"], r["room"], int(r["occupancy"]))
        price = r["price"]
        if price is None:
            continue
        if key not in best or price < best[key]["price"]:
            best[key] = dict(r)
            best[key]["slug"] = slug_lower
            best[key]["price"] = float(price)
            best[key]["occupancy"] = int(r["occupancy"])
    return list(best.values())
