# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) — Variant-aware
#
# Goals:
# - Scrape 1..4 adults by setting group_adults in URL (one run per adults)
# - Extract *rate variants* per room (breakfast/dinner/half-board, refundable, prepay)
# - Produce stable `rate_key` so variants don't overwrite each other
# - Robust price parsing (currency-anchored; avoids random numbers like room size)
#
# Notes:
# - Booking has 2 common layouts:
#   (A) Classic availability table (#hprt-table / hprt-table)
#   (B) Card layout with multiple "rate plan" blocks inside room cards
# We support both by:
#   - Walking price elements and attaching context from nearest logical container
#   - Deriving room name from nearest room-name element above the price
# ──────────────────────────────────────────────────────────────────────────────

import os
import re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode, urlparse

import requests
from requests import HTTPError
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
# User hotel discovery (own + competitors)
#
# Data model recap (your project):
#   public.user_hotels(user_id, hotel_id, link_type)
#   public.hotels(id, name, url, ...)
#   public.hotel_profiles(hotel_id, booking_slug, booking_cc, updated_at, ...)
#
# We intentionally join through `hotels(hotel_profiles(...))` because PostgREST
# relationship discovery relies on FKs. In your schema you have hotels ↔ hotel_profiles
# (via hotel_id), and user_hotels ↔ hotels (via hotel_id). There is *not* necessarily
# a direct FK user_hotels ↔ hotel_profiles, so we avoid `user_hotels(hotel_profiles(...))`.

def _get_user_links(user_id: str) -> List[dict]:
    """Return user's linked hotels with resolved booking slug + cc.

    Output fields per row:
      hotel_id, link_type, name, url, booking_slug, booking_cc
    """
    url = f"{SUPABASE_URL}/rest/v1/user_hotels"
    params = {
        "select": "hotel_id,link_type,hotels(name,url,hotel_profiles(booking_slug,booking_cc,updated_at))",
        "user_id": f"eq.{user_id}",
    }
    # Sort so newest hotel_profiles wins when there are multiple versions
    # (PostgREST nests arrays; we handle both dict and list cases below)
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=20)
    r.raise_for_status()
    rows = r.json() or []

    out: List[dict] = []
    for row in rows:
        h = (row.get("hotels") or {})
        prof = h.get("hotel_profiles")

        # hotel_profiles can come back as an array (1-to-many) or object.
        bslug, bcc = "", ""
        if isinstance(prof, list) and prof:
            # prefer newest by updated_at if present
            def _ts(p):
                return (p.get("updated_at") or "")
            prof_sorted = sorted(prof, key=_ts, reverse=True)
            bslug = (prof_sorted[0].get("booking_slug") or "").strip()
            bcc = (prof_sorted[0].get("booking_cc") or "").strip().lower()
        elif isinstance(prof, dict) and prof:
            bslug = (prof.get("booking_slug") or "").strip()
            bcc = (prof.get("booking_cc") or "").strip().lower()

        out.append({
            "hotel_id": row.get("hotel_id"),
            "link_type": (row.get("link_type") or "").strip(),
            "name": (h.get("name") or "").strip(),
            "url": (h.get("url") or "").strip(),
            "booking_slug": bslug,
            "booking_cc": bcc or "",
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
                return {
                    "hotel_id": l.get("hotel_id"),
                    "name": l.get("name") or "My Hotel",
                    "slug": slug,
                    "cc": cc,
                }

    # fallback: first link
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {
        "hotel_id": first.get("hotel_id"),
        "name": first.get("name") or "My Hotel",
        "slug": slug,
        "cc": cc,
    }


def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l.get("hotel_id") == own_hotel_id:
            continue
        if l.get("link_type") != "competitor":
            continue
        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({
            "hotel_id": l.get("hotel_id"),
            "name": l.get("name") or "",
            "slug": slug,
            "cc": cc,
        })
    return out
#   public.hotels(id, name, url, ...)
#   public.hotel_profiles(hotel_id, booking_slug, booking_cc, updated_at, ...)
#
# Relationships in Supabase schema cache can be finicky if you don't have FKs.
# For the Python worker we avoid depending on implicit PostgREST relationships
# between user_hotels ↔ hotel_profiles.
#
# We instead:
#   1) Fetch user_hotels joined to hotels (this relationship usually exists).
#   2) Fetch hotel_profiles directly by hotel_id.
#   3) Merge in Python.
#
# This keeps the worker stable even when the dashboard/API routes hit
# "Could not find relationship between 'user_hotels' and 'hotel_profiles'".
# ──────────────────────────────────────────────────────────────────────────────

def _http_get(path: str, params: Dict[str, Any], timeout: float = 20.0) -> requests.Response:
    r = requests.get(
        f"{SUPABASE_URL}{path}",
        params=params,
        headers=supabase_headers(json_pref=False),
        timeout=timeout,
    )
    r.raise_for_status()
    return r


def _get_user_links(user_id: str) -> List[dict]:
    """Return flattened user hotel links with booking_slug + booking_cc."""
    if not user_id:
        return []

    # 1) user_hotels + hotels
    r = _http_get(
        "/rest/v1/user_hotels",
        {
            "select": "hotel_id,link_type,inserted_at,hotels(name,url)",
            "user_id": f"eq.{user_id}",
            "order": "inserted_at.asc",
        },
    )
    links = r.json() or []
    hotel_ids = [x.get("hotel_id") for x in links if x.get("hotel_id")]
    if not hotel_ids:
        return []

    # 2) hotel_profiles by hotel_id (prefer latest updated_at)
    # NOTE: your hotel_profiles does NOT have inserted_at; it has updated_at.
    # Also: PostgREST IN syntax: in.(id1,id2,...)
    in_list = ",".join(hotel_ids)
    pr = _http_get(
        "/rest/v1/hotel_profiles",
        {
            "select": "hotel_id,booking_slug,booking_cc,updated_at",
            "hotel_id": f"in.({in_list})",
            "order": "updated_at.desc.nullslast",
        },
    )
    prof_rows = pr.json() or []

    # Pick latest profile per hotel_id
    prof_by_hotel: Dict[str, Dict[str, Any]] = {}
    for p in prof_rows:
        hid = p.get("hotel_id")
        if hid and hid not in prof_by_hotel:
            prof_by_hotel[hid] = p

    out: List[dict] = []
    for row in links:
        hid = row.get("hotel_id")
        h = row.get("hotels") or {}
        prof = prof_by_hotel.get(hid) or {}
        out.append(
            {
                "hotel_id": hid,
                "link_type": (row.get("link_type") or "").strip(),
                "inserted_at": row.get("inserted_at"),
                "name": (h.get("name") or "").strip(),
                "url": (h.get("url") or "").strip(),
                "booking_slug": (prof.get("booking_slug") or "").strip(),
                "booking_cc": (prof.get("booking_cc") or "").strip().lower() or "",
            }
        )

    return out


def get_own_hotel(user_id: str) -> Optional[Dict[str, Any]]:
    links = _get_user_links(user_id)
    if not links:
        return None

    # Prefer explicit link_type='own'
    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {
                    "hotel_id": l.get("hotel_id"),
                    "name": l.get("name") or "My Hotel",
                    "slug": slug,
                    "cc": cc,
                }

    # Fallback to first link
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug:
        return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {
        "hotel_id": first.get("hotel_id"),
        "name": first.get("name") or "My Hotel",
        "slug": slug,
        "cc": cc,
    }


def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = _get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        hid = l.get("hotel_id")
        if own_hotel_id and hid == own_hotel_id:
            continue
        if (l.get("link_type") or "") != "competitor":
            continue

        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({"hotel_id": hid, "name": l.get("name") or "", "slug": slug, "cc": cc})

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

    # sanity
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
# Variant attribute parsing (English first; works surprisingly well cross-locale)
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

    # Meals
    if re.search(r"breakfast.{0,20}included", t) or re.search(r"\bwith breakfast\b", t):
        flags["breakfast_included"] = True
    elif re.search(r"\bbreakfast\b", t) and re.search(r"not included|extra charge|for an extra fee|surcharge|per person|pp\b", t):
        flags["breakfast_included"] = False

    half_board = bool(re.search(r"\bhalf[-\s]?board\b|\bhb\b", t))
    dinner = bool(re.search(r"dinner.{0,20}included|\bevening meal\b|buffet dinner", t))
    breakfast_and_dinner = bool(re.search(r"breakfast.{0,40}dinner.{0,20}included|dinner.{0,40}breakfast.{0,20}included", t))
    all_inclusive = bool(re.search(r"\ball[-\s]?inclusive\b", t))

    if half_board:
        flags["half_board"] = True
        # infer both meals for HB
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        flags["dinner_included"] = True
    elif breakfast_and_dinner or dinner or all_inclusive:
        flags["dinner_included"] = True
        if breakfast_and_dinner and flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
            flags["half_board"] = True

    # Cancellation
    nonref = bool(re.search(r"\bnon[-\s]?refundable\b|\bno refund\b|\btotal cost to cancel\b|\bnrf\b", t))
    free_canc = bool(re.search(r"\bfree cancellation\b|\bfully refundable\b|\bfree to cancel\b|\bcancel for free\b", t)) and not nonref

    if nonref:
        flags["nonrefundable"] = True
        flags["free_cancellation"] = False if flags["free_cancellation"] is None else flags["free_cancellation"]
    if free_canc:
        flags["free_cancellation"] = True
        flags["nonrefundable"] = False if flags["nonrefundable"] is None else flags["nonrefundable"]

    # Prepay
    if re.search(r"\bprepay\b|\bprepaid\b|\bpay in advance\b|\bpay now\b|\bcharged in advance\b|payment before arrival", t):
        flags["prepay_required"] = True

    # Rate plan label (for UI)
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
    # ternary fingerprint (unknowns don't collide)
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

# price selectors (we rely on elements that are very likely to actually be the price)
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

    # cookies
    for sel in ["#onetrust-accept-btn-handler", "button#onetrust-accept-btn-handler"]:
        try:
            if cancelled(): return
            page.locator(sel).first.click(timeout=2000)
        except Exception:
            pass

    # click expanders a few times
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

def _closest_room_name(page, price_el) -> Optional[str]:
    """
    Try to find a room name *near* the price element.
    Works for both table and card layouts by walking up a few parents
    and searching for room-name selectors within the ancestor.
    """
    try:
        # Playwright element_handle.evaluate to find closest ancestor container
        # and then query within it for known room name selectors.
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
                // fallback: look for closest table row room cell
                node = el.closest('tr');
                if (node){
                    for (const sel of sels){
                        const cand = node.querySelector(sel);
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

def _context_text_near_price(price_el) -> str:
    """
    Pull text from a small context window around the price element.
    Critical: do NOT use full page/card text (causes 1124-style errors).
    """
    try:
        return price_el.evaluate(
            """(el) => {
                function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
                let node = el;
                let chunks = [];
                for (let depth=0; depth<6 && node; depth++){
                    const t = clean(node.innerText || node.textContent || '');
                    if (t) chunks.push(t);
                    // stop early if we already captured policies keywords
                    const joined = chunks.join(' ').toLowerCase();
                    if (joined.includes('breakfast') || joined.includes('cancellation') || joined.includes('non-refundable') || joined.includes('half board') || joined.includes('dinner') || joined.includes('prepay') || joined.includes('pay at the property') || joined.includes('no prepayment')) {
                        break;
                    }
                    node = node.parentElement;
                }
                return chunks.join(' ');
            }"""
        ) or ""
    except Exception:
        return ""

# ──────────────────────────────────────────────────────────────────────────────
# Scrape one hotel/day (adults 1..4)
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

                # Wait for *any* room table container; if none, still try price elements
                try:
                    page.wait_for_selector(",".join(ROOM_TABLE_SELECTORS), timeout=WAIT_TABLE_TIMEOUT_MS)
                except Exception:
                    pass

                # Gather price elements in the DOM (scoped to containers if possible)
                price_els = []
                # prefer inside known containers
                for cont_sel in ROOM_TABLE_SELECTORS:
                    try:
                        cont = page.query_selector(cont_sel)
                        if not cont:
                            continue
                        for psel in PRICE_SELECTORS:
                            price_els.extend(cont.query_selector_all(psel))
                    except Exception:
                        continue
                # fallback global
                if not price_els:
                    for psel in PRICE_SELECTORS:
                        try:
                            price_els.extend(page.query_selector_all(psel))
                        except Exception:
                            pass

                # Build rows from each price element with local context
                seen = set()
                for pel in price_els:
                    if cancelled():
                        break

                    try:
                        ptxt = (pel.inner_text() or "").strip()
                    except Exception:
                        ptxt = ""
                    price = clean_price(ptxt)
                    if price is None:
                        # some layouts have price nested; try textContent anyway
                        try:
                            price = clean_price(pel.text_content() or "")
                        except Exception:
                            price = None
                    if price is None:
                        continue

                    room_name = _closest_room_name(page, pel) or ""
                    room_name = re.sub(r"\s+", " ", room_name).strip()
                    if not room_name or len(room_name) < 3:
                        continue

                    ctx_text = _context_text_near_price(pel)
                    flags = parse_rate_attributes(ctx_text)
                    rate_key = build_rate_key(flags)

                    key = (booking_slug.lower(), checkin, room_name, adults, rate_key, price)
                    # dedupe within page (same element appears twice sometimes)
                    if key in seen:
                        continue
                    seen.add(key)

                    out.append({
                        "hotel": name,
                        "slug": booking_slug.lower(),  # worker will rewrite to "<slug>__<cc>"
                        "checkin": checkin,
                        "room": room_name,
                        "occupancy": adults,
                        "price": float(price),
                        "breakfast_included": flags.get("breakfast_included"),
                        "dinner_included": flags.get("dinner_included"),
                        "half_board": flags.get("half_board"),
                        "free_cancellation": flags.get("free_cancellation"),
                        "nonrefundable": flags.get("nonrefundable"),
                        "prepay_required": flags.get("prepay_required"),
                        "rate_plan": flags.get("rate_plan"),
                        "rate_key": rate_key,
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

