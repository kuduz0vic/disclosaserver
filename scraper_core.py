# scraper_core.py
# ------------------------------------------------------------
# Booking.com scraper core (sync Playwright)
#  - Table-based parsing (hprt table)
#  - Variant-aware (rate_key) : breakfast/dinner/half_board + cancel/refund/prepay
#  - Occupancy-aware (adults=1..4)
#  - Supabase helpers to fetch linked hotels WITHOUT relying on schema cache
# ------------------------------------------------------------

import os
import re
from functools import lru_cache
from typing import Optional, Dict, Any, List, Callable, Tuple
from urllib.parse import urlencode, urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# -----------------------------
# Env
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
)
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

DEFAULT_CC = (os.getenv("DEFAULT_CC", "si") or "si").lower()
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "0") == "1"
COMMON_CCS = (
    os.getenv("COMMON_CCS")
    or "si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr"
).split(",")

HEADLESS = os.getenv("HEADLESS", "1") == "1"
NO_SANDBOX = os.getenv("NO_SANDBOX", "1") == "1"

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "30000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "12000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "10"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# -----------------------------
# Supabase headers
# -----------------------------

def supabase_headers(*, json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if json_pref:
        h["Content-Type"] = "application/json"
        h["Accept"] = "application/json"
    if upsert:
        h["Prefer"] = "resolution=merge-duplicates"
    return h

# -----------------------------
# Slug helpers
# -----------------------------

def normalize_slug(v: str) -> str:
    if not v:
        return ""
    t = v.strip()
    if t.startswith("http"):
        try:
            u = urlparse(t)
            parts = [p for p in u.path.split("/") if p]
            if len(parts) >= 3 and parts[0].lower() == "hotel":
                slug = parts[2]
                slug = re.sub(r"\.[a-z]{2}(?:-[a-z]{2})?\.html?$", "", slug, flags=re.I)
                return slug.replace(" ", "").lower()
        except Exception:
            pass
    t = re.sub(r"\.[a-z]{2}(?:-[a-z]{2})?\.html?$", "", t, flags=re.I)
    return t.replace(" ", "").lower()


def to_scrape_slug(base_slug: str, cc: Optional[str]) -> str:
    b = normalize_slug(base_slug)
    c = (cc or "").strip().lower()
    return f"{b}__{c}" if c else b


def split_scrape_slug(scrape_slug: str) -> Tuple[str, Optional[str]]:
    s = (scrape_slug or "").strip().lower()
    if "__" in s:
        base, cc = s.split("__", 1)
        return base, (cc or None)
    return s, None

# -----------------------------
# RPC-based hotel discovery (avoids PostgREST relationship cache)
# -----------------------------

RPC_LINKS = os.getenv("RPC_USER_LINKS", "fn_user_linked_hotels")


def _rpc_user_links(user_id: str) -> List[dict]:
    url = f"{SUPABASE_URL}/rest/v1/rpc/{RPC_LINKS}"
    r = requests.post(url, json={"p_user_id": user_id}, headers=supabase_headers(), timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(r.text)
    return r.json() or []


def _fallback_user_links(user_id: str) -> List[dict]:
    # Fallback if RPC isn't installed. This relies on relationships, so it's best-effort.
    url = f"{SUPABASE_URL}/rest/v1/user_hotels"
    params = {
        "select": "hotel_id,link_type,hotels(name),hotel_profiles(booking_slug,booking_cc,updated_at)",
        "user_id": f"eq.{user_id}",
    }
    r = requests.get(url, params=params, headers=supabase_headers(json_pref=False), timeout=20)
    r.raise_for_status()
    rows = r.json() or []

    out: List[dict] = []
    for row in rows:
        name = ((row.get("hotels") or {}).get("name") or "").strip()
        prof = row.get("hotel_profiles")
        booking_slug, booking_cc, updated_at = None, None, None
        if isinstance(prof, list) and prof:
            prof = prof[0]
        if isinstance(prof, dict) and prof:
            booking_slug = prof.get("booking_slug")
            booking_cc = prof.get("booking_cc")
            updated_at = prof.get("updated_at")
        out.append({
            "hotel_id": row.get("hotel_id"),
            "link_type": row.get("link_type"),
            "name": name,
            "booking_slug": booking_slug,
            "booking_cc": (booking_cc or "").lower() or None,
            "profile_updated_at": updated_at,
        })
    return out


def get_user_links(user_id: str) -> List[dict]:
    try:
        return _rpc_user_links(user_id)
    except Exception:
        return _fallback_user_links(user_id)


def get_own_hotel(user_id: str) -> Optional[Dict[str, Any]]:
    links = get_user_links(user_id)
    if not links:
        return None
    for l in links:
        if (l.get("link_type") or "").strip() == "own":
            slug = normalize_slug(l.get("booking_slug") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {"hotel_id": l.get("hotel_id"), "name": l.get("name") or "My Hotel", "slug": slug, "cc": cc}
    return None


def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[Dict[str, Any]]:
    links = get_user_links(user_id)
    out: List[Dict[str, Any]] = []
    for l in links:
        if own_hotel_id and l.get("hotel_id") == own_hotel_id:
            continue
        if (l.get("link_type") or "").strip() != "competitor":
            continue
        slug = normalize_slug(l.get("booking_slug") or "")
        if not slug:
            continue
        cc = (l.get("booking_cc") or "").lower() or None
        out.append({"hotel_id": l.get("hotel_id"), "name": l.get("name") or "", "slug": slug, "cc": cc})
    return out

# -----------------------------
# Country resolver (optional)
# -----------------------------

@lru_cache(maxsize=512)
def resolve_cc_for_slug(slug: str, timeout: float = 4.0) -> Optional[str]:
    if not slug:
        return None
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-GB,en;q=0.9"}
    s = requests.Session()
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
        except Exception:
            continue
    return None

# -----------------------------
# Booking URL
# -----------------------------


def build_hotel_url(cc: Optional[str], slug: str, checkin: str, checkout: str, adults: int, lang: str = "en-gb") -> str:
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

# -----------------------------
# Price parsing (currency anchored)
# -----------------------------

_CURRENCY_RE = re.compile(r"(?:€|eur)\s*([0-9][0-9\s\.,]+)|([0-9][0-9\s\.,]+)\s*(?:€|eur)", re.I)


def _parse_number(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = raw.replace("\xa0", " ").strip()
    s = re.sub(r"[^0-9,\.]", "", s)
    if not s:
        return None

    if "," in s and "." in s:
        # decimal is whichever appears last
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts[-1]) in (1, 2):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if s.count(".") > 1:
            s = s.replace(".", "")

    try:
        val = float(s)
    except Exception:
        return None

    if val < 5 or val > 50000:
        return None
    return val


def clean_price(text: str) -> Optional[float]:
    if not text:
        return None
    txt = text.replace("\xa0", " ")
    matches = []
    for m in _CURRENCY_RE.finditer(txt):
        raw = m.group(1) or m.group(2)
        v = _parse_number(raw)
        if v is not None:
            matches.append(v)
    return min(matches) if matches else None

# -----------------------------
# Variant attribute parsing
# -----------------------------

_MEAL_B = re.compile(r"\bbreakfast\b", re.I)
_MEAL_B_INCLUDED = re.compile(r"breakfast.{0,20}included|with breakfast|includes breakfast", re.I)
_MEAL_D = re.compile(r"\bdinner\b|evening meal", re.I)
_MEAL_D_INCLUDED = re.compile(r"dinner.{0,20}included|includes dinner|breakfast.{0,40}dinner.{0,20}included", re.I)
_MEAL_HB = re.compile(r"\bhalf[-\s]?board\b|\bhb\b", re.I)

_NONREF = re.compile(r"non[-\s]?refundable|no refund|total cost to cancel|\bnrf\b", re.I)
_FREEC = re.compile(r"free cancellation|fully refundable|free to cancel", re.I)
_PREPAY = re.compile(r"prepay|prepaid|pay in advance|charged in advance|pay now", re.I)


def parse_rate_attributes_from_row(row) -> Dict[str, Optional[Any]]:
    flags: Dict[str, Optional[Any]] = {
        "breakfast_included": None,
        "dinner_included": None,
        "half_board": None,
        "free_cancellation": None,
        "nonrefundable": None,
        "prepay_required": None,
        "rate_plan": None,
    }

    chunks: List[str] = []

    # Try conditions cell first
    try:
        cond_el = row.query_selector("td.hprt-table-cell-conditions")
        if cond_el:
            t = (cond_el.inner_text() or "").strip()
            if t:
                chunks.append(t)
    except Exception:
        pass

    # Fallback to whole row text
    try:
        whole = (row.inner_text() or "").strip()
        if whole:
            chunks.append(whole)
    except Exception:
        pass

    blob = " ".join(chunks)
    t = " ".join(blob.split()).lower()

    # Meals
    if _MEAL_B_INCLUDED.search(t):
        flags["breakfast_included"] = True
    elif _MEAL_B.search(t) and re.search(r"not included|extra charge|for an extra fee|surcharge|per person|pp\b", t):
        flags["breakfast_included"] = False

    if _MEAL_HB.search(t):
        flags["half_board"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
        flags["dinner_included"] = True
    elif _MEAL_D_INCLUDED.search(t):
        flags["dinner_included"] = True

    # Cancellation
    nonref = bool(_NONREF.search(t))
    freec = bool(_FREEC.search(t)) and not nonref

    if nonref:
        flags["nonrefundable"] = True
    if freec:
        flags["free_cancellation"] = True
        if flags["nonrefundable"] is None:
            flags["nonrefundable"] = False

    # Prepay
    if _PREPAY.search(t):
        flags["prepay_required"] = True

    # rate_plan
    if flags["half_board"]:
        flags["rate_plan"] = "Half board"
    elif flags["dinner_included"]:
        flags["rate_plan"] = "Dinner"
    elif flags["breakfast_included"]:
        flags["rate_plan"] = "Breakfast"

    if flags["nonrefundable"]:
        flags["rate_plan"] = ("NRF " + (flags["rate_plan"] or "")).strip()
    elif flags["free_cancellation"]:
        flags["rate_plan"] = ("Free cancel " + (flags["rate_plan"] or "")).strip()

    return flags


def build_rate_key(flags: Dict[str, Any]) -> str:
    def tri(v, t):
        return f"{t}1" if v is True else (f"{t}0" if v is False else f"{t}?")

    return "|".join(
        [
            tri(flags.get("breakfast_included"), "b"),
            tri(flags.get("free_cancellation"), "rc"),
            tri(flags.get("nonrefundable"), "nrf"),
            tri(flags.get("prepay_required"), "pp"),
            tri(flags.get("dinner_included"), "din"),
            tri(flags.get("half_board"), "hb"),
        ]
    )

# -----------------------------
# Occupancy helpers
# -----------------------------

_MAX_PERSONS_RE = re.compile(r"max persons:\s*(\d+)", re.I)
_ONLY_FOR_RE = re.compile(r"only for\s+(\d+)\s+guest", re.I)


def _extract_occ_restrictions(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    m1 = _MAX_PERSONS_RE.search(text)
    m2 = _ONLY_FOR_RE.search(text)
    mx = int(m1.group(1)) if m1 else None
    only = int(m2.group(1)) if m2 else None
    return mx, only


def _room_name_from_row(row) -> Optional[str]:
    for sel in [
        ".hprt-roomtype-icon-link",
        ".hprt-roomtype-name",
        "[data-testid='room-name']",
    ]:
        try:
            el = row.query_selector(sel)
            if el:
                nm = (el.inner_text() or "").strip()
                nm = re.sub(r"\s+", " ", nm)
                if nm:
                    return nm
        except Exception:
            continue
    return None


ROOM_TABLE_SELECTORS = ["#hprt-table", "table.hprt-table", "[data-testid='hprt-table']"]
PRICE_SELECTORS = [
    "[data-testid='price-and-discounted-price']",
    ".bui-price-display__value",
    ".prco-valign-middle-helper",
    ".prco-inline-price",
]

EXPAND_SELECTORS = [
    "button:has-text('Show all')",
    "button:has-text('Show more')",
    "button:has-text('See all rooms')",
    "a:has-text('Show all')",
    "a:has-text('Show more')",
    "a:has-text('See all rooms')",
]


def aggressively_expand_and_scroll(page, should_cancel: Optional[Callable[[], bool]] = None):
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    # cookie banner
    try:
        if cancelled():
            return
        page.wait_for_selector("#onetrust-accept-btn-handler", timeout=2000)
        page.click("#onetrust-accept-btn-handler")
    except Exception:
        pass

    # click expand buttons
    for _ in range(3):
        if cancelled():
            return
        for sel in EXPAND_SELECTORS:
            try:
                page.locator(sel).first.click(timeout=350)
                page.wait_for_timeout(120)
            except Exception:
                pass

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
            break
        if h == last_h:
            break
        last_h = h


def _collect_variants_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for table_sel in ROOM_TABLE_SELECTORS:
        table = page.query_selector(table_sel)
        if not table:
            continue

        trs = table.query_selector_all("tr")
        current_room: Optional[str] = None
        current_room_text: str = ""

        for tr in trs:
            try:
                nm = _room_name_from_row(tr)
                if nm:
                    current_room = nm
                    try:
                        current_room_text = tr.inner_text() or ""
                    except Exception:
                        current_room_text = ""
                    continue

                if not current_room:
                    continue

                # Use variant row text to enforce occupancy restrictions when possible
                try:
                    row_text = tr.inner_text() or ""
                except Exception:
                    row_text = ""

                mx, only = _extract_occ_restrictions(row_text)
                if only is not None and only != adults:
                    continue
                if mx is not None and mx < adults:
                    continue

                # Parse attributes per variant row
                attrs = parse_rate_attributes_from_row(tr)
                rate_key = build_rate_key(attrs)

                # Find price elements within THIS variant row
                found_price = False
                for psel in PRICE_SELECTORS:
                    for el in tr.query_selector_all(psel):
                        try:
                            ptxt = el.inner_text() or ""
                        except Exception:
                            continue
                        price = clean_price(ptxt)
                        if price is None:
                            continue

                        results.append(
                            {
                                "room": current_room,
                                "occupancy": adults,
                                "price": price,
                                **attrs,
                                "rate_key": rate_key,
                            }
                        )
                        found_price = True

                if found_price:
                    continue

                # Fallback: price from row text (still currency anchored)
                price = clean_price(row_text)
                if price is None:
                    continue
                results.append(
                    {
                        "room": current_room,
                        "occupancy": adults,
                        "price": price,
                        **attrs,
                        "rate_key": rate_key,
                    }
                )
            except Exception:
                continue

        if results:
            break

    return results


def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    out: List[dict] = []

    base_slug = normalize_slug(slug)
    cc_eff = (cc or "").strip().lower() or None
    if not cc_eff and RESOLVE_CC_IF_MISSING:
        cc_eff = resolve_cc_for_slug(base_slug)
    cc_eff = cc_eff or DEFAULT_CC

    args = ["--no-sandbox"] if NO_SANDBOX else []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=args)
        context = browser.new_context(
            locale="en-GB",
            viewport={"width": 1400, "height": 900},
            user_agent=USER_AGENT,
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
        )
        context.add_init_script(
            """
            Object.defineProperty(navigator, 'language', {get: ()=>'en-GB'});
            Object.defineProperty(navigator, 'languages', {get: ()=>['en-GB','en']});
            """
        )

        # reduce bandwidth
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

            url = build_hotel_url(cc_eff, base_slug, checkin, checkout, adults, lang="en-gb")
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

                rows = _collect_variants_for_adults(page, adults)
                for r in rows:
                    out.append(
                        {
                            "hotel": name,
                            "slug": to_scrape_slug(base_slug, cc_eff),
                            "checkin": checkin,
                            "room": r.get("room"),
                            "occupancy": r.get("occupancy"),
                            "price": r.get("price"),
                            "breakfast_included": r.get("breakfast_included"),
                            "dinner_included": r.get("dinner_included"),
                            "half_board": r.get("half_board"),
                            "free_cancellation": r.get("free_cancellation"),
                            "nonrefundable": r.get("nonrefundable"),
                            "prepay_required": r.get("prepay_required"),
                            "rate_plan": r.get("rate_plan"),
                            "rate_key": r.get("rate_key") or "",
                        }
                    )

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


def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    best: Dict[tuple, dict] = {}
    for r in rows or []:
        slug_lower = (r.get("slug") or "").lower()
        rate_key = r.get("rate_key") or ""
        key = (slug_lower, r.get("checkin"), r.get("room"), int(r.get("occupancy") or 0), rate_key)
        try:
            price = float(r.get("price"))
        except Exception:
            continue
        if not slug_lower or not key[1] or not key[2] or not key[3]:
            continue
        if price < 5:
            continue
        if key not in best or price < float(best[key].get("price", 1e18)):
            best[key] = {**r, "slug": slug_lower, "occupancy": int(r.get("occupancy") or 0), "price": price, "rate_key": rate_key}
    return list(best.values())
