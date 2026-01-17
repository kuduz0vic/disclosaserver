# scraper_core.py
# ──────────────────────────────────────────────────────────────────────────────
# Booking.com scraper core (sync Playwright) — Variant-aware + strict occupancy
#
# v8 fixes:
# 1) Capacity detection is PER-OFFER (variant row first), not inherited from header row.
#    This fixes cases like "Triposteljna ..." where Booking shows both 3-guest and 1-guest
#    pricing under the same room header; previously we used header icons and misfiled 1-guest
#    prices under 3-guest.
# 2) "Only for X guest" detection expanded to multiple languages and the "Sleeps: A - B guests"
#    pattern used in Booking's room picker.
# 3) Optional debug logging:
#      DEBUG_MEAL_BLOB=1   -> prints the offer blob (same as before)
#      DEBUG_CAPACITY=1    -> prints capacity decisions for offers (limited)
#
# Requirements:
# - lang=en-gb in URL, but Booking may still render local strings. We include a few.
# - STRICT occupancy rule:
#     keep an offer only if detected capacity == adults (exact match)
#     (prevents 2-room combos being treated as 4-person rooms)
# - For adults=1, we *still* require capacity == 1 (so doubles won't show under "1-person")
#   (this matches your desired demo semantics: 1-person means a real single offer)
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

DEBUG_MEAL_BLOB = os.getenv("DEBUG_MEAL_BLOB", "0") == "1"
DEBUG_CAPACITY = os.getenv("DEBUG_CAPACITY", "0") == "1"
DEBUG_CAPACITY_LIMIT = int(os.getenv("DEBUG_CAPACITY_LIMIT", "30"))

# Debug classification for breakfast/half-board per offer.
# NOTE: This only prints if you enable it.
DEBUG_BREAKFAST = os.getenv("DEBUG_BREAKFAST", "0") == "1"
DEBUG_BREAKFAST_LIMIT = int(os.getenv("DEBUG_BREAKFAST_LIMIT", "40"))
_DEBUG_BREAKFAST_COUNT = 0
DEBUG_BREAKFAST_LIMIT = int(os.getenv("DEBUG_BREAKFAST_LIMIT", "40"))
DEBUG_BREAKFAST_SLUG = (os.getenv("DEBUG_BREAKFAST_SLUG", "") or "").strip().lower()

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
    """Best-effort variant parsing from text near an offer.

    We intentionally keep this tolerant:
    - Booking is often forced to English via `lang=en-gb`, but some strings leak local language.
    - For filtering we mainly care about:
        breakfast_included (True/False/None)
        dinner_included   (True/False/None)
        half_board        (True/False/None)   <-- treat 'Breakfast & dinner included' as half-board
        free_cancellation, nonrefundable, prepay_required
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
    # IMPORTANT:
    # Booking can render multiple offers (breakfast optional + breakfast included + half-board)
    # under the same room. If our "offer blob" accidentally captures text from a sibling offer,
    # we can see BOTH "breakfast included" and "Good breakfast € 18". In that scenario the
    # fee line is the most *offer-specific* signal.

    hb_kw = r"(half[-\s]?board|\bhb\b|halvpension|halbpension|polpenzion|polpansion)"
    dinner_kw = r"(dinner|evening meal|supper|abendessen|vecerj|večerj|večerja)"

    has_hb = bool(re.search(hb_kw, t))
    has_breakfast_and_dinner = bool(
        re.search(r"breakfast.{0,80}" + dinner_kw + r".{0,40}included", t)
        or re.search(dinner_kw + r".{0,80}breakfast.{0,40}included", t)
        or "breakfast & dinner included" in t
        or "breakfast and dinner included" in t
        or "breakfast & dinner" in t
        or "breakfast and dinner" in t
    )
    has_dinner_included = bool(re.search(dinner_kw + r".{0,40}included", t)) or bool(re.search(r"includes.{0,40}" + dinner_kw, t))
    has_ai = bool(re.search(r"\ball[-\s]?inclusive\b", t))

    # Breakfast signals
    has_breakfast_included = bool(
        re.search(r"\bbreakfast\b.{0,40}\bincluded\b", t)
        or re.search(r"\bwith breakfast\b", t)
        or "includes breakfast" in t
    )
    # Fee / optional signals (very common: 'Good breakfast € 18' or 'Good breakfast 18 €').
    # We keep this intentionally broad: if the blob shows a breakfast price, it's NOT included.
    has_breakfast_fee = bool(
        re.search(r"\bgood breakfast\b\s*(?:€|eur)\s*[0-9]", t)
        or re.search(r"\bgood breakfast\b.{0,12}[0-9]{1,4}\s*(?:€|eur)\b", t)
        or re.search(r"\bbreakfast\b\s*(?:€|eur)\s*[0-9]", t)
        or re.search(r"\bbreakfast\b.{0,12}[0-9]{1,4}\s*(?:€|eur)\b", t)
        # Weak signal: currency may be rendered separately; treat a visible number near breakfast as a fee.
        or re.search(r"\bgood breakfast\b.{0,12}[0-9]{1,4}\b", t)
        or re.search(r"\bbreakfast\b.{0,12}[0-9]{1,4}\b", t)
        or re.search(r"\bbreakfast\b.{0,30}(?:extra charge|for an extra fee|surcharge|optional|not included)", t)
    )

    # Half-board implies both meals included (treat 'Breakfast & dinner included' as HB)
    if has_hb or has_breakfast_and_dinner:
        flags["half_board"] = True
        flags["dinner_included"] = True
        flags["breakfast_included"] = True
    elif has_ai:
        flags["dinner_included"] = True
        if flags["breakfast_included"] is None:
            flags["breakfast_included"] = True
    else:
        # Not half-board: classify breakfast.
        # IMPORTANT: Booking sometimes renders multiple offer blocks close together.
        # If our blob accidentally contains both "breakfast included" (from another offer)
        # and "Good breakfast € 18" (a paid add-on), we must NOT default to included.
        # The paid-add-on signal is more specific for the current offer.
        if has_breakfast_fee and not has_breakfast_and_dinner:
            flags["breakfast_included"] = False
        elif has_breakfast_included:
            flags["breakfast_included"] = True

    if DEBUG_BREAKFAST and ("breakfast" in t or "hb" in t or "half board" in t or "dinner" in t):
        # Keep logs compact. This helps diagnose cases where breakfast is being over-attributed.
        snippet = t[:220].replace("\n", " ")
        print(
            "🧪 BF_CLASSIFY:",
            {
                "b_inc": flags.get("breakfast_included"),
                "hb": flags.get("half_board"),
                "din": flags.get("dinner_included"),
                "b_included_sig": has_breakfast_included,
                "b_fee_sig": has_breakfast_fee,
                "b_and_d": has_breakfast_and_dinner,
                "snippet": snippet,
            },
        )

        # Dinner only (rare) if explicitly stated
        if has_dinner_included:
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
    if "no prepayment" in t or "pay at the property" in t:
        if flags["prepay_required"] is None:
            flags["prepay_required"] = False

    # ── Rate plan label (UI helper) ───────────────────────────────────────
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

    # Optional debug: show breakfast/half-board classification and key triggers.
    # This helps diagnose cases where the offer blob misses the "Good breakfast € X" line.
    global _DEBUG_BREAKFAST_COUNT
    if DEBUG_BREAKFAST and _DEBUG_BREAKFAST_COUNT < DEBUG_BREAKFAST_LIMIT and ("breakfast" in t or "dinner" in t or "half board" in t):
        _DEBUG_BREAKFAST_COUNT += 1
        try:
            print(
                "🧪 BFDBG:",
                {
                    "b_included": flags.get("breakfast_included"),
                    "din": flags.get("dinner_included"),
                    "hb": flags.get("half_board"),
                    "fee": has_breakfast_fee,
                    "b_inc_tok": has_breakfast_included,
                    "b_din_tok": has_breakfast_and_dinner,
                },
            )
            print("🧪 BFDBG_BLOB:", t[:260])
        except Exception:
            pass

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
# Strict occupancy helpers (per-offer)
# ──────────────────────────────────────────────────────────────────────────────
_MAX_PERSONS_RE = re.compile(r"max persons:\s*(\d+)", re.IGNORECASE)
# "Only for X guest" (EN) + common variants (SL/DE/IT/FR) + tolerant endings
_ONLY_FOR_RE = re.compile(
    r"(?:only\s+for|samo\s+za|nur\s+f(?:u|ü)r|solo\s+per|seulement\s+pour)\s+(\d+)\s*(?:guest|guests|gosta|gostov|osebo|osebe|person|personen|persona|personnes)?",
    re.IGNORECASE,
)

# Backwards-compat alias (older versions referenced _ONLY_FOR_GUEST_RE)
_ONLY_FOR_GUEST_RE = _ONLY_FOR_RE
# "Sleeps: 1 - 2 guests" / "Sleeps 1 - 2"
_SLEEPS_RE = re.compile(r"\bsleeps\s*:?\s*(\d+)\s*(?:[-–—]|to)\s*(\d+)\s*(?:guests?|people|persons?)\b|\bsleeps\s*:?\s*(\d+)\s*(?:guests?|people|persons?)\b", re.IGNORECASE)
# Also catches "Sleeps 1 - 2 guests" without colon
_SLEEPS_RE2 = re.compile(r"\bspi\s*:?\s*(\d+)\s*(?:[-–—]|do)\s*(\d+)\s*(?:gost|oseb)\b|\bspi\s*:?\s*(\d+)\s*(?:gost|oseb)\b", re.IGNORECASE)

def _extract_max_persons_from_text(text: str) -> Optional[int]:
    """Return the **maximum** 'Max persons: X' found in the text.

    Booking tables sometimes include multiple 'Max persons:' snippets in the same
    row because of rowspans / duplicated hidden blocks. Taking the first match
    can incorrectly downgrade capacity (e.g. picking 1 when the offer is for 2).
    """
    if not text:
        return None
    nums = []
    for m in _MAX_PERSONS_RE.finditer(text):
        try:
            nums.append(int(m.group(1)))
        except Exception:
            pass
    if not nums:
        return None
    return max(nums)

def _extract_only_for_guest(text: str) -> Optional[int]:
    """Return the 'Only for X guest' number if present (prefer max if multiple)."""
    if not text:
        return None
    nums = []
    for m in _ONLY_FOR_GUEST_RE.finditer(text):
        try:
            nums.append(int(m.group(1)))
        except Exception:
            pass
    if not nums:
        return None
    return max(nums)


def _extract_sleeps_capacity(text: str) -> Optional[int]:
    """Extract the implied capacity from Booking's "Sleeps" line.

    We treat range forms ("Sleeps: 1 - 2 guests") as capacity=2 (upper bound),
    because a 1..2 room is still a *double* and should NOT be counted as a true
    single room when adults=1 unless Booking explicitly says "Only for 1 guest".
    """
    if not text:
        return None
    t = " ".join(text.split())

    m = _SLEEPS_RE.search(t)
    if m:
        try:
            a = int(m.group(1))
            b = int(m.group(2)) if m.group(2) else None
            return b if b is not None else a
        except Exception:
            pass

    m2 = _SLEEPS_RE2.search(t)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            pass
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
                "[data-testid*='occupancy'] svg", "[data-testid*='occupancy'] i",
                // Booking sometimes uses role/img
                "span[role='img'][aria-label*='person']",
                "span[role='img'][aria-label*='guest']"
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

def _variant_blob(variant_row) -> str:
    """Text blob scoped to a *single offer row*.

    IMPORTANT: do NOT merge the header row here.
    For some properties (Occidental is the worst offender), the header row can
    contain meal-plan text from a different variant. If we merge it, we end up
    marking *all* variants as breakfast-included, which makes "no breakfast"
    offers disappear from the data.
    """
    chunks: List[str] = []
    try:
        vtxt = (variant_row.inner_text() or "").strip()
    except Exception:
        vtxt = ""
    if vtxt:
        chunks.append(vtxt)

    try:
        vtc = (variant_row.evaluate("(el) => el.textContent") or "").strip()
    except Exception:
        vtc = ""
    if vtc and vtc != vtxt:
        chunks.append(vtc)

    blob = " ".join([c for c in chunks if c]).strip()
    return re.sub(r"\s+", " ", blob)




def _offer_blob_for_price(price_el, max_depth: int = 8) -> str:
    # Return a tight text blob for the specific offer/variant that contains this price.
    # This is critical for optional-breakfast cases, where one room has multiple offers
    # (breakfast included vs breakfast for a fee) inside the SAME table row.
    try:
        return (price_el.evaluate(
            """(el, maxDepth) => {
              const clean = (s) => (s||'').replace(/\s+/g,' ').trim();
              const hasSignal = (t) => {
                const s = (t||'').toLowerCase();
                return (
                  s.includes('breakfast') || s.includes('dinner') || s.includes('half') ||
                  s.includes('cancellation') || s.includes('non-refundable') ||
                  s.includes('prepayment') || s.includes('pay at the property') ||
                  s.includes('select rooms') || s.includes('total cost to cancel')
                );
              };
              let node = el;
              let best = '';
              for (let d=0; d<maxDepth && node; d++){
                const t = clean(node.innerText || node.textContent || '');
                if (t && t.length > best.length && hasSignal(t)) best = t;
                if (node.tagName && node.tagName.toLowerCase()==='tr') break;
                node = node.parentElement;
              }
              if (!best){
                // fallback: row-level
                const tr = el.closest('tr');
                if (tr) best = clean(tr.innerText || tr.textContent || '');
              }
              return best || '';
            }""",
            max_depth,
        ) or '').strip()
    except Exception:
        return ''
def _offer_blob_for_variant(room_name_row, variant_row) -> str:
    """Offer blob used for CAPACITY detection (may include header row).

    We still keep this local, but capacity signals (icons/max persons) sometimes
    only live on the header row.
    """
    chunks: List[str] = [_variant_blob(variant_row)]

    if room_name_row is not None:
        try:
            htxt = (room_name_row.inner_text() or "").strip()
        except Exception:
            htxt = ""
        if htxt:
            chunks.append(htxt)

        try:
            htc = (room_name_row.evaluate("(el) => el.textContent") or "").strip()
        except Exception:
            htc = ""
        if htc and htc != htxt:
            chunks.append(htc)

    blob = " ".join([c for c in chunks if c]).strip()
    return re.sub(r"\s+", " ", blob)

def _detect_offer_capacity(room_name_row, variant_row, blob: str) -> Optional[int]:
    """
    PER-OFFER capacity:
    1) "Only for X guest" / language variants in offer blob
    2) "Sleeps: A - B guests" in offer blob
    3) icons INSIDE variant row (not header)
    4) "Max persons: X" in offer blob
    5) fallback: header icons / header max persons
    """
    only_for = _extract_only_for_guest(blob)
    if only_for is not None:
        return only_for

    sleeps = _extract_sleeps_capacity(blob)
    if sleeps is not None:
        return sleeps

    icons_variant = _count_person_icons_in_node(variant_row)
    if icons_variant:
        return icons_variant

    mp = _extract_max_persons_from_text(blob)
    if mp is not None:
        return mp

    # fallbacks:
    icons_header = _count_person_icons_in_node(room_name_row) if room_name_row is not None else None
    if icons_header:
        return icons_header

    try:
        htxt = (room_name_row.inner_text() or "") if room_name_row is not None else ""
    except Exception:
        htxt = ""
    mp2 = _extract_max_persons_from_text(htxt)
    return mp2

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
    dbg_left = DEBUG_CAPACITY_LIMIT

    for tr in trs:
        try:
            nm = _is_room_name_row(tr)
            if nm:
                current_room = nm
                current_room_row = tr
                continue

            if not current_room or not current_room_row:
                continue

            blob = _offer_blob_for_variant(current_room_row, tr)
            if DEBUG_MEAL_BLOB and ("breakfast" in blob.lower() or "dinner" in blob.lower() or "half" in blob.lower()):
                print("🧪 MEAL_BLOB:", blob[:800])

            cap = _detect_offer_capacity(current_room_row, tr, blob)
            if DEBUG_CAPACITY and dbg_left > 0:
                dbg_left -= 1
                print(f"🧪 CAP adults={adults} cap={cap} room='{current_room[:50]}' blob_has='{'B&D' if 'breakfast & dinner' in blob.lower() else ''}{' breakfast' if 'breakfast' in blob.lower() else ''}'")

            # STRICT: require cap == adults (exact)
            if cap is None or cap != adults:
                continue

            # --- Variant attributes (meals/refund/prepay) ---
            # IMPORTANT:

            # We must compute flags per *offer* (per price element), not per variant row.
            # Booking often renders multiple offers inside the same <tr>.

            header_txt = ""
            try:
                header_txt = (current_room_row.inner_text() or "").strip()
            except Exception:
                header_txt = ""

            vtxt = blob  # used only as fallback text for price parsing

            found_price = False
            for psel in PRICE_SELECTORS:
                for pel in tr.query_selector_all(psel):
                    try:
                        ptxt = (pel.inner_text() or "").strip()
                    except Exception:
                        ptxt = ""
                    price = clean_price(ptxt)
                    if price is None:
                        # fallback: sometimes the price text is not directly on the element
                        price = clean_price(pel.text_content() or "")
                    if price is None:
                        continue

                    offer_blob = _offer_blob_for_price(pel, tr, current_room_row)

                    # Header fallback only if the offer blob has *no* meal keywords.
                    if header_txt and not re.search(r"(breakfast|dinner|half[-\s]?board|halfboard|breakfast\s*&\s*dinner)", offer_blob, re.I):
                        if re.search(r"(breakfast|dinner|half[-\s]?board|halfboard|breakfast\s*&\s*dinner)", header_txt, re.I):
                            offer_blob = (offer_blob + " " + header_txt).strip()

                    attrs = parse_rate_attributes(offer_blob)
                    rate_key = build_rate_key(attrs)

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
                # last resort: try to parse a price from the variant blob; flags from the variant row
                price = clean_price(vtxt)
                if price is not None:
                    attrs = parse_rate_attributes(vtxt)
                    rate_key = build_rate_key(attrs)
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

        # strict capacity (exact)
        cap = _extract_only_for_guest(txt) or _extract_sleeps_capacity(txt) or _extract_max_persons_from_text(txt) or _count_person_icons_in_node(c)
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
