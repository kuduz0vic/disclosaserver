# scraper_core.py
# ------------------------------------------------------------------------------
# Scraper/core with retries + crash recovery and FULL rate-variant capture.
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
#   WAIT_TABLE_TIMEOUT_MS=8000
#   SCROLL_PASSES=12
#   NO_SANDBOX=1
#   NAV_RETRIES=3
#   RECREATE_BROWSER_ON_CRASH=1
#   PREMIUM_ALL_RATES=1       # if 0 -> you can later aggregate elsewhere
# ------------------------------------------------------------------------------

import os
import re
from time import sleep
from typing import Optional, Dict, Any, List, Callable
from urllib.parse import urlencode, urlparse

import requests
from requests import HTTPError
from playwright.sync_api import sync_playwright, TimeoutError
from functools import lru_cache
import hashlib
import json

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
COMMON_CCS = (os.getenv("COMMON_CCS") or "si,at,de,it,hr,hu,cz,sk,pl,fr,es,pt,nl,be,dk,se,no,fi,gb,ie,ch,gr").split(",")

PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "20000"))
WAIT_TABLE_TIMEOUT_MS = int(os.getenv("WAIT_TABLE_TIMEOUT_MS", "8000"))
SCROLL_PASSES = int(os.getenv("SCROLL_PASSES", "12"))

NAV_RETRIES = int(os.getenv("NAV_RETRIES", "3"))
RECREATE_BROWSER_ON_CRASH = os.getenv("RECREATE_BROWSER_ON_CRASH", "1") == "1"
PREMIUM_ALL_RATES = os.getenv("PREMIUM_ALL_RATES", "1") == "1"

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
    if not links: return None
    for l in links:
        if l.get("link_type") == "own":
            slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
            if slug:
                cc = (l.get("booking_cc") or "").lower() or None
                return {"hotel_id": l["hotel_id"], "name": l["name"] or "My Hotel", "slug": slug, "cc": cc}
    first = links[0]
    slug = _normalize_slug(first.get("booking_slug") or first.get("url") or "")
    if not slug: return None
    cc = (first.get("booking_cc") or "").lower() or None
    return {"hotel_id": first["hotel_id"], "name": first["name"] or "My Hotel", "slug": slug, "cc": cc}

def get_competitor_hotels(user_id: str, own_hotel_id: Optional[str]) -> List[dict]:
    links = _get_user_links(user_id)
    out: List[dict] = []
    for l in links:
        if own_hotel_id and l["hotel_id"] == own_hotel_id:
            continue
        slug = _normalize_slug(l.get("booking_slug") or l.get("url") or "")
        if not slug: continue
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
# Scraper helpers (retries, parsing)
# ──────────────────────────────────────────────────────────────────────────────
EXPAND_SELECTORS = [
    "button:has-text('Show all')","button:has-text('Show more')",
    "button:has-text('See all rooms')","a:has-text('Show all')",
    "a:has-text('Show more')","a:has-text('See all rooms')",
    "button:has-text('Prikaži več')","button:has-text('Prikaži vse')",
    "a:has-text('Prikaži več')","a:has-text('Prikaži vse')",
]
ROOM_TABLE_SELECTORS = ["tr.js-rt-block-row","table.hprt-table tr","[data-testid='room-row']"]
PRICE_SELECTORS = [
    ".prco-valign-middle-helper",".bui-price-display__value",
    "[data-testid='price-and-discounted-price']", ".prco-inline-price",
]

OCC_PATTERNS = [
    r"\bfor\s+(\d+)\s+adults?\b", r"\bprice\s+for\s+(\d+)\s+adults?\b", r"\b(\d+)\s+adults?\b",
    r"\bfor\s+(\d+)\s+people\b", r"\b(\d+)\s+guests?\b", r"\bsleeps\s+(\d+)\b",
    r"\bza\s+(\d+)\s*oseb[ioa]?\b", r"\b(\d+)\s*oseb[ioa]?\b", r"\bnajvečje\s+število\s+oseb:\s*(\d+)\b",
    r"\bsamo\s+za\s+(\d+)\s+gost[oa]?\b",
]

MEAL_WORDS = ["breakfast", "zajtrk", "doručak", "colazione", "petit déjeuner", " frühstück"]
REFUND_WORDS = ["free cancellation", "brezplačna odpoved", "odpoved", "refund", "refundable"]
NONREF_WORDS = ["non-refundable", "nerazpovratno", "non refundable"]
PREPAY_WORDS = ["prepayment", "predplačilo", "advance", "pay in advance", "plačate vnaprej"]

def clean_price(text: str) -> Optional[float]:
    if not text:
        return None
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

def feature_flags(text: str) -> Dict[str, bool]:
    t = (text or "").lower()
    breakfast = any(w in t for w in MEAL_WORDS)
    refundable = any(w in t for w in REFUND_WORDS) and not any(w in t for w in NONREF_WORDS)
    prepaid = any(w in t for w in PREPAY_WORDS) or any("prepay" in s for s in [t])
    return {"breakfast": breakfast, "refundable": refundable, "prepaid": prepaid}

def make_rate_key(room_name: str, occ: int, near_text: str) -> str:
    # robust fingerprint of the “plan” block context
    raw = json.dumps(
        {"room": room_name.strip().lower(), "occ": occ, "txt": " ".join(near_text.split()).lower()},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def row_level_occupancy_hint(row) -> Optional[int]:
    try:
        txt = row.inner_text() or ""
        return find_adults_in_text(txt)
    except Exception:
        return None

def get_occupancy_from_name(name: str) -> int:
    n = (name or "").lower()
    if any(x in n for x in ["triposteljna", "troposteljna", "triple"]): return 3
    if any(x in n for x in ["štiriposteljna", "stiriposteljna", "quadruple", "družinska", "familij"]): return 4
    if any(x in n for x in ["enoposteljna", "single"]): return 1
    return 2

def is_valid_room(name: str) -> bool:
    n = (name or "").lower()
    return not any(x in n for x in ["dnevna soba", "review", "rezultati", "ocena"])

def build_hotel_url(cc: Optional[str], slug: str, checkin: str, checkout: str, adults: int, lang="en-gb"):
    cc_eff = (cc or DEFAULT_CC).lower()
    base = f"https://www.booking.com/hotel/{cc_eff}/{slug}.html"
    qs = {
        "checkin": checkin, "checkout": checkout, "group_adults": adults,
        "group_children": 0, "no_rooms": 1, "selected_currency": "EUR",
        "lang": lang, "sb_price_type": "total",
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
        if h == last_h: break
        last_h = h

def extract_price_with_context(price_el):
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
                // collect nearby visible text to fingerprint the plan
                const texts = [];
                let node = el;
                for (let i=0; i<4 && node; i++){
                  texts.push(node.innerText || '');
                  node = node.parentElement;
                }
                return texts.join(' ');
            }"""
        )
    except Exception:
        context_text = ""
    return price, context_text

def collect_rate_variants_for_adults(page, adults: int) -> List[Dict[str, Any]]:
    """
    Collect ALL visible rate variants for each room for the requested 'adults'.
    Each variant includes: price, rate_key, plan JSON, flags.
    """
    variants: List[Dict[str, Any]] = []

    for table_sel in ROOM_TABLE_SELECTORS:
        rows = page.query_selector_all(table_sel)
        for row in rows:
            try:
                name_el = (
                    row.query_selector("[data-testid='room-name']")
                    or row.query_selector(".hprt-roomtype-icon-link")
                    or row.query_selector(".hprt-roomtype-name")
                )
                room_name = (name_el.inner_text().strip() if name_el else None)
                if not room_name or not is_valid_room(room_name):
                    continue

                row_occ = row_level_occupancy_hint(row)
                any_for_occ = False

                # Walk all possible price “cells” and read the price + surrounding text
                for td in row.query_selector_all("td,div,section"):
                    for psel in PRICE_SELECTORS:
                        pel = td.query_selector(psel)
                        if not pel:
                            continue
                        price, near_text = extract_price_with_context(pel)
                        if price is None:
                            continue

                        # determine occupancy for this price
                        occ_ctx = find_adults_in_text(near_text)
                        if occ_ctx is not None:
                            occ_final = occ_ctx
                        elif row_occ is not None:
                            occ_final = row_occ
                        else:
                            occ_final = get_occupancy_from_name(room_name)

                        if occ_final != adults:
                            continue

                        flags = feature_flags(near_text)
                        rkey = make_rate_key(room_name, occ_final, near_text)

                        variants.append({
                            "room": re.sub(r"\s+", " ", room_name),
                            "occupancy": occ_final,
                            "price": price,
                            "rate_key": rkey,
                            "plan": {
                                "near_text": " ".join(near_text.split())[:1000],
                            },
                            "breakfast": flags["breakfast"],
                            "refundable": flags["refundable"],
                            "prepaid": flags["prepaid"],
                        })
                        any_for_occ = True

                # Fallback: if nothing matched by context, infer & take the first visible price
                if not any_for_occ:
                    inferred = get_occupancy_from_name(room_name)
                    if inferred == adults:
                        first_price, first_text = None, ""
                        for td in row.query_selector_all("td,div,section"):
                            for psel in PRICE_SELECTORS:
                                pel = td.query_selector(psel)
                                if pel:
                                    first_price = clean_price(pel.inner_text())
                                    if first_price is not None:
                                        try:
                                            first_text = pel.inner_text() or ""
                                        except Exception:
                                            first_text = ""
                                        break
                            if first_price is not None:
                                break
                        if first_price is not None:
                            flags = feature_flags(first_text)
                            rkey = make_rate_key(room_name, inferred, first_text)
                            variants.append({
                                "room": re.sub(r"\s+", " ", room_name),
                                "occupancy": inferred,
                                "price": first_price,
                                "rate_key": rkey,
                                "plan": {"near_text": " ".join(first_text.split())[:1000]},
                                "breakfast": flags["breakfast"],
                                "refundable": flags["refundable"],
                                "prepaid": flags["prepaid"],
                            })

            except Exception:
                continue

    # If you later want to “basic mode”, you could reduce to min/max here.
    if not PREMIUM_ALL_RATES:
        reduced: Dict[tuple, List[Dict[str, Any]]] = {}
        for v in variants:
            key = (v["room"], v["occupancy"])
            reduced.setdefault(key, []).append(v)
        out: List[Dict[str, Any]] = []
        for key, arr in reduced.items():
            mn = min(arr, key=lambda x: x["price"])
            mx = max(arr, key=lambda x: x["price"])
            out.extend([mn] + ([mx] if mx["price"] != mn["price"] else []))
        return out

    return variants

# Robust navigation
def goto_with_retries(page, url: str, attempts: int, timeout_ms: int):
    last_err = None
    for i in range(1, attempts + 1):
        try:
            page.goto(url, timeout=timeout_ms, wait_until="load")
            return
        except TimeoutError as e:
            last_err = e
            print(f"⚠️ Timeout #{i} loading: {url}")
        except Exception as e:
            last_err = e
            print(f"⚠️ goto crash #{i}: {e}")
        sleep(0.6 * i)
    raise last_err

def scrape_hotel_for_dates(
    name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[dict]:
    def cancelled() -> bool:
        return bool(should_cancel and should_cancel())

    out: List[dict] = []
    with sync_playwright() as p:
        extra_args = ["--no-sandbox"] if os.getenv("NO_SANDBOX") == "1" else []
        browser = p.chromium.launch(headless=True, args=extra_args)

        def new_context_and_page():
            ctx = browser.new_context(locale="en-GB", viewport={"width": 1400, "height": 900}, user_agent=USER_AGENT)
            ctx.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image", "media", "font"} else route.continue_())
            return ctx, ctx.new_page()

        context, page = new_context_and_page()

        def recreate_if_crashed():
            nonlocal context, page
            try:
                page.title()
                return
            except Exception:
                try: context.close()
                except Exception: pass
                context, page = new_context_and_page()

        for adults in (1, 2, 3, 4):
            if cancelled(): break
            url = build_hotel_url(cc, slug, checkin, checkout, adults, lang="en-gb")
            print(f"🏨 {name} | {checkin}→{checkout} | adults={adults}\n   {url}")
            try:
                recreate_if_crashed()
                goto_with_retries(page, url, attempts=NAV_RETRIES, timeout_ms=PAGE_GOTO_TIMEOUT_MS)
            except Exception as e:
                print("⚠️ Navigation failed after retries:", e)
                if RECREATE_BROWSER_ON_CRASH:
                    recreate_if_crashed()
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

                variants = collect_rate_variants_for_adults(page, adults)
                for v in variants:
                    out.append({
                        "hotel": name, "slug": slug.lower(), "checkin": checkin,
                        "room": v["room"], "occupancy": v["occupancy"], "price": v["price"],
                        "rate_key": v["rate_key"], "plan": v["plan"],
                        "breakfast": v["breakfast"], "refundable": v["refundable"], "prepaid": v["prepaid"],
                    })

            except Exception as e:
                print("⚠️ Page error:", e)
                if RECREATE_BROWSER_ON_CRASH:
                    recreate_if_crashed()
                continue

        try: context.close()
        except Exception: pass
        browser.close()
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Post-processing
# (we no longer dedupe to min-only; keep all, or min/max if PREMIUM_ALL_RATES=0)
# ──────────────────────────────────────────────────────────────────────────────
def dedupe_min_per_room_and_occupancy(rows: List[dict]) -> List[dict]:
    # Keep ALL variants; use rate_key uniqueness if present.
    # If upstream accidentally emits exact duplicates, drop perfect dupes.
    seen = set()
    out = []
    for r in rows:
        key = (
            (r.get("slug") or "").lower(),
            r.get("checkin"),
            r.get("room"),
            int(r.get("occupancy") or 0),
            r.get("rate_key") or "",
            float(r.get("price") or 0.0),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
