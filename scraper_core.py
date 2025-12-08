# scraper_core.py

import os
import re
from typing import Any, Callable, Dict, List, Optional

import requests
from requests import HTTPError

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


# ─────────────────────────────────────────────────────────────
# Env + Supabase helpers
# ─────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
# Prefer service-role key; fall back if needed
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_SERVICE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
)

DEFAULT_CC = os.getenv("DEFAULT_CC", "si").lower()
RESOLVE_CC_IF_MISSING = os.getenv("RESOLVE_CC_IF_MISSING", "1") == "1"

SESSION = requests.Session()


def supabase_headers(json_pref: bool = True, upsert: bool = False) -> Dict[str, str]:
    h: Dict[str, str] = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if json_pref:
        h["Content-Type"] = "application/json"
        h["Accept"] = "application/json"
    if upsert:
        # caller can still override Prefer manually when needed
        h.setdefault("Prefer", "resolution=merge-duplicates")
    return h


def _sb_get(path: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{SUPABASE_URL}{path}"
    r = SESSION.get(url, params=params, headers=supabase_headers(json_pref=True))
    r.raise_for_status()
    return r


# ─────────────────────────────────────────────────────────────
# Booking.com slug / cc helpers
# ─────────────────────────────────────────────────────────────


def booking_slug_from_url(url: str) -> str:
    """
    Turn a full Booking.com URL into a canonical slug used by the scraper,
    e.g. https://www.booking.com/hotel/si/nox.html?anything -> "hotel/si/nox"
    """
    try:
        if "booking.com/" in url:
            url = url.split("booking.com/", 1)[1]
        url = url.split("?", 1)[0]
        url = url.strip("/")
        if url.endswith(".html"):
            url = url[:-5]
        return url.lower()
    except Exception:
        return url.strip().lower()


def infer_cc_from_slug(slug: str) -> Optional[str]:
    slug = (slug or "").strip().lower()
    parts = slug.split("/")
    # typical: "hotel/si/nox" -> "si"
    if len(parts) >= 3 and len(parts[1]) == 2:
        return parts[1]
    return None


def resolve_cc_for_slug(slug: str) -> Optional[str]:
    """
    Best-effort country-code resolver: infer from slug or fall back to DEFAULT_CC.
    """
    cc = infer_cc_from_slug(slug)
    return cc or DEFAULT_CC


# ─────────────────────────────────────────────────────────────
# Hotel lookup helpers
# ─────────────────────────────────────────────────────────────


def get_own_hotel(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Resolve the user's OWN hotel from user_hotels → hotels.

    Returns dict:
       {
         "hotel_id": ...,
         "name": ...,
         "slug": "hotel/si/nox",
         "cc": "si",
         "own": True,
       }
    or None if nothing is configured.
    """
    try:
        params = {
            "select": "hotel_id,link_type,hotels(name,url,id)",
            "user_id": f"eq.{user_id}",
            "link_type": "eq.own",
            "limit": 1,
        }
        r = _sb_get("/rest/v1/user_hotels", params)
        rows = r.json()
        if not rows:
            return None

        row = rows[0]
        hotels_obj = row.get("hotels") or {}
        name = hotels_obj.get("name") or "Own hotel"
        url = hotels_obj.get("url") or ""

        slug = booking_slug_from_url(url)
        cc = resolve_cc_for_slug(slug) if RESOLVE_CC_IF_MISSING else DEFAULT_CC

        return {
            "hotel_id": row.get("hotel_id") or hotels_obj.get("id"),
            "name": name,
            "slug": slug,
            "cc": cc,
            "own": True,
        }
    except HTTPError as e:
        print("⚠️ get_own_hotel HTTP error:", e, getattr(e.response, "text", "")[:400])
        return None
    except Exception as e:
        print("⚠️ get_own_hotel error:", e)
        return None


def get_competitor_hotels(
    user_id: str,
    own_hotel_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Resolve competitor hotels from user_competitors.

    Returns list of dicts:
      { "hotel_id", "name", "slug", "cc", "own": False }
    """
    try:
        params = {
            "select": "id,comp_name,booking_slug,booking_cc",
            "user_id": f"eq.{user_id}",
        }
        r = _sb_get("/rest/v1/user_competitors", params)
        rows = r.json() or []
    except HTTPError as e:
        print(
            "⚠️ get_competitor_hotels HTTP error:",
            e,
            getattr(e.response, "text", "")[:400],
        )
        rows = []
    except Exception as e:
        print("⚠️ get_competitor_hotels error:", e)
        rows = []

    out: List[Dict[str, Any]] = []
    for row in rows:
        slug_raw = (row.get("booking_slug") or "").strip().lower()
        if not slug_raw:
            continue

        cc = (row.get("booking_cc") or "").strip().lower()
        if not cc and RESOLVE_CC_IF_MISSING:
            cc = resolve_cc_for_slug(slug_raw)
        if not cc:
            cc = DEFAULT_CC

        name = (row.get("comp_name") or slug_raw).strip() or slug_raw

        out.append(
            {
                "hotel_id": row.get("id"),
                "name": name,
                "slug": slug_raw,
                "cc": cc,
                "own": False,
            }
        )

    return out


# ─────────────────────────────────────────────────────────────
# Scraping helpers (Booking.com)
# ─────────────────────────────────────────────────────────────

_BOOL_TRUE = {"1", "true", "yes", "y", "on"}


def _launch_browser():
    from playwright.sync_api import Browser

    no_sandbox = os.getenv("NO_SANDBOX", "1").lower() in _BOOL_TRUE
    args: List[str] = []
    if no_sandbox:
        args.extend(["--no-sandbox", "--disable-setuid-sandbox"])

    pw = sync_playwright().start()
    browser: Browser = pw.chromium.launch(headless=True, args=args)
    return pw, browser


def _parse_rate_flags(raw_text: str) -> Dict[str, Any]:
    """
    Parse human-readable rate text into structured flags:

      - breakfast_included
      - dinner_included
      - half_board
      - free_cancellation
      - nonrefundable
      - prepay_required
      - rate_plan (compact label for UI)

    NOTE: dinner/half-board detection is intentionally conservative so we
    DON'T accidentally mark BB as HB for Nox etc.
    """
    txt = " ".join(raw_text.split()).lower()

    breakfast_included = any(
        kw in txt
        for kw in [
            "breakfast included",
            "includes breakfast",
            "zajtrk vključen",
            "z zajtrkom",
        ]
    )

    # Be *very* strict to avoid false "dinner" for B&B
    dinner_phrases = [
        "dinner included",
        "evening meal included",
        "večerja vključena",
        "z večerjo",
    ]
    half_board_phrases = [
        "half board",
        "half-board",
        "polpenzion",
        "hb ",
        "(hb)",
    ]

    dinner_included = any(kw in txt for kw in dinner_phrases)
    half_board = any(kw in txt for kw in half_board_phrases)

    free_cancellation = any(
        kw in txt
        for kw in [
            "free cancellation",
            "brezplačna odpoved",
            "brezplačno odpoved",
        ]
    )

    nonrefundable = any(
        kw in txt
        for kw in [
            "non-refundable",
            "nonrefundable",
            "nepovratno",
        ]
    )

    prepay_required = any(
        kw in txt
        for kw in [
            "prepayment required",
            "payment in advance",
            "predplačilo",
        ]
    )

    # Simple label for debugging / charts
    rate_bits: List[str] = []
    if breakfast_included:
        rate_bits.append("BB")
    if half_board:
        rate_bits.append("HB")
    if dinner_included and not half_board:
        rate_bits.append("DIN")
    if free_cancellation:
        rate_bits.append("FLEX")
    if nonrefundable:
        rate_bits.append("NRF")

    rate_plan = "+".join(rate_bits) if rate_bits else ""

    return {
        "breakfast_included": breakfast_included,
        "dinner_included": dinner_included,
        "half_board": half_board,
        "free_cancellation": free_cancellation,
        "nonrefundable": nonrefundable,
        "prepay_required": prepay_required,
        "rate_plan": rate_plan,
    }


def _extract_price_from_text(txt: str) -> Optional[float]:
    """
    Extract a float from Booking-style price strings ("€ 123", "1.234,56 €", ...).
    """
    txt = txt.replace("\xa0", " ")
    m = re.search(r"(\d[\d\.\,]*)", txt)
    if not m:
        return None
    num = m.group(1)

    # Normalize: remove thousands, convert decimal to dot
    if "." in num and "," in num:
        # If dot comes before comma, assume dot is thousands and comma is decimal (EU style)
        if num.find(".") < num.find(","):
            num = num.replace(".", "").replace(",", ".")
        else:
            num = num.replace(",", "")
    else:
        num = num.replace(".", "").replace(",", ".")

    try:
        return float(num)
    except ValueError:
        return None


def scrape_hotel_for_dates(
    hotel_name: str,
    slug: str,
    cc: Optional[str],
    checkin: str,
    checkout: str,
    should_cancel: Callable[[], bool],
) -> List[Dict[str, Any]]:
    """
    Core Booking.com scraper for a single hotel and (checkin, checkout) pair.

    Returns a list of raw rows like:
      {
        "slug": slug,
        "checkin": checkin,
        "room": <room name>,
        "occupancy": <int>,
        "price": <float>,
        "hotel": hotel_name,
        ...rate flags...
      }

    This version:
      - Uses sync Playwright (no route interception) to avoid the
        giant asyncio CancelledError spam you were seeing.
      - Loops over adults = 1..4; worker then dedupes per room/occ.
    """
    slug = slug.strip().lower()
    rows: List[Dict[str, Any]] = []

    pw, browser = _launch_browser()
    context = None
    try:
        context = browser.new_context(locale="en-GB")
        page = context.new_page()
        page.set_default_timeout(45000)

        for occ in (1, 2, 3, 4):
            if should_cancel():
                break

            base_url = f"https://www.booking.com/{slug}.html"
            query = (
                f"?checkin={checkin}"
                f"&checkout={checkout}"
                f"&group_adults={occ}"
                f"&group_children=0"
                f"&no_rooms=1"
                f"&selected_currency=EUR"
                f"&lang=en-gb"
                f"&sb_price_type=total"
            )
            full_url = base_url + query

            print(
                f"🏨 {hotel_name} | {checkin}→{checkout} | adults={occ}\n   {full_url}"
            )

            try:
                page.goto(full_url, wait_until="networkidle")
            except PlaywrightTimeoutError:
                # still try to parse whatever rendered
                pass
            except Exception as e:
                print("⚠️ goto error:", e)
                continue

            if should_cancel():
                break

            # Booking DOM changes often; try a couple of patterns.
            room_cards = page.query_selector_all('[data-testid="room-card"]')
            if not room_cards:
                room_cards = page.query_selector_all('[data-testid*="roomrow"]')

            for card in room_cards:
                if should_cancel():
                    break

                # Room name
                room_name_el = card.query_selector('[data-testid="room-name"]') or card.query_selector(
                    "h3, h2"
                )
                room_name = (
                    (room_name_el.inner_text().strip()) if room_name_el else ""
                )
                if not room_name:
                    continue

                # Price
                price_el = card.query_selector(
                    '[data-testid="price-and-discounted-price"]'
                ) or card.query_selector('[data-testid="price"]')
                price_val: Optional[float] = None
                if price_el:
                    try:
                        price_text = price_el.inner_text().strip()
                        price_val = _extract_price_from_text(price_text)
                    except Exception:
                        price_val = None

                if price_val is None:
                    continue

                # Conditions / rate-text (for board, cancellation, etc.)
                cond_el = card.query_selector('[data-testid="policy"]') or card
                cond_text = ""
                try:
                    cond_text = cond_el.inner_text()
                except Exception:
                    cond_text = ""

                flags = _parse_rate_flags(cond_text)

                rows.append(
                    {
                        "slug": slug,
                        "checkin": checkin,
                        "room": room_name,
                        "occupancy": occ,
                        "price": price_val,
                        "hotel": hotel_name,
                        **flags,
                    }
                )

        return rows

    finally:
        try:
            if context is not None:
                context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Dedup helper (min per room+occupancy)
# ─────────────────────────────────────────────────────────────


def dedupe_min_per_room_and_occupancy(
    raw_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    From raw rows (possibly multiple rate options per room/occ),
    keep the cheapest one.

    This is what worker_poll upserts into room_prices_raw:
      - one min row per (room, occ) per checkin.
    """
    best: Dict[tuple, Dict[str, Any]] = {}

    for r in raw_rows:
        room = r.get("room")
        occ = r.get("occupancy")
        price = r.get("price")

        if not room or price is None:
            continue

        try:
            occ_int = int(occ)
        except Exception:
            continue

        try:
            price_f = float(price)
        except Exception:
            continue

        key = (room, occ_int)
        prev = best.get(key)
        if prev is None or price_f < float(prev.get("price", 1e12)):
            newr = dict(r)
            newr["occupancy"] = occ_int
            newr["price"] = price_f
            best[key] = newr

    return list(best.values())
