import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import httpx
from playwright.async_api import async_playwright, Browser, Page

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class RoomPriceRow:
    user_id: str
    slug: str
    checkin: dt.date
    occupancy: int
    room_name: str
    price: Optional[float]
    breakfast_included: Optional[bool] = None
    dinner_included: Optional[bool] = None
    half_board: Optional[bool] = None
    free_cancellation: Optional[bool] = None
    nonrefundable: Optional[bool] = None
    prepay_required: Optional[bool] = None
    rate_plan: Optional[str] = None
    rate_key: Optional[str] = None
    inserted_at: Optional[str] = None  # ISO timestamp


class ScraperCore:
    """
    Core booking.com scraper.

    - Uses Playwright
    - Always uses group_adults=4 to see full room capacity
    - Writes rows to Supabase room_prices_raw
    """

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        *,
        user_id: str,
        project: str = "disclosa",
        http_timeout: float = 30.0,
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.user_id = user_id
        self.project = project
        self.http_timeout = http_timeout

        self._http = httpx.AsyncClient(
            timeout=http_timeout,
            headers={
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
                "Content-Type": "application/json",
            },
        )

    # ─────────────────────────────────────────
    # Public entrypoints
    # ─────────────────────────────────────────

    async def scrape_hotel_range(
        self,
        slug: str,
        country_code: str,
        start_date: dt.date,
        days: int,
    ) -> None:
        """
        Scrape `days` nights starting from `start_date` for one hotel.
        """

        end_date = start_date + dt.timedelta(days=days)
        logger.info(
            "[SCRAPER] user=%s slug=%s range=%s→%s",
            self.user_id,
            slug,
            start_date,
            end_date,
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                for offset in range(days):
                    checkin = start_date + dt.timedelta(days=offset)
                    checkout = checkin + dt.timedelta(days=1)

                    try:
                        rows = await self._scrape_single_day(
                            browser=browser,
                            slug=slug,
                            country_code=country_code,
                            checkin=checkin,
                            checkout=checkout,
                        )
                        await self._upsert_room_prices(slug, checkin, rows)
                    except Exception as e:
                        logger.exception(
                            "[SCRAPER] Failed for %s %s: %s", slug, checkin, e
                        )
                        # still continue next day
            finally:
                await browser.close()

    async def scrape_single_day(
        self,
        slug: str,
        country_code: str,
        checkin: dt.date,
    ) -> None:
        """
        Convenience wrapper for single day (1 night).
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                checkout = checkin + dt.timedelta(days=1)
                rows = await self._scrape_single_day(
                    browser=browser,
                    slug=slug,
                    country_code=country_code,
                    checkin=checkin,
                    checkout=checkout,
                )
                await self._upsert_room_prices(slug, checkin, rows)
            finally:
                await browser.close()

    # ─────────────────────────────────────────
    # Internal scraping
    # ─────────────────────────────────────────

    async def _scrape_single_day(
        self,
        browser: Browser,
        slug: str,
        country_code: str,
        checkin: dt.date,
        checkout: dt.date,
    ) -> List[RoomPriceRow]:
        """
        Load booking.com hotel page and parse room cards.

        We rely on:
        - ?checkin=YYYY-MM-DD&checkout=YYYY-MM-DD
        - group_adults=4&group_children=0&no_rooms=1
        """

        checkin_str = checkin.isoformat()
        checkout_str = checkout.isoformat()

        hotel_url = self._build_booking_url(
            slug=slug,
            country_code=country_code,
            checkin=checkin_str,
            checkout=checkout_str,
        )

        logger.info(
            "[SCRAPER] Fetching slug=%s cc=%s checkin=%s",
            slug,
            country_code,
            checkin_str,
        )

        page = await browser.new_page()
        await page.goto(hotel_url, wait_until="networkidle")

        # ensure all room content loaded (scroll etc.)
        await self._auto_scroll(page)

        # NOTE: selectors are fragile; adjust to your current DOM
        room_cards = await page.query_selector_all('[data-testid="room-card"]')
        logger.info("[SCRAPER] slug=%s rooms found=%d", slug, len(room_cards))

        rows: List[RoomPriceRow] = []
        now_iso = dt.datetime.utcnow().isoformat()

        for card in room_cards:
            try:
                title_el = await card.query_selector('[data-testid="title"]')
                room_name = (await title_el.inner_text()) if title_el else "Room"

                # occupancy – fallback to 2 if not parseable
                occ = await self._parse_occupancy(card)
                if not occ or occ < 1:
                    occ = 2

                price = await self._parse_price(card)

                plan_text = await self._extract_plan_text(card)
                flags = self._derive_flags_from_text(plan_text)

                rows.append(
                    RoomPriceRow(
                        user_id=self.user_id,
                        slug=slug,
                        checkin=checkin,
                        occupancy=occ,
                        room_name=room_name.strip(),
                        price=price,
                        breakfast_included=flags.get("breakfast"),
                        dinner_included=flags.get("dinner"),
                        half_board=flags.get("half_board"),
                        free_cancellation=flags.get("free_cancellation"),
                        nonrefundable=flags.get("nonrefundable"),
                        prepay_required=flags.get("prepay_required"),
                        rate_plan=plan_text[:255] if plan_text else None,
                        rate_key=None,
                        inserted_at=now_iso,
                    )
                )
            except Exception as e:
                logger.exception("[SCRAPER] error parsing card: %s", e)

        await page.close()
        return rows

    def _build_booking_url(
        self,
        slug: str,
        country_code: str,
        checkin: str,
        checkout: str,
    ) -> str:
        # e.g. https://www.booking.com/hotel/si/<slug>.html
        cc = country_code.lower()
        if cc:
            base = f"https://www.booking.com/hotel/{cc}/{slug}.html"
        else:
            # fallback if we already pass full slug like "nox-ljubljana"
            base = f"https://www.booking.com/hotel/{slug}.html"

        params = (
            f"?checkin={checkin}"
            f"&checkout={checkout}"
            "&group_adults=4"
            "&group_children=0"
            "&no_rooms=1"
            "&selected_currency=EUR"
            "&lang=en-gb"
            "&sb_price_type=total"
        )
        return base + params

    async def _auto_scroll(self, page: Page, *, step: int = 600, delay_ms: int = 200):
        """Scroll down progressively to trigger lazy-loaded rooms."""
        prev_height = 0
        while True:
            height = await page.evaluate("document.body.scrollHeight")
            if height == prev_height:
                break
            prev_height = height
            await page.mouse.wheel(0, step)
            await page.wait_for_timeout(delay_ms)

    async def _parse_occupancy(self, card: Any) -> Optional[int]:
        """
        Try to parse occupancy from the bed/people icon, fallback heuristics.
        """
        occ_el = await card.query_selector('[data-testid="occupancy-group"]')
        if not occ_el:
            return None
        text = (await occ_el.inner_text()).strip().lower()
        # e.g. "Sleeps 3", "Sleeps 4 guests"
        for n in range(1, 7):
            if str(n) in text:
                return n
        return None

    async def _parse_price(self, card: Any) -> Optional[float]:
        price_el = await card.query_selector('[data-testid="price-and-discounted-price"]')
        if not price_el:
            price_el = await card.query_selector('[data-testid="price"]')
        if not price_el:
            return None

        text = (await price_el.inner_text()).strip()
        # Typical format: "€ 145" or "€145"
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return None
        try:
            return float(digits)
        except ValueError:
            return None

    async def _extract_plan_text(self, card: Any) -> str:
        """
        Extract textual description that contains plan info: breakfast,
        cancellation, prepayment, etc.
        """
        chunks: List[str] = []
        plan_els = await card.query_selector_all('[data-testid="mealplan"]')
        for el in plan_els:
            txt = (await el.inner_text()).strip()
            if txt:
                chunks.append(txt)

        policy_els = await card.query_selector_all('[data-testid="policy"]')
        for el in policy_els:
            txt = (await el.inner_text()).strip()
            if txt:
                chunks.append(txt)

        return " ".join(chunks)

    # ─────────────────────────────────────────
    # Flag derivation (mirrors frontend)
    # ─────────────────────────────────────────

    def _derive_flags_from_text(self, text: str) -> Dict[str, bool]:
        t = (text or "").lower()

        breakfast = (
            ("breakfast" in t or "b&b" in t or "bed & breakfast" in t)
            and "no breakfast" not in t
            and "breakfast not included" not in t
        )

        dinner_explicit = ("dinner" in t or "supper" in t) and "no dinner" not in t
        breakfast_and_dinner = "breakfast" in t and "dinner" in t
        all_inclusive = "all inclusive" in t or "all-inclusive" in t or "ai board" in t
        dinner_via_hb = "half board" in t or "half-board" in t or "hb" in t

        dinner = (
            dinner_explicit
            or breakfast_and_dinner
            or all_inclusive
            or dinner_via_hb
            or "evening meal" in t
            or "buffet dinner" in t
        )

        half_board = (
            ("half board" in t or "half-board" in t or "hb" in t)
            and "no half board" not in t
            and "without half board" not in t
        )

        nonref = (
            "non-refundable" in t
            or "non refundable" in t
            or "nonref" in t
            or "no refund" in t
            or "no refunds" in t
            or "total cost to cancel" in t
        )

        free_cancellation = (
            ("free cancellation" in t or "fully refundable" in t or "free to cancel" in t)
            and not nonref
        )

        prepay_required = (
            "prepay" in t
            or "prepaid" in t
            or "pay in advance" in t
            or "charged in advance" in t
            or "payment before arrival" in t
            or "pay now" in t
        )

        return {
            "breakfast": breakfast,
            "dinner": dinner,
            "half_board": half_board,
            "free_cancellation": free_cancellation,
            "nonrefundable": nonref,
            "prepay_required": prepay_required,
        }

    # ─────────────────────────────────────────
    # Supabase persistence
    # ─────────────────────────────────────────

    async def _upsert_room_prices(
        self,
        slug: str,
        checkin: dt.date,
        rows: List[RoomPriceRow],
    ) -> None:
        """
        Delete old rows for (user_id, slug, checkin) and insert new snapshot.
        """
        logger.info(
            "[SCRAPER] Upserting %d rows slug=%s checkin=%s",
            len(rows),
            slug,
            checkin,
        )

        # 1) Delete old
        delete_url = (
            f"{self.supabase_url}/rest/v1/room_prices_raw"
            f"?user_id=eq.{self.user_id}"
            f"&slug=eq.{slug}"
            f"&checkin=eq.{checkin.isoformat()}"
        )
        resp_del = await self._http.delete(delete_url)
        if resp_del.is_error:
            logger.warning(
                "[SCRAPER] Delete old room_prices_raw failed: %s %s",
                resp_del.status_code,
                resp_del.text,
            )

        if not rows:
            return

        # 2) Insert new
        insert_url = f"{self.supabase_url}/rest/v1/room_prices_raw"
        payload = [asdict(r) for r in rows]

        resp_ins = await self._http.post(insert_url, content=json.dumps(payload))
        if resp_ins.is_error:
            logger.error(
                "[SCRAPER] Insert room_prices_raw failed: %s %s",
                resp_ins.status_code,
                resp_ins.text,
            )

    async def close(self) -> None:
        await self._http.aclose()


async def main_demo():
    """
    Tiny demo entry for local testing.
    """
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    user_id = os.environ.get("SCRAPER_USER_ID", "00000000-0000-0000-0000-000000000000")

    core = ScraperCore(supabase_url, supabase_key, user_id=user_id)
    today = dt.date.today()
    # example slug / cc – replace with your real one:
    await core.scrape_hotel_range(
        slug="occidental-ljubljana",
        country_code="si",
        start_date=today,
        days=1,
    )
    await core.close()


if __name__ == "__main__":
    asyncio.run(main_demo())
