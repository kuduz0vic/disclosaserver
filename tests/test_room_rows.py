# Offline tests for room-row parsing and per-page diagnostics (scraper_core.collect_room_rows_for_adults,
# classify_page). Booking.com pages are represented by small fake element handles that answer exactly the calls the
# parser makes (query_selector[_all], inner_text, evaluate, title) — no browser, no network.
# Run: python3 -m unittest discover -s tests
import io
import json
import os
import sys
import types
import unittest
from contextlib import redirect_stdout

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
pw = types.ModuleType("playwright")
pw_sync = types.ModuleType("playwright.sync_api")
pw_sync.sync_playwright = lambda: None
pw_sync.TimeoutError = type("TimeoutError", (Exception,), {})
sys.modules.setdefault("playwright", pw)
sys.modules.setdefault("playwright.sync_api", pw_sync)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scraper_core as core  # noqa: E402

PRICE_SEL = core.PRICE_SELECTORS[0]
NAME_SEL = ".hprt-roomtype-icon-link"


class Price:
    def __init__(self, text):
        self.text = text

    def inner_text(self):
        return self.text

    def evaluate(self, js):
        # extract_effective_price's candidate scan
        return [{"t": self.text, "strike": False}]


class Name:
    def __init__(self, text):
        self.text = text

    def inner_text(self):
        return self.text


class Row:
    """One <tr> of the hprt room table: optional room name (header row), its text, person icons and price."""

    def __init__(self, text, name=None, icons=0, price=None):
        self.text = text
        self.name = name
        self.icons = icons
        self.price = price

    def query_selector(self, sel):
        return Name(self.name) if self.name and sel == NAME_SEL else None

    def query_selector_all(self, sel):
        return [Price(self.price)] if self.price and sel == PRICE_SEL else []

    def inner_text(self):
        return self.text

    def evaluate(self, js):
        if "textContent" in js and "querySelectorAll" not in js:
            return self.text
        if "bicon-occupancy" in js:  # _count_person_icons_in_node
            return self.icons or None
        raise AssertionError("unexpected evaluate")


class Table:
    def __init__(self, rows):
        self.rows = rows

    def query_selector_all(self, sel):
        assert sel == "tr"
        return self.rows


class Page:
    def __init__(self, table=None, table_sel="#hprt-table", body="", title="Hotel Nox, Ljubljana – Booking.com"):
        self.table = table
        self.table_sel = table_sel
        self.body = body
        self._title = title

    def query_selector(self, sel):
        return self.table if self.table is not None and sel == self.table_sel else None

    def query_selector_all(self, sel):
        return []  # no card-layout candidates

    def inner_text(self, sel="body", timeout=None):
        return self.body

    def title(self):
        return self._title


def room(name, offers):
    """offers: [(text, icons, price)] → header row (with the room name) + variant rows, like Booking's rowspan table."""
    rows = []
    for i, (text, icons, price) in enumerate(offers):
        rows.append(Row(f"{name} {text}" if i == 0 else text, name=name if i == 0 else None, icons=icons, price=price))
    return rows


def parse(page, adults):
    d = core.new_page_diag()
    rows = core.collect_room_rows_for_adults(page, adults, d)
    if not rows:
        rows = core.collect_card_rows_for_adults(page, adults, d)
    no_avail = not rows and core.page_says_no_availability(page)
    d["class"] = core.classify_page(d, len(rows), no_avail)
    return rows, d


# Current known markup: per-offer person icons + "Max persons: N" screen-reader text in the occupancy cell.
KNOWN = Table(
    room("Double Room", [("Max persons: 2 € 231 Free cancellation", 2, "€ 231"), ("Max persons: 2 € 245 Breakfast included", 2, "€ 245")])
    + room("Triple Room", [("Max persons: 3 € 300", 3, "€ 300")])
)


class KnownMarkup(unittest.TestCase):
    def test_rows_for_two_adults(self):
        rows, d = parse(Page(KNOWN), 2)
        self.assertEqual(sorted(r["price"] for r in rows), [231.0, 245.0])
        self.assertTrue(all(r["occupancy"] == 2 for r in rows))
        self.assertEqual(d["class"], "rows_parsed")
        self.assertEqual((d["rows_seen"], d["offer_rows"], d["capacity_parsed"], d["capacity_match"]), (3, 3, 3, 2))
        self.assertEqual(d["caps"], {"2": 2, "3": 1})
        self.assertEqual(d["cap_src"], {"icons_variant": 3})
        self.assertEqual(d["table_selector"], "#hprt-table")

    def test_three_adults_gets_the_triple_only(self):
        rows, d = parse(Page(KNOWN), 3)
        self.assertEqual([r["price"] for r in rows], [300.0])

    def test_diag_does_not_change_results(self):
        self.assertEqual(core.collect_room_rows_for_adults(Page(KNOWN), 2), parse(Page(KNOWN), 2)[0])


class ChangedCapacityMarkup(unittest.TestCase):
    def test_capacity_unknown_when_icons_and_wording_change(self):
        # icons no longer matched by our selectors, occupancy text reworded ("Guests: 2" instead of "Max persons: 2")
        t = Table(room("Double Room", [("Guests: 2 € 231 Free cancellation", 0, "€ 231"), ("Guests: 2 € 245", 0, "€ 245")]))
        rows, d = parse(Page(t), 2)
        self.assertEqual(rows, [])
        self.assertEqual(d["class"], "capacity_unknown")
        self.assertEqual((d["offer_rows"], d["capacity_parsed"], d["capacity_match"]), (2, 0, 0))
        self.assertEqual(d["cap_src"], {"none": 2})
        self.assertIn("Guests: 2", d["samples"][0]["text"], "a short capacity sample shows what the page says now")

    def test_table_present_but_every_row_filtered(self):
        t = Table(room("Family Room", [("Max persons: 4 € 400", 4, "€ 400"), ("Max persons: 4 € 420", 4, "€ 420")]))
        rows, d = parse(Page(t), 2)
        self.assertEqual(rows, [])
        self.assertEqual(d["class"], "capacity_rejected_all")
        self.assertEqual(d["caps"], {"4": 2})

    def test_table_without_room_rows(self):
        rows, d = parse(Page(Table([Row("Select rooms"), Row("Your choices")])), 2)
        self.assertEqual(d["class"], "table_zero_rows")
        self.assertEqual((d["rows_seen"], d["offer_rows"]), (2, 0))

    def test_price_parse_failed(self):
        t = Table(room("Double Room", [("Max persons: 2 price on request", 2, None)]))
        rows, d = parse(Page(t), 2)
        self.assertEqual(d["class"], "price_parse_failed")
        self.assertEqual((d["capacity_match"], d["price_failed"]), (1, 1))


class NoTableAndSoldOut(unittest.TestCase):
    def test_explicit_sold_out(self):
        rows, d = parse(Page(None, body="We have no availability here between Mon 13 Oct and Tue 14 Oct"), 2)
        self.assertEqual(d["class"], "no_availability_message")
        self.assertIsNone(d["table_selector"])

    def test_no_table_no_message(self):
        rows, d = parse(Page(None, body="Hotel Nox Ljubljana Overview Facilities House rules"), 2)
        self.assertEqual(d["class"], "table_not_found")

    def test_classes_are_exhaustive_and_ordered(self):
        d = core.new_page_diag()
        self.assertEqual(core.classify_page(d, 3, True), "rows_parsed", "rows win over a message")
        self.assertEqual(core.classify_page(d, 0, True), "no_availability_message")


class OfferScopedCapacity(unittest.TestCase):
    """Regression: capacity comes from the offer's own row. It used to include the room's header row (= the first
    offer's cells), and "Only for X" / "Max persons" take the maximum match, so the first offer's capacity wording
    decided the room's other offers (proven by these tests failing before the fix)."""

    SINGLE_USE_FIRST = Table(room("Double Room", [("Only for 1 guest € 180", 1, "€ 180"), ("Max persons: 2 € 231", 2, "€ 231")]))

    def test_single_use_first_offer_no_longer_hides_the_double_offer(self):
        rows, d = parse(Page(self.SINGLE_USE_FIRST), 2)
        self.assertEqual([r["price"] for r in rows], [231.0])
        self.assertEqual(d["cap_src"], {"only_for": 1, "icons_variant": 1})
        self.assertEqual(d["class"], "rows_parsed")

    def test_single_use_offer_is_still_a_one_adult_rate_and_the_double_is_not(self):
        rows, _ = parse(Page(self.SINGLE_USE_FIRST), 1)
        self.assertEqual([r["price"] for r in rows], [180.0], "exact occupancy: the double is not a 1-adult rate")

    def test_header_max_persons_no_longer_overrides_the_offer_row(self):
        # icons not matched: text decides; the header's "Max persons: 3" must not outrank the offer's own 2
        t = Table(room("Flexible Room", [("Max persons: 3 € 300", 0, "€ 300"), ("Max persons: 2 € 231", 0, "€ 231")]))
        self.assertEqual([r["price"] for r in parse(Page(t), 2)[0]], [231.0])
        self.assertEqual([r["price"] for r in parse(Page(t), 3)[0]], [300.0])

    def test_offer_row_without_any_capacity_signal_still_falls_back_to_the_header(self):
        # occupancy cell spanning several rows: the later offer row has no capacity text or icons of its own
        t = Table(room("Double Room", [("Max persons: 2 € 231", 2, "€ 231"), ("€ 245 Breakfast included", 0, "€ 245")]))
        rows, d = parse(Page(t), 2)
        self.assertEqual(sorted(r["price"] for r in rows), [231.0, 245.0])
        self.assertEqual(d["cap_src"], {"icons_variant": 1, "icons_header": 1})

    def test_offer_row_own_only_for_still_wins(self):
        t = Table(room("Double Room", [("Max persons: 2 € 231", 2, "€ 231"), ("Only for 1 guest € 180", 1, "€ 180")]))
        self.assertEqual([r["price"] for r in parse(Page(t), 1)[0]], [180.0])
        self.assertEqual([r["price"] for r in parse(Page(t), 2)[0]], [231.0])

    def test_duplicate_hidden_blocks_in_the_same_row_still_take_the_max(self):
        t = Table(room("Double Room", [("Max persons: 1 Max persons: 2 € 231", 0, "€ 231")]))
        self.assertEqual([r["price"] for r in parse(Page(t), 2)[0]], [231.0])

    def test_meal_plan_still_reads_the_header_text(self):
        t = Table(room("Double Room", [("Max persons: 2 Breakfast included € 231", 2, "€ 231"), ("Max persons: 2 € 245", 2, "€ 245")]))
        rows, _ = parse(Page(t), 2)
        self.assertTrue(all(r["breakfast_included"] for r in rows), "unchanged: the meal blob includes the header")

    def test_all_guest_counts_on_a_mixed_room(self):
        t = Table(
            room("Double Room", [("Only for 1 guest € 180", 1, "€ 180"), ("Max persons: 2 € 231", 2, "€ 231")])
            + room("Triple Room", [("Max persons: 3 € 300", 3, "€ 300")])
            + room("Family Room", [("Max persons: 4 € 400", 4, "€ 400")])
        )
        got = {a: sorted(r["price"] for r in parse(Page(t), a)[0]) for a in (1, 2, 3, 4)}
        self.assertEqual(got, {1: [180.0], 2: [231.0], 3: [300.0], 4: [400.0]})


class Card:
    """A card-layout room block (no hprt table): text, optional room name, icons and price."""

    def __init__(self, text, name="Room", icons=0, price=None):
        self.text, self.name, self.icons, self.price = text, name, icons, price

    def inner_text(self):
        return self.text

    def query_selector(self, sel):
        return Name(self.name) if self.name and sel == "[data-testid='room-name']" else None

    def query_selector_all(self, sel):
        return [Price(self.price)] if self.price and sel == "[data-testid='price-and-discounted-price']" else []

    def evaluate(self, js):
        if "bicon-occupancy" in js:
            return self.icons or None
        raise AssertionError("unexpected evaluate")


class CardPage(Page):
    def __init__(self, cards, **kw):
        super().__init__(None, **kw)
        self.cards = cards

    def query_selector_all(self, sel):
        return self.cards if sel == "[data-testid='property-card']" else []


# Offer-row wording seen in production on 11 Oct 2026 (🧪 PAGE samples, job f3c63824), shortened.
G2 = "Includes taxes and charges Guests: 2 adults Superb breakfast included Free cancellation € 231"
G1 = "Includes taxes and charges Guests: 1 adult Superb breakfast included Free cancellation € 180"
G3 = "Includes taxes and charges 10% off Guests: 3 adults Superb breakfast included € 300"
S1 = "Double Room with Balcony Sleeps: 1 adult 1 large double bed 35 m² Balcony € 180"


class ProductionWording(unittest.TestCase):
    def test_extractors(self):
        self.assertEqual(core._extract_guests_adults("Guests: 2 adults"), 2)
        self.assertEqual(core._extract_guests_adults("Guests: 1 adult"), 1)
        self.assertEqual(core._extract_guests_adults("guests:3 adults"), 3)
        self.assertEqual(core._extract_sleeps_capacity("Sleeps: 1 adult 1 large double bed"), 1)
        self.assertEqual(core._extract_sleeps_capacity("Sleeps: 2 adults"), 2)
        self.assertEqual(core._extract_sleeps_capacity("Sleeps 1 - 2 adults"), 2, "range: upper bound, as before")

    def test_not_broadened(self):
        self.assertIsNone(core._extract_guests_adults("Guests: 2"), "no 'adults': unknown, as before")
        self.assertIsNone(core._extract_guests_adults("Guests: 2 adults, 1 child"), "children named: not an N-adult offer")
        self.assertIsNone(core._extract_guests_adults("Guests: 2 adults + 1 child"))
        self.assertIsNone(core._extract_guests_adults("Guests: 1 adult … Guests: 2 adults"), "two different numbers: ambiguous")
        self.assertIsNone(core._extract_guests_adults("Recommended for 3 adults"), "not the 'Guests:' label")
        self.assertIsNone(core._extract_sleeps_capacity("1 large double bed 35 m²"))

    def test_single_form_sleeps_now_parses(self):
        # latent bug: the single form's number is regex group 3, but group 1 was read → always None
        self.assertEqual(core._extract_sleeps_capacity("Sleeps: 2 guests"), 2)
        self.assertEqual(core._extract_sleeps_capacity("Sleeps: 3 people"), 3)

    def test_guests_rows_exact_occupancy(self):
        t = Table(room("Double Room", [(G2, 0, "€ 231"), (G1, 0, "€ 180")]) + room("Triple Room", [(G3, 0, "€ 300")]))
        for adults, want in ((1, [180.0]), (2, [231.0]), (3, [300.0]), (4, [])):
            rows, d = parse(Page(t), adults)
            self.assertEqual(sorted(r["price"] for r in rows), want, adults)
            self.assertEqual(d["cap_src"], {"guests": 3})
        self.assertEqual(parse(Page(t), 4)[1]["class"], "capacity_rejected_all")

    def test_header_guests_does_not_leak(self):
        # header offer "Guests: 1 adult", later offer "Guests: 2 adults": each is read from its own row
        t = Table(room("Double Room", [(G1, 0, "€ 180"), (G2, 0, "€ 231")]))
        self.assertEqual([r["price"] for r in parse(Page(t), 2)[0]], [231.0])
        self.assertEqual([r["price"] for r in parse(Page(t), 1)[0]], [180.0])

    def test_sleeps_adult_row(self):
        t = Table(room("Double Room with Balcony", [(S1, 0, "€ 180")]))
        self.assertEqual([r["price"] for r in parse(Page(t), 1)[0]], [180.0])
        rows, d = parse(Page(t), 2)
        self.assertEqual((rows, d["class"], d["cap_src"]), ([], "capacity_rejected_all", {"sleeps": 1}))

    def test_older_icon_markup_unchanged(self):
        rows, d = parse(Page(KNOWN), 2)
        self.assertEqual(sorted(r["price"] for r in rows), [231.0, 245.0])
        self.assertEqual(d["cap_src"], {"icons_variant": 3})


class CardLayoutDiagnostics(unittest.TestCase):
    def test_card_capacity_unknown(self):
        p = CardPage([Card("Deluxe Double 30 m² € 231", price="€ 231"), Card("Twin Room 22 m² € 200", price="€ 200")])
        rows, d = parse(p, 2)
        self.assertEqual((rows, d["class"]), ([], "capacity_unknown"))
        self.assertEqual((d["cards_seen"], d["card_capacity_unknown"], d["card_capacity_parsed"]), (2, 2, 0))
        self.assertEqual(d["samples"][0]["src"], "card")

    def test_card_capacity_parsed_but_rejected(self):
        p = CardPage([Card("Family Room Guests: 4 adults € 400", price="€ 400")])
        rows, d = parse(p, 2)
        self.assertEqual((rows, d["class"], d["card_caps"]), ([], "capacity_rejected_all", {"4": 1}))

    def test_card_rows_with_new_wording(self):
        p = CardPage([Card("Double Room " + G2, price="€ 231"), Card("Single Room " + G1, price="€ 180")])
        rows, d = parse(p, 2)
        self.assertEqual(([r["price"] for r in rows], d["class"], d["card_capacity_match"]), ([231.0], "rows_parsed", 1))

    def test_card_price_parse_failed(self):
        p = CardPage([Card("Double Room Guests: 2 adults price on request", price=None)])
        self.assertEqual(parse(p, 2)[1]["class"], "price_parse_failed")


class LogLine(unittest.TestCase):
    def test_one_structured_line_without_page_content(self):
        t = Table(room("Double Room", [("Guests: 2 € 231 " + "x" * 500, 0, "€ 231")]))
        _, d = parse(Page(t), 2)
        buf = io.StringIO()
        with redirect_stdout(buf):
            core.log_page_diag("nox", "si", "2026-10-12", 2, d, "Hotel Nox, Ljubljana – Booking.com")
        out = buf.getvalue().strip().splitlines()
        self.assertEqual(len(out), 1)
        self.assertTrue(out[0].startswith("🧪 PAGE "))
        rec = json.loads(out[0][len("🧪 PAGE "):])
        self.assertEqual((rec["slug"], rec["adults"], rec["class"], rec["rows_seen"], rec["capacity_match"]), ("nox__si", 2, "capacity_unknown", 1, 0))
        self.assertLessEqual(max(len(s["text"]) for s in rec["samples"]), 80)
        self.assertNotIn("x" * 100, out[0])

    def test_sample_is_short_and_plain(self):
        s = core.capacity_text_sample('<span class="a">Max persons: 2</span> <script>alert(1)</script> € 231')
        self.assertLessEqual(len(s), 80)
        self.assertNotIn("<", s)
        self.assertIn("Max persons: 2", s.replace("span classa", "").replace("/span", ""))
        self.assertEqual(core.capacity_text_sample("€ 231 Free cancellation"), "", "no capacity wording at all")


if __name__ == "__main__":
    unittest.main()
