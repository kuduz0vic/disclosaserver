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


class KnownDefects(unittest.TestCase):
    """Proven from code: _offer_blob_for_variant() adds the room's HEADER row text (= the first offer's cells) to
    every later offer's blob, and _extract_only_for_guest / _extract_max_persons_from_text take the MAXIMUM over all
    matches. So capacity wording of the room's first offer leaks into its other offers. Expected failures until fixed
    (the fix must keep strict per-offer occupancy, not loosen it)."""

    @unittest.expectedFailure
    def test_single_use_first_offer_hides_the_double_offer(self):
        # offer 1: single use ("Only for 1 guest"); offer 2: the normal double for 2 adults
        t = Table(room("Double Room", [("Only for 1 guest € 180", 1, "€ 180"), ("Max persons: 2 € 231", 2, "€ 231")]))
        rows, _ = parse(Page(t), 2)
        self.assertEqual([r["price"] for r in rows], [231.0])

    def test_single_use_leak_is_visible_in_diagnostics(self):
        t = Table(room("Double Room", [("Only for 1 guest € 180", 1, "€ 180"), ("Max persons: 2 € 231", 2, "€ 231")]))
        rows, d = parse(Page(t), 2)
        self.assertEqual(rows, [], "today: the double is lost")
        self.assertEqual(d["cap_src"], {"only_for": 2}, "both offers decided by the header's 'Only for 1 guest'")
        self.assertEqual(d["class"], "capacity_rejected_all")
        rows1, _ = parse(Page(t), 1)
        self.assertEqual(sorted(r["price"] for r in rows1), [180.0, 231.0], "…and the double is stored as a 1-adult rate")

    @unittest.expectedFailure
    def test_header_max_persons_overrides_variant_without_icons(self):
        # icons not matched (selector drift): text decides, and the header's "Max persons: 3" outranks the variant's 2
        t = Table(room("Flexible Room", [("Max persons: 3 € 300", 0, "€ 300"), ("Max persons: 2 € 231", 0, "€ 231")]))
        rows, _ = parse(Page(t), 2)
        self.assertEqual([r["price"] for r in rows], [231.0])


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
