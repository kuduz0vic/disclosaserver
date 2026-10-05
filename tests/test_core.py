# Offline unit tests for pure helpers + sold-out vs failure handling.
# Run: python3 -m unittest discover -s tests
import os
import sys
import types
import unittest
from unittest import mock

# Stub env + playwright so modules import without network/browsers.
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
import worker_poll as worker  # noqa: E402


class NormalizeSlug(unittest.TestCase):
    def test_strips_plain_html(self):
        self.assertEqual(core._normalize_slug("my-hotel.html"), "my-hotel")

    def test_strips_language_html(self):
        self.assertEqual(core._normalize_slug("my-hotel.sl.html"), "my-hotel")
        self.assertEqual(core._normalize_slug("my-hotel.en-gb.html"), "my-hotel")

    def test_url(self):
        self.assertEqual(
            core._normalize_slug("https://www.booking.com/hotel/at/marienhof-reichenau.html?aid=1"),
            "marienhof-reichenau",
        )

    def test_bare_slug_untouched(self):
        self.assertEqual(core._normalize_slug("city-hotel"), "city-hotel")


class ParsePrice(unittest.TestCase):
    def test_thousands_separators(self):
        self.assertEqual(core.clean_price("€1,234"), 1234.0)
        self.assertEqual(core.clean_price("€ 1.234,50"), 1234.5)
        self.assertEqual(core.clean_price("€\xa0189"), 189.0)

    def test_no_currency_or_absurd(self):
        self.assertIsNone(core.clean_price("189"))
        self.assertIsNone(core.clean_price("€2"))


class ScrapeOnceFailureVsSoldOut(unittest.TestCase):
    hotel = {"name": "H", "slug": "h", "cc": "si", "scrape_slug": "h__si"}

    def run_once(self, rows, status):
        with mock.patch.object(worker, "is_canceled", return_value=False), mock.patch.object(
            worker, "scrape_hotel_for_dates_with_status", return_value=(rows, status)
        ):
            return worker._scrape_once("job", "u", "p", self.hotel, "2026-10-05", "2026-10-06")

    def test_all_pages_loaded_and_empty_is_sold_out(self):
        r = self.run_once([], {"attempted": 4, "loaded": 4, "errors": [], "no_availability": 4})
        self.assertFalse(r.get("failed"))
        self.assertEqual(r["rows"], [])

    def test_empty_without_no_availability_message_is_failure(self):
        r = self.run_once([], {"attempted": 4, "loaded": 4, "errors": [], "no_availability": 0})
        self.assertTrue(r.get("failed"))

    def test_timeouts_are_failures_not_sold_out(self):
        r = self.run_once([], {"attempted": 4, "loaded": 2, "errors": ["adults=1: timeout"]})
        self.assertTrue(r.get("failed"))

    def test_blocked_is_failure(self):
        r = self.run_once([], {"attempted": 4, "loaded": 0, "errors": ["adults=1: HTTP 403"]})
        self.assertTrue(r.get("failed"))

    def test_exception_is_failure(self):
        with mock.patch.object(worker, "is_canceled", return_value=False), mock.patch.object(
            worker, "scrape_hotel_for_dates_with_status", side_effect=RuntimeError("boom")
        ):
            r = worker._scrape_once("job", "u", "p", self.hotel, "2026-10-05", "2026-10-06")
        self.assertTrue(r.get("failed"))

    def test_rows_with_partial_failures_are_kept(self):
        row = {"hotel": "H", "slug": "h", "checkin": "2026-10-05", "room": "Double",
               "occupancy": 2, "price": 120.0, "rate_key": "b1"}
        r = self.run_once([row], {"attempted": 4, "loaded": 3, "errors": ["adults=4: timeout"]})
        self.assertFalse(r.get("failed"))
        self.assertEqual(len(r["rows"]), 1)
        self.assertEqual(r["rows"][0]["slug"], "h__si")


class SlugFilter(unittest.TestCase):
    def test_cc_keys_match(self):
        hotels = [{"scrape_slug": "a__si"}, {"scrape_slug": "b__at"}]
        out = worker.apply_job_slug_filter({"slugs": ["a__si"]}, hotels)
        self.assertEqual([h["scrape_slug"] for h in out], ["a__si"])


if __name__ == "__main__":
    unittest.main()


class Liveness(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.dir = tempfile.mkdtemp()
        self.patch = mock.patch.object(worker, "LIVENESS_FILE", os.path.join(self.dir, "alive"))
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_missing_file_is_stale(self):
        self.assertEqual(worker.liveness_age_sec(), float("inf"))

    def test_touch_makes_it_fresh(self):
        worker.touch_liveness()
        self.assertLess(worker.liveness_age_sec(), 5)

    def test_old_touch_exceeds_limit(self):
        worker.touch_liveness()
        later = os.path.getmtime(worker.LIVENESS_FILE) + worker.LIVENESS_MAX_AGE_SEC + 1
        self.assertGreater(worker.liveness_age_sec(now=later), worker.LIVENESS_MAX_AGE_SEC)

    def test_watchdog_exits_when_file_missing(self):
        with mock.patch.object(worker.os, "_exit", side_effect=SystemExit(3)) as ex:
            with self.assertRaises(SystemExit):
                worker.check_liveness()
            ex.assert_called_once_with(3)

    def test_watchdog_quiet_when_fresh(self):
        worker.touch_liveness()
        with mock.patch.object(worker.os, "_exit") as ex:
            worker.check_liveness()
            ex.assert_not_called()


class ReplaceSnapshot(unittest.TestCase):
    """A successful check replaces current offers only for guest counts it saw conclusively."""

    hotel = {"name": "H", "slug": "h", "cc": "si", "scrape_slug": "h__si"}
    row2 = {"hotel": "H", "slug": "h", "checkin": "2026-10-05", "room": "Double",
            "occupancy": 2, "price": 120.0, "rate_key": "b1"}

    def run_once(self, rows, status):
        with mock.patch.object(worker, "is_canceled", return_value=False), mock.patch.object(
            worker, "scrape_hotel_for_dates_with_status", return_value=(rows, status)
        ):
            return worker._scrape_once("job", "u", "p", self.hotel, "2026-10-05", "2026-10-06")

    def test_all_pages_with_offers_replace_every_guest_count(self):
        pages = {1: "rows", 2: "rows", 3: "rows", 4: "rows"}
        r = self.run_once([self.row2], {"attempted": 4, "loaded": 4, "errors": [], "pages": pages})
        self.assertEqual(r["replace_occupancies"], [1, 2, 3, 4])

    def test_failed_page_keeps_that_guest_count(self):
        pages = {1: "rows", 2: "rows", 3: "failed", 4: "rows"}
        r = self.run_once([self.row2], {"attempted": 4, "loaded": 3, "errors": ["adults=3: timeout"], "pages": pages})
        self.assertFalse(r.get("failed"))
        self.assertEqual(r["replace_occupancies"], [1, 2, 4])

    def test_explicit_no_availability_replaces_inconclusive_empty_does_not(self):
        pages = {1: "rows", 2: "rows", 3: "no_availability", 4: "empty"}
        r = self.run_once([self.row2], {"attempted": 4, "loaded": 4, "errors": [], "no_availability": 1, "pages": pages})
        self.assertEqual(r["replace_occupancies"], [1, 2, 3])

    def test_old_status_without_pages_replaces_nothing(self):
        r = self.run_once([self.row2], {"attempted": 4, "loaded": 4, "errors": []})
        self.assertEqual(r["replace_occupancies"], [])

    def test_replace_sends_one_scoped_rpc(self):
        rows = [dict(self.row2, slug="other__xx", checkin="2030-01-01", user_id="u", property_id="p")]
        with mock.patch.object(worker, "http_post") as post:
            post.return_value.json.return_value = {"archived": 3, "deleted": 3, "written": 1}
            worker.replace_current_snapshot("u", "p", "H__SI", "2026-10-05", [1, 2], rows)
        post.assert_called_once()
        path, _params, body = post.call_args[0][:3]
        self.assertEqual(path, "/rest/v1/rpc/fn_replace_day_rows")
        self.assertEqual((body["p_user_id"], body["p_property_id"], body["p_slug"], body["p_checkin"]), ("u", "p", "h__si", "2026-10-05"))
        self.assertEqual(body["p_occupancies"], [1, 2])
        self.assertEqual(len(body["p_rows"]), 1)
        # rows can't redirect the write to another hotel/night/tenant
        for k in ("slug", "checkin", "user_id", "property_id"):
            self.assertNotIn(k, body["p_rows"][0])
        self.assertEqual(body["p_rows"][0]["room"], "Double")
        self.assertEqual(body["p_rows"][0]["price"], 120.0)


class StepWrites(unittest.TestCase):
    """What a finished step writes, per outcome (job loop branches)."""

    def _process(self, result):
        calls = []
        names = ["replace_current_snapshot", "upsert_room_prices", "archive_previous_snapshot",
                 "delete_current_snapshot", "ensure_soldout_marker", "insert_soldout_alert", "clear_soldout_marker"]
        patches = [mock.patch.object(worker, n, side_effect=(lambda *a, _n=n, **k: calls.append(_n) or (True if _n == "ensure_soldout_marker" else None))) for n in names]
        for p in patches:
            p.start()
        try:
            worker.write_step_result("u", "p", "job", result)
        finally:
            for p in patches:
                p.stop()
        return calls

    def test_success_replaces_and_clears_marker(self):
        res = {"slug": "h__si", "checkin": "2026-10-05", "rows": [{"room": "Double"}], "replace_occupancies": [2]}
        self.assertEqual(self._process(res), ["replace_current_snapshot", "clear_soldout_marker"])

    def test_failure_writes_nothing(self):
        res = {"slug": "h__si", "checkin": "2026-10-05", "rows": [], "failed": True, "error": "timeout"}
        self.assertEqual(self._process(res), [])

    def test_sold_out_path_unchanged(self):
        res = {"slug": "h__si", "checkin": "2026-10-05", "rows": []}
        self.assertEqual(
            self._process(res),
            ["archive_previous_snapshot", "delete_current_snapshot", "ensure_soldout_marker", "insert_soldout_alert"],
        )
