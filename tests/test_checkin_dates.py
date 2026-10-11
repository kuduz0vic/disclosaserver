# Offline tests: the first check-in night is "today" in the property's timezone, not the container's UTC date.
# Run: python3 -m unittest discover -s tests
import datetime as dt
import os
import sys
import types
import unittest
from unittest import mock

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
pw = types.ModuleType("playwright")
pw_sync = types.ModuleType("playwright.sync_api")
pw_sync.sync_playwright = lambda: None
pw_sync.TimeoutError = type("TimeoutError", (Exception,), {})
sys.modules.setdefault("playwright", pw)
sys.modules.setdefault("playwright.sync_api", pw_sync)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import worker_poll as worker  # noqa: E402

UTC = dt.timezone.utc
LJ = "Europe/Ljubljana"


def at(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=UTC)


def container_date(d="2026-10-11"):
    return lambda: dt.date.fromisoformat(d)


class CheckinStartDate(unittest.TestCase):
    def start(self, now, candidates, fallback="2026-10-11"):
        return worker.checkin_start_date(at(now), candidates, fallback_today=container_date(fallback))

    def test_after_local_midnight_before_utc_midnight(self):
        # job 70c26f97: started 23:22 UTC on 11 Oct = 01:22 on 12 Oct in Ljubljana → first night is the 12th
        self.assertEqual(self.start("2026-10-11T23:22:26", [("job.meta.tz", LJ)]), (dt.date(2026, 10, 12), LJ, "job.meta.tz"))

    def test_winter_time_after_local_midnight(self):
        # CET (UTC+1): 23:30 UTC on 10 Nov = 00:30 on 11 Nov
        self.assertEqual(self.start("2026-11-10T23:30:00", [("x", LJ)])[0], dt.date(2026, 11, 11))

    def test_normal_daytime(self):
        self.assertEqual(self.start("2026-10-11T16:40:00", [("x", LJ)])[0], dt.date(2026, 10, 11))

    def test_scheduled_slots_unchanged(self):
        # 06:00 and 18:00 Ljubljana slots: local date == UTC date in summer and winter time, as before
        for now in ("2026-10-12T04:00:00", "2026-10-12T16:00:00", "2026-11-12T05:00:00", "2026-11-12T17:00:00"):
            local, _, _ = self.start(now, [("job.meta.tz", LJ)])
            self.assertEqual(local, at(now).date(), now)

    def test_other_timezones_not_hardcoded(self):
        self.assertEqual(self.start("2026-10-11T23:22:00", [("x", "America/New_York")])[0], dt.date(2026, 10, 11))
        self.assertEqual(self.start("2026-10-11T15:30:00", [("x", "Asia/Tokyo")])[0], dt.date(2026, 10, 12))

    def test_missing_or_invalid_timezone_falls_back_to_the_container_date(self):
        for bad in (None, "", "  ", "Mars/Olympus", "../etc/passwd", 42, "x" * 100):
            self.assertEqual(self.start("2026-10-11T23:22:00", [("job.meta.tz", bad)]), (dt.date(2026, 10, 11), None, "container_date"), repr(bad))

    def test_first_valid_candidate_wins_and_later_lookups_are_lazy(self):
        called = []

        def lookup():
            called.append(1)
            return "Asia/Tokyo"

        self.assertEqual(self.start("2026-10-11T23:22:00", [("job.meta.tz", LJ), ("property", lookup)])[1], LJ)
        self.assertEqual(called, [], "scheduled jobs (meta.tz) need no extra lookups")
        self.assertEqual(self.start("2026-10-11T23:22:00", [("job.meta.tz", None), ("property", lookup)])[1:], ("Asia/Tokyo", "property"))

    def test_failing_lookup_is_skipped(self):
        def boom():
            raise RuntimeError("network")

        self.assertEqual(self.start("2026-10-11T23:22:00", [("property", boom), ("prefs", LJ)])[1:], (LJ, "prefs"))

    def test_no_zone_data_in_the_image_keeps_old_behaviour(self):
        with mock.patch.object(worker, "ZoneInfo", side_effect=worker.ZoneInfoNotFoundError("no tzdata")):
            self.assertEqual(self.start("2026-10-11T23:22:00", [("job.meta.tz", LJ)]), (dt.date(2026, 10, 11), None, "container_date"))


class JobCheckinStart(unittest.TestCase):
    now = at("2026-10-11T23:22:26")

    def test_scheduled_job_uses_meta_tz_without_lookups(self):
        with mock.patch.object(worker, "http_get", side_effect=AssertionError("no lookup expected")):
            d, tz, src = worker.job_checkin_start({"meta": {"tz": LJ, "source": "auto"}}, "u", "p", self.now)
        self.assertEqual((d, tz, src), (dt.date(2026, 10, 12), LJ, "job.meta.tz"))

    def test_manual_job_reads_the_property_timezone(self):
        resp = mock.Mock()
        resp.json.return_value = [{"timezone": LJ}]
        with mock.patch.object(worker, "http_get", return_value=resp) as g:
            d, tz, src = worker.job_checkin_start({"meta": {"source": "manual", "slugs": None}}, "u", "p", self.now)
        self.assertEqual((d, tz, src), (dt.date(2026, 10, 12), LJ, "property_hotel_settings"))
        path, params = g.call_args[0]
        self.assertEqual((path, params["property_id"]), ("/rest/v1/property_hotel_settings", "eq.p"))

    def test_manual_job_falls_back_to_autoscrape_prefs(self):
        empty, prefs = mock.Mock(), mock.Mock()
        empty.json.return_value = []
        prefs.json.return_value = [{"timezone": LJ}]
        with mock.patch.object(worker, "http_get", side_effect=[empty, prefs]) as g:
            _, tz, src = worker.job_checkin_start({"meta": {"source": "manual"}}, "u", "p", self.now)
        self.assertEqual((tz, src), (LJ, "autoscrape_prefs"))
        self.assertEqual(g.call_args[0][1]["user_id"], "eq.u")

    def test_nothing_known_keeps_the_container_date(self):
        empty = mock.Mock()
        empty.json.return_value = []
        with mock.patch.object(worker, "http_get", return_value=empty), mock.patch.object(worker, "WORKER_FALLBACK_TZ", None):
            _, tz, src = worker.job_checkin_start({"meta": None}, "u", "p", self.now)
        self.assertEqual((tz, src), (None, "container_date"))

    def test_env_fallback_timezone(self):
        empty = mock.Mock()
        empty.json.return_value = []
        with mock.patch.object(worker, "http_get", return_value=empty), mock.patch.object(worker, "WORKER_FALLBACK_TZ", LJ):
            self.assertEqual(worker.job_checkin_start({}, "u", "p", self.now)[1:], (LJ, "WORKER_FALLBACK_TZ"))


if __name__ == "__main__":
    unittest.main()
