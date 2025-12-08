import asyncio
import datetime as dt
import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from scraper_core import ScraperCore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class WorkerPoll:
    """
    Polls Supabase scrape_jobs and runs ScraperCore for queued jobs.

    Expected table scrape_jobs fields:
      - id (uuid)
      - user_id (uuid)
      - slug (text)
      - country_code (text)
      - start_date (date)
      - days (int)
      - scope ("day" | "range")
      - status ("queued" | "running" | "done" | "error")
      - error_message (text)
      - created_at, updated_at
    """

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        *,
        poll_interval: float = 5.0,
        max_concurrent_jobs: int = 2,
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.poll_interval = poll_interval
        self.max_concurrent_jobs = max_concurrent_jobs

        self._http = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
                "Content-Type": "application/json",
            },
        )

    # ─────────────────────────────────────────
    # Main loop
    # ─────────────────────────────────────────

    async def run_forever(self) -> None:
        """
        Poll loop.
        """
        logger.info(
            "[WORKER] Starting poll loop (interval=%ss, max_concurrent=%s)",
            self.poll_interval,
            self.max_concurrent_jobs,
        )

        sem = asyncio.Semaphore(self.max_concurrent_jobs)
        tasks: List[asyncio.Task] = []

        try:
            while True:
                # Clean up finished tasks
                tasks = [t for t in tasks if not t.done()]

                # If capacity left, fetch jobs
                if len(tasks) < self.max_concurrent_jobs:
                    jobs = await self._fetch_queued_jobs(
                        limit=self.max_concurrent_jobs - len(tasks)
                    )
                    for job in jobs:
                        logger.info("[WORKER] Picked job id=%s", job["id"])
                        t = asyncio.create_task(self._run_job_with_sem(job, sem))
                        tasks.append(t)

                await asyncio.sleep(self.poll_interval)
        finally:
            for t in tasks:
                with contextlib.suppress(Exception):
                    t.cancel()
            await self._http.aclose()

    async def _run_job_with_sem(self, job: Dict[str, Any], sem: asyncio.Semaphore):
        async with sem:
            await self._run_job(job)

    # ─────────────────────────────────────────
    # Job fetching / status updates
    # ─────────────────────────────────────────

    async def _fetch_queued_jobs(self, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Get jobs with status='queued', ordered by created_at.
        """
        url = (
            f"{self.supabase_url}/rest/v1/scrape_jobs"
            "?status=eq.queued"
            "&order=created_at.asc"
            f"&limit={limit}"
        )
        resp = await self._http.get(url)
        if resp.is_error:
            logger.error(
                "[WORKER] fetch_queued_jobs failed: %s %s",
                resp.status_code,
                resp.text,
            )
            return []
        return resp.json()

    async def _update_job_status(
        self,
        job_id: str,
        status: str,
        *,
        error_message: Optional[str] = None,
    ) -> None:
        url = f"{self.supabase_url}/rest/v1/scrape_jobs?id=eq.{job_id}"
        payload: Dict[str, Any] = {
            "status": status,
            "updated_at": dt.datetime.utcnow().isoformat(),
        }
        if error_message:
            payload["error_message"] = error_message

        resp = await self._http.patch(url, content=json.dumps(payload))
        if resp.is_error:
            logger.error(
                "[WORKER] update_job_status failed: %s %s",
                resp.status_code,
                resp.text,
            )

    # ─────────────────────────────────────────
    # Job execution
    # ─────────────────────────────────────────

    async def _run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["id"]
        user_id = job["user_id"]
        slug = job["slug"]
        country_code = job.get("country_code") or "si"
        scope = job.get("scope") or "range"

        start_date_str = job.get("start_date")
        days = job.get("days") or 1

        if not start_date_str:
            await self._update_job_status(
                job_id, "error", error_message="Missing start_date"
            )
            return

        start_date = dt.date.fromisoformat(start_date_str)

        await self._update_job_status(job_id, "running")

        core = ScraperCore(
            supabase_url=self.supabase_url,
            supabase_key=self.supabase_key,
            user_id=user_id,
        )

        try:
            if scope == "day":
                await core.scrape_single_day(
                    slug=slug,
                    country_code=country_code,
                    checkin=start_date,
                )
            else:
                await core.scrape_hotel_range(
                    slug=slug,
                    country_code=country_code,
                    start_date=start_date,
                    days=days,
                )

            await self._update_job_status(job_id, "done")
        except Exception as e:
            logger.exception("[WORKER] Job %s failed: %s", job_id, e)
            await self._update_job_status(job_id, "error", error_message=str(e))
        finally:
            await core.close()


async def main():
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    poll_interval = float(os.environ.get("SCRAPER_POLL_INTERVAL", "5"))
    max_concurrent = int(os.environ.get("SCRAPER_MAX_CONCURRENT", "2"))

    worker = WorkerPoll(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        poll_interval=poll_interval,
        max_concurrent_jobs=max_concurrent,
    )

    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
