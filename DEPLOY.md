# Production deployment (Hetzner)

The worker runs on a Hetzner VM under Docker Compose (this repo's `docker-compose.yml`).
Current deployment: 2026-10-05.

## Where it runs

| | |
| --- | --- |
| Host | `rativa-server` (SSH alias `rativamac`), Ubuntu 24.04 |
| Process manager | Docker Compose, project `disclosaserver`, 3 replicas of service `worker` |
| Directory | `/home/rativa/rativa-worker/disclosaserver` (owner `rativa`) |
| Config | `.env` in that directory (names: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `MAX_WORKERS`, `DEFAULT_CC`, `RESOLVE_CC_IF_MISSING`, `NO_SANDBOX`, `HEADLESS`, `PLAYWRIGHT_BROWSERS_PATH`, `SCROLL_PASSES`, `WAIT_TABLE_TIMEOUT_MS`, `PAGE_GOTO_TIMEOUT_MS`) — unchanged by this deploy |
| Boot | `docker` + `containerd` enabled; `restart: unless-stopped` |

## What changed on 2026-10-05

- `scraper_core.py`, `worker_poll.py`: patched worker (failures ≠ sold out, explicit "no availability"
  detection, partial-failure job status, per-step upserts, tolerant cancel check) plus a **liveness
  watchdog**: the main loop touches `/tmp/worker-alive`; a watchdog thread exits the process (code 3)
  when it is older than `LIVENESS_MAX_AGE_SEC` (default 900), so the restart policy brings it back.
  This covers the Jul 26 → Oct 5 outage, where all three workers hung without exiting.
- `tests/test_core.py` (18 offline tests; run inside the image, see below).
- `docker-compose.yml` (now tracked in git):
  - `healthcheck` — healthy while `/tmp/worker-alive` is < 180 s old (status only; Docker does not
    restart unhealthy containers — the in-process watchdog does that);
  - `logging` — `json-file`, `max-size: 10m`, `max-file: 5` (previously unbounded).

## Backups / rollback

- Files: `/home/rativa/rativa-worker/backups/20261005T162815Z/` (`disclosaserver.tgz` without `.env`,
  `docker-compose.yml.orig`).
- Image: `disclosaserver-worker:pre-20261005T162815Z`.
- Roll back: restore the tarball's `scraper_core.py`, `worker_poll.py`, `docker-compose.yml`, then
  `docker tag disclosaserver-worker:pre-20261005T162815Z disclosaserver-worker:latest &&
  docker compose up -d --force-recreate --no-build --scale worker=3`.

## Operating

```sh
cd /home/rativa/rativa-worker/disclosaserver
docker compose ps                                   # all three "(healthy)"
docker compose logs -f --tail 50                    # live logs
docker compose build && docker run --rm --entrypoint python disclosaserver-worker:latest -m unittest discover -s tests
docker compose up -d --force-recreate --scale worker=3   # deploy (keep --scale: replicas are not in the file)
```

Known: `docker compose stop` ends with exit 137 after 10 s — the worker runs as PID 1 without a
SIGTERM handler. A job interrupted that way stays `running` until the stale-job reaper fails it
(90 min without heartbeat).
