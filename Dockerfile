# Uses Ubuntu Jammy with Playwright v1.46 and browsers preinstalled
FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

# Faster, cleaner Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Optional defaults (override in Railway env settings)
ENV MAX_WORKERS=4 \
    DEFAULT_CC=si \
    RESOLVE_CC_IF_MISSING=1 \
    NO_SANDBOX=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# 1) Only copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# If you don't need dev headers, this is enough (base image includes runtimes)
RUN pip install --no-cache-dir -r requirements.txt

# 2) Now copy the rest of the app
#    Make sure these files exist in your repo
#      - scraper_core.py
#      - worker_poll.py
#      - (any other modules your code imports)
COPY . .

# (Optional) If your code writes temporary files, create a writable dir
RUN mkdir -p /app/tmp && chown -R pwuser:pwuser /app

# Drop root privileges
USER pwuser

# Healthcheck (optional but recommended). Adjust URL/table/ping if you expose one.
# Here we just check the process keeps running by touching a file every ~30s in your code,
# or you can remove this if you don't use health checks.
# HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
#   CMD python -c "import os,sys; sys.exit(0)"

# Railway run command — worker that polls Supabase for jobs
CMD ["python", "-u", "worker_poll.py"]
