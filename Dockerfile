# Uses Ubuntu Jammy with browsers preinstalled for Playwright v1.46
FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# (Optional) Your defaults; override via Railway env vars
ENV MAX_WORKERS=4 \
    DEFAULT_CC=si \
    RESOLVE_CC_IF_MISSING=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

CMD ["python", "scrape_worker.py"]
