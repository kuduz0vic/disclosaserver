FROM python:3.11-slim

# System deps for Chromium/Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates fonts-liberation libasound2 libatk1.0-0 \
    libatk-bridge2.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libdrm2 \
    libexpat1 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 \
    libpango-1.0-0 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 \
    libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
    libxshmfence1 libxss1 libxtst6 libu2f-udev xvfb curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium for Playwright (with OS deps)
RUN python -m playwright install --with-deps chromium

COPY . .

# Sensible defaults (override in Railway vars)
ENV MAX_WORKERS=4 \
    DEFAULT_CC=si \
    RESOLVE_CC_IF_MISSING=1 \
    PLAYWRIGHT_BROWSERS_PATH=0

CMD ["python", "scrape_worker.py"]
