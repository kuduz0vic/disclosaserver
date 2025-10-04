# Yield worker

## Run locally

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m playwright install --with-deps chromium

export SUPABASE_URL=...
export SUPABASE_SERVICE_ROLE_KEY=...
export PLAYWRIGHT_BROWSERS_PATH=0
python scrape_worker.py

# Manual test for a single user:

RUN_FOR_USER_ID=<uuid> RUN_DAYS=3 python scrape_worker.py
