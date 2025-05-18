# 2025-05-16 Dark Pool & News Collector Deployment Summary

## Overview
This document summarizes the deployment, troubleshooting, and validation process for the dark pool and news collectors on both local and production environments. It is intended as a reference for future maintenance and improvements.

---

## Key Steps & Milestones

### 1. **Preparation & Code Sync**
- Ensured all code (including migration scripts and collector updates) was committed and pushed to the `master` branch.
- Switched production from the obsolete `main` branch to `master` and deleted `main` locally and remotely to avoid confusion.
- Pulled the latest code on the production server and confirmed the working tree was clean.
- **Cleaned up systemd service files:** Only the dedicated `*-worker.service` and `*-beat.service` files are used for each collector. Monolithic service files (`darkpool-collector.service`, `news-collector.service`, `collectors.service`, `collectors-beat.service`) have been removed for clarity and best practice.

### 2. **Database Migration**
- Created and ran a migration script to alter the `sentiment` column in `trading.news_headlines` from `double precision` to `text` to match API data types.
- Confirmed successful migration on both local and production databases.

### 3. **Systemd Service Configuration**
- For each collector (news and dark pool), only two services are used:
  - `*-worker.service` (Celery worker)
  - `*-beat.service` (Celery beat/scheduler)
- Each service uses the correct user, group, working directory, environment file, and venv path for Celery.
- Unique Celery node names are assigned (using `-n news_collector@%h` and `-n darkpool_collector@%h`) to avoid duplicate nodename warnings.
- Reloaded systemd and restarted both services.
- Verified both collectors were running and ready via `systemctl status` and `journalctl` logs.

### 4. **Environment Variable Troubleshooting**
- Ensured `UW_API_TOKEN` and other secrets were present in `.env.prod`.
- Used `export ENV_FILE=.env.prod` and/or `set -a; source .env.prod; set +a` to guarantee environment variables were available to Python subprocesses.
- Confirmed environment loading with a test Python one-liner.

### 5. **Collector Backfill & Validation**
- Ran both collectors in backfill mode for the last 24 hours using the correct environment loading method.
- Used a validation script to check the number of rows inserted in the last 24 hours for both `news_headlines` and `darkpool_trades`.
- Confirmed news headlines were being collected and inserted; dark pool trades required further troubleshooting (no new rows in last 24h).

### 6. **Export & Analysis**
- Used the export script to generate CSVs of the last 24 hours of data for both tables.
- Confirmed the workflow for local and production export and analysis.

---

## Issues & Solutions
- **Environment variables not set:** Fixed by exporting or sourcing `.env.prod` before running scripts.
- **Collector method signature mismatch:** Ensured production code was up to date and supported date range arguments.
- **Celery duplicate nodename warning:** Resolved by assigning unique node names in systemd service files.
- **No new dark pool data:** Validated with logs and DB queries; further troubleshooting required if expected data is missing.
- **Service file confusion:** Resolved by removing monolithic service files and using only the dedicated worker and beat service files for each collector.

---

## Useful Commands

**Validate data for last 24h:**
```sh
ENV_FILE=.env.prod python3 scripts/validate_data_last24h.py
```

**Backfill news headlines (last 24h):**
```sh
set -a; source .env.prod; set +a
python3 -c "from collectors.news_collector import NewsCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); NewsCollector().collect(start_date=start, end_date=end)"
```

**Backfill dark pool trades (last 24h):**
```sh
set -a; source .env.prod; set +a
python3 -c "from collectors.darkpool_collector import DarkPoolCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); DarkPoolCollector().collect_darkpool_trades(start_date=start, end_date=end, incremental=False)"
```

**Export last 24h to CSV:**
```sh
ENV_FILE=.env.prod python3 scripts/export_last24h.py
```

**Monitor logs:**
```sh
journalctl -u news-collector-worker.service -f --no-pager
journalctl -u news-collector-beat.service -f --no-pager
journalctl -u darkpool-collector-worker.service -f --no-pager
journalctl -u darkpool-collector-beat.service -f --no-pager
```

---

## Next Steps
- Continue monitoring collectors and database for expected data.
- Troubleshoot dark pool collector if no new data is being inserted.
- Use this document as a reference for future deployments and troubleshooting. 