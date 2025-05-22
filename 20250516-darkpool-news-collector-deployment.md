# 2025-05-16 Dark Pool & News Collector Deployment Summary (Updated)

## Overview
This document summarizes the deployment, troubleshooting, and validation process for the dark pool and news collectors on both local and production environments. It is intended as a reference for future maintenance and improvements.

---

## Key Steps & Milestones

### 1. **Preparation & Code Sync**
- All code (including migration scripts and collector updates) is committed and pushed to the `master` branch.
- Both local and production environments are in sync with the latest code.
- Only the dedicated `*-worker.service` and `*-beat.service` files are used for each collector. Monolithic service files have been removed.

### 2. **Celery App Split**
- `celery_app.py` now defines two separate Celery apps:
  - `news_app` for news collector tasks and scheduling.
  - `darkpool_app` for dark pool collector tasks and scheduling.
- Each beat service now only schedules its own collector's tasks, preventing duplicate or cross-scheduling.

### 3. **Systemd Service Configuration**
- **news-collector-worker.service**
  ```
  ExecStart=/bin/bash -c 'export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && /opt/darkpool_collector/venv/bin/celery -A celery_app.news_app worker --loglevel=info -Q news_queue -n news_worker@%h'
  ```
- **news-collector-beat.service**
  ```
  ExecStart=/bin/bash -c 'export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && /opt/darkpool_collector/venv/bin/celery -A celery_app.news_app beat --loglevel=info --schedule=/opt/darkpool_collector/celerybeat-news-schedule.db'
  ```
- **darkpool-collector-worker.service**
  ```
  ExecStart=/bin/bash -c 'export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && /opt/darkpool_collector/venv/bin/celery -A celery_app.darkpool_app worker --loglevel=info -Q darkpool_queue -n darkpool_worker@%h'
  ```
- **darkpool-collector-beat.service**
  ```
  ExecStart=/bin/bash -c 'export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && /opt/darkpool_collector/venv/bin/celery -A celery_app.darkpool_app beat --loglevel=info --schedule=/opt/darkpool_collector/celerybeat-darkpool-schedule.db'
  ```
- After editing, reload systemd and restart all services:
  ```sh
  sudo systemctl daemon-reload
  sudo systemctl restart news-collector-worker.service news-collector-beat.service darkpool-collector-worker.service darkpool-collector-beat.service
  ```

### 4. **Testing & Validation**
- **Manual Task Triggering:**
  - News:  
    ```sh
    export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && ENV_FILE=.env.prod celery -A celery_app.news_app call celery_app.run_news_collector_task
    ```
  - Dark Pool:  
    ```sh
    export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && ENV_FILE=.env.prod celery -A celery_app.darkpool_app call celery_app.run_darkpool_collector_task
    ```
- **Monitor logs:**
  ```sh
  sudo journalctl -u news-collector-worker.service -n 50
  sudo journalctl -u news-collector-beat.service -n 50
  sudo journalctl -u darkpool-collector-worker.service -n 50
  sudo journalctl -u darkpool-collector-beat.service -n 50
  ```
- **Validate data:**  
  Use the validation script to check for new rows in the last 24 hours.

### 5. **Ongoing Monitoring**
- Let the beat services run for at least 10–15 minutes and confirm both collectors are being triggered on schedule.
- Continue to monitor logs and database for expected data.

---

## Issues & Solutions (Updated)
- **Duplicate scheduling:** Resolved by splitting Celery apps and updating service files.
- **Environment variables:** 
  - Resolved by explicitly setting `UW_API_TOKEN` in service files using `/bin/bash -c 'export ... && ...'` syntax.
  - Updated news collector to use main API config from `flow_analysis/config/api_config.py` instead of separate config.
- **Celery node name warnings:** Use unique node names in service files.
- **No new dark pool data:** If the market is closed, this is expected; otherwise, check logs and API.
- **Rate limiting:** Need to implement rate limiting improvements as per `20250519-colector-fixes.md` after market hours.

---

## Current Status (Updated 2025-05-20)
- ✅ Services are running properly with correct configuration
- ✅ Environment variables are properly set in service files
- ✅ News collector updated to use main API config
- ⏳ Waiting for market hours to end before:
  - Validating data collection
  - Implementing rate limiting improvements
  - Making any further changes

---

## Useful Commands (Updated)

**Validate data for last 24h:**
```sh
ENV_FILE=.env.prod python3 scripts/validate_data_last24h.py
```
**Backfill news headlines (last 24h):**
```sh
export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && ENV_FILE=.env.prod python3 -c "from collectors.news_collector import NewsCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); NewsCollector().collect(start_date=start, end_date=end)"
```
**Backfill dark pool trades (last 24h):**
```sh
export UW_API_TOKEN=9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999 && ENV_FILE=.env.prod python3 -c "from collectors.darkpool_collector import DarkPoolCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); DarkPoolCollector().collect_darkpool_trades(start_date=start, end_date=end, incremental=False)"
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

## Next Steps (Updated)
- [ ] Wait for market hours to end
- [ ] Validate data collection after market hours
- [ ] Implement rate limiting improvements from `20250519-colector-fixes.md`
- [ ] Monitor logs and validate data collection
- [ ] Use this document as a reference for future deployments and troubleshooting. 