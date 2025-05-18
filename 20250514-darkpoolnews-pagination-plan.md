# Dark Pool & News Collector Pagination/Backfill Plan (2025-05-14)

## Dark Pool Collector: Implementation Plan

### 1. Incremental Collection (Every 5 Minutes)
- **Goal:** On each run, fetch only new trades since the last run for each ticker.
- **How:**
  1. For each ticker you track (e.g., SPY, QQQ):
      - Query your DB for the latest `executed_at` (or `tracking_id`) for that ticker.
      - Use the `/api/darkpool/{ticker}` endpoint with:
        - `date` = today's date (or the trading date you want)
        - `newer_than` = latest `executed_at` (or ISO date/time)
        - `limit` = 500 (max allowed)
      - Fetch the first page.
      - If you get 500 results, repeat with `newer_than` set to the latest `executed_at` from the last batch, until you get less than 500.
      - Insert all results, deduplication handled by DB (primary key on `tracking_id`).

### 2. Historical Backfill
- **Goal:** Populate your DB with all historical trades for each ticker.
- **How:**
  1. For each ticker:
      - For each date from your desired start date to today:
          - Use the same incremental logic as above, but start with `newer_than` unset (or the earliest possible).
          - Paginate through all results for that date.
          - Insert all results, deduplication handled by DB.

### 3. Error Handling & Logging
- Log API errors, DB errors, and number of trades inserted per run.
- Add a small sleep (e.g., 0.5s) between requests to avoid rate limits.

---

## News Collector: Implementation Plan

### 1. Incremental Collection (Every 5 Minutes)
- **Goal:** On each run, fetch all new news headlines since the last run.
- **How:**
  1. Use `/api/news/headlines` with:
      - `limit` = 100 (max allowed)
      - `page` = 0, 1, 2, ... until you get less than 100 results.
  2. Insert all results, deduplication handled by DB (unique on `headline` + `created_at`).

### 2. Historical Backfill
- **Goal:** Populate your DB with all historical news headlines.
- **How:**
  1. Start with `page=0`, fetch with `limit=100`.
  2. Continue incrementing `page` until you get less than 100 results.
  3. Insert all results, deduplication handled by DB.

### 3. Error Handling & Logging
- Log API errors, DB errors, and number of headlines inserted per run.
- Add a small sleep (e.g., 0.5s) between requests to avoid rate limits.

---

## Summary Table

| Collector     | Endpoint                        | Pagination | Time Filter      | Deduplication Key         | Notes                        |
|---------------|---------------------------------|------------|------------------|---------------------------|------------------------------|
| Dark Pool     | /api/darkpool/{ticker}          | limit=500  | date, newer_than | tracking_id (PK)          | Use newer_than for incremental|
| News          | /api/news/headlines             | limit=100, page | None         | headline + created_at (unique) | Fetch all pages each run      |

---

## Next Steps

1. Implement the above plan for the dark pool collector first.
2. Test and validate that new and historical data is being collected and deduplicated.
3. Once dark pool is working, repeat for the news collector. 