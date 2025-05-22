# 2025-05-19 Collector Fixes: Developer TODO List

## Overview
This document tracks the current issues encountered during local backfill of news and dark pool data, and outlines the steps required to resolve them.

---

## Issues

### 1. News Collector: 429 Too Many Requests
- **Description:**
  - The news collector is hitting the Unusual Whales API rate limit (HTTP 429) when backfilling large date ranges (e.g., 7 days).
  - This results in incomplete data collection and failed requests.
  - The API supports the following query parameters: `limit` (max 100), `major_only`, `page`, `search_term`, `sources`.
  - Official documentation recommends using the `limit` and `page` parameters for pagination, and respecting rate limits.

### 2. Dark Pool Collector: 403 Forbidden
- **Description:**
  - The dark pool collector receives HTTP 403 Forbidden errors from the Unusual Whales API for all requests.
  - This indicates the API key is not authorized for this endpoint, or the endpoint is restricted for the account.

---

## TODOs & Next Steps

### [ ] 1. News Collector: Handle API Rate Limiting and Pagination
- [ ] Review Unusual Whales API documentation for official rate limits (requests per minute/hour).
- [ ] Add a delay (e.g., `time.sleep(1)` or longer) between requests in the news collector's `fetch_data` loop.
- [ ] Consider batching or reducing the backfill window if needed.
- [ ] Optionally, implement retry logic with exponential backoff for 429 errors.
- [ ] Review and optimize use of query parameters: `limit`, `page`, `major_only`, `search_term`, `sources`.
- [ ] Ensure correct pagination and data completeness.
- [ ] Test backfill again and monitor for 429 errors.

### [ ] 2. Dark Pool Collector: Fix API Authorization
- [ ] Double-check the `UW_API_TOKEN` in `.env` (ensure it matches the working production/pro account token).
- [ ] Test the dark pool endpoint manually with `curl` or Postman using the current token.
- [ ] Check Unusual Whales API documentation and your account permissions for dark pool data access.
- [ ] If the token should have access but still gets 403, contact Unusual Whales support.
- [ ] Update `.env` with a valid token if needed and retest.

---

## Notes
- Both collectors now use a unified, environment-aware loading pattern (`ENV_FILE`).
- All environment variable issues should be resolved; focus is now on API usage, permissions, and correct use of request parameters.
- Revisit this checklist after each fix and update as needed. 