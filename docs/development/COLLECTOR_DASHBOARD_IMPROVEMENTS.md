# Collector Dashboard: Prioritized Implementation Order

1. Dashboard Badges for "Last Seen"/Heartbeat (Quick win, visible impact)
2. Status History Panel (Timeline of status changes for each collector)
3. Log Viewer (Recent log messages per collector, filterable by type/severity)
4. Manual Controls (Backfill/Restart) (Buttons to trigger backfill or restart for each collector, with permission checks)
5. Completeness & Freshness Metrics (Show expected vs. actual items, completeness %, and freshness badges)
6. Automated Alerts (Slack/Email) (Alert if collector is stalled, no heartbeat, or unhealthy)

---

# Collector Dashboard: Improvements & Roadmap

This document tracks proposed and implemented improvements for the collector-dashboard system, focusing on reliability, observability, and maintainability.

---

## 1. Collector Health & Status Improvements
- **Granular Statuses:**
  - Add more detailed statuses: `running`, `waiting_for_market_open`, `error`, `no_data`, etc.
- **Heartbeat Logging:**
  - Each collector logs a "heartbeat" (e.g., `Collector alive at ...`) to the `collector_logs` table every N minutes for liveness detection.

---

## 2. Alerting & Notifications
- **Automated Alerts:**
  - Integrate with Slack, email, or PagerDuty to send alerts if:
    - A collector is stalled for more than X minutes.
    - No heartbeat is received in Y minutes.
    - The dashboard status is `unhealthy` for Z minutes.
- **Dashboard Badges:**
  - Add visual indicators for "last seen"/heartbeat time for each collector.

---

## 3. Dashboard Enhancements
- **Status History:**
  - Populate the "Status History" panel with a timeline of status changes (from `collector_logs`).
- **Log Viewer:**
  - Add a log viewer to the dashboard for recent log messages per collector.
- **Manual Controls:**
  - Add buttons to trigger manual backfill or restart collectors from the dashboard (if permissions allow).

---

## 4. Database & Logging
- **Structured Logging:**
  - Add more structured fields to `collector_logs` (e.g., `collector_name`, `task_type`, `details` JSONB).
- **Retention Policy:**
  - Set up retention/cleanup for old logs to keep the table performant.

---

## 5. Collector Code Quality
- **Unit & Integration Tests:**
  - Add tests for collector logic, especially edge cases (market closed, API errors, DB errors).
- **Retry & Backoff:**
  - Ensure all API calls have robust retry and exponential backoff logic.
- **Graceful Shutdown:**
  - Collectors handle SIGTERM/SIGINT gracefully and log shutdown events.

---

## 6. Observability & Metrics
- **Prometheus/Grafana:**
  - Expose Prometheus metrics (e.g., collection latency, error count, last successful run) and build Grafana dashboards.
- **Sentry or Error Tracking:**
  - Integrate with Sentry or similar for error tracking and alerting on exceptions.

---

## 7. Documentation & Onboarding
- **Runbooks:**
  - Document troubleshooting, restart procedures, and escalation paths.
- **API Usage:**
  - Track API usage/credits and alert if nearing limits.

---

## 8. Security & Reliability
- **Secrets Management:**
  - Store DB/API credentials in a secure vault (not in env files or code).
- **Redundancy:**
  - Consider redundant/failover collectors if uptime is critical.

---

## 9. News Collector
- **Apply the Same Improvements:**
  - Once the dark pool collector is "green," apply the same health/status/alerting improvements to the news collector.

---

## 10. UI/UX: Data Completeness & Freshness (Top Priority)
- **Data Freshness Indicator:**
  - Show the timestamp of the most recent data item collected (not just the last log/heartbeat).
  - Display a badge ("Up to Date"/"Stale") based on how recent the last data is.
- **Completeness Metrics:**
  - Show expected vs. actual items collected for each interval (e.g., hour/day).
  - Display a completeness percentage (e.g., 93/100 = 93%).
  - Highlight missing data or gaps with alerts or color coding.
- **Visualizations:**
  - Add a chart of items collected per interval.
  - Add a timeline/status history for collector activity.
- **Recent Data Preview:**
  - Show a table of the most recent data items (e.g., headlines, trades).
- **Custom Alerts:**
  - Alert if data is stale or incomplete for a configurable period.

---

## Next Steps
- Prioritize which improvements to implement next.
- Reference this document in planning and development discussions. 