# 2025-05-29 Dev Work Notes

## Summary of Achievements

### 1. Production Deployment and Collector Optimization
- Successfully merged and pushed the `feature/celery-optimization` branch into `master`.
- Pulled and deployed the latest changes on the production server.
- Updated and fixed all systemd service files for Celery workers and beat services.
- Ensured correct Celery app paths and environment variables in all service files.

### 2. Celery and Systemd Troubleshooting
- Fixed issues with unsupported Celery CLI flags (`--broker-connection-retry-on-startup`).
- Moved `broker_connection_retry_on_startup=True` to Python config (`celery_config.py`).
- Committed and pushed all service and config changes to git.
- Purged old Celery queues and deleted outdated beat schedule files to clear stale tasks.
- Restarted all Celery services and confirmed they are running.

### 3. Data Validation and Monitoring
- Created and ran a Python script to validate that new data is being inserted into the production database for both news headlines and dark pool trades.
- Identified that no new data was being inserted due to collector worker issues, then resolved those issues.
- Confirmed that collectors are now running and ready to process new data.

### 4. Dashboard Troubleshooting
- Investigated and began troubleshooting the collector dashboard at invest.veveve.dk.
- Identified schema and data freshness issues reflected in the dashboard.
- Planned next steps for ongoing monitoring and dashboard validation.

## Next Steps
- Monitor collector logs and dashboard for healthy status and recent data.
- Continue troubleshooting any remaining dashboard or data pipeline issues.
- Document any further fixes or improvements as needed.

---

*End of 2025-05-29 dev work notes.* 