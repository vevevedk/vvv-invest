# Production Dark Pool Collector Issues & Improvement Plan (2025-05-06)

## Summary of Issues

### 1. Resource Exhaustion
- **High CPU and memory usage**: Load averages >8, swap fully used, only ~44MB RAM free.
- **kswapd0** (kernel swap daemon) using high CPU, indicating constant swapping.
- **OOM killer** terminating python collector processes due to memory exhaustion.

### 2. Multiple Overlapping Collector Processes
- **Dozens of darkpool_collector.py processes** running simultaneously.
- Each started by cron every 5 minutes, but previous ones do not exit before the next starts.
- Each process opens DB connections and consumes memory/CPU.

### 3. Database Connection Pool Exhaustion
- Too many concurrent collectors exhaust the DB connection pool, preventing new connections and impacting other services.

### 4. Cron Job Configuration
- Cron job runs every 5 minutes regardless of whether the previous collector finished.
- No locking or process management to prevent overlap.

### 5. System Stability
- System becomes slow and unresponsive due to resource exhaustion.
- Frequent OOM kills and high swap usage.

---

## Root Causes
- **No singleton enforcement**: Cron allows multiple overlapping runs.
- **No process locking**: Collector does not check if another instance is running.
- **No resource cleanup**: DB connections and memory not always released.
- **No runtime monitoring**: No alerting or self-healing for stuck/failed collectors.

---

## Recommendations & Improvement Plan

### Short-Term
- Use `pkill -f darkpool_collector.py` to kill all running collectors.
- Comment out the cron job to prevent new runs until fixed.

### Medium/Long-Term

#### 1. **Switch from Cron to Systemd or Celery**
- Use `systemd` to manage the collector as a service:
  - Ensures only one instance runs at a time.
  - Can auto-restart on failure.
  - Supports logging and resource limits.
- Or use a task queue (e.g., Celery) for distributed, managed scheduling.

#### 2. **Add Singleton Locking**
- Implement a file lock or database lock so only one collector runs at a time.
- Example: Use `flock` or a lock row in the database.

#### 3. **Graceful Shutdown & Resource Management**
- Always close DB connections (use context managers).
- Handle signals (SIGTERM, SIGINT) for graceful shutdown.
- Ensure all resources are released on exit.

#### 4. **Monitoring & Alerting**
- Log process start/stop, errors, and resource usage.
- Set up alerts for high memory/CPU or too many DB connections.
- Consider using Prometheus/node_exporter for system metrics.

#### 5. **Optimize Collector Runtime**
- Profile and optimize code to reduce memory/CPU usage.
- Batch DB writes and limit in-memory data.
- Add timeouts and fail-safes for long-running operations.

#### 6. **Generalize for Future Collectors**
- Create a base collector class with:
  - Robust scheduling
  - Locking
  - Resource management
  - Logging and alerting
- Use this as a template for all future data collectors.

---

## Next Steps
- Review and implement the above improvements.
- Test on staging before re-enabling on production.
- Monitor system and DB health after deployment.

---

*Documented by: [Your Name], 2025-05-06* 