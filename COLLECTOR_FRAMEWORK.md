# Collector Framework Guide

## Overview
This framework provides a robust, extensible base for all data collectors using Celery for distributed scheduling and execution. All collectors should inherit from `BaseCollector` and be registered as Celery tasks.

---

## Creating a New Collector

1. **Inherit from BaseCollector**
   - Create a new file in `collectors/`, e.g., `my_collector.py`.
   - Inherit from `BaseCollector` and implement the `collect()` method.

```python
from collectors.base_collector import BaseCollector

class MyCollector(BaseCollector):
    def collect(self):
        self.logger.info("Collecting my data...")
        # Your collection logic here
```

2. **Register as a Celery Task**
   - In `collectors/tasks.py`, add:

```python
from collectors.my_collector import MyCollector

@app.task
def run_my_collector():
    collector = MyCollector()
    collector.run()
```

3. **Schedule with Celery Beat**
   - In `celery_app.py`, add to `beat_schedule`:

```python
app.conf.beat_schedule = {
    'run-my-collector-every-5-mins': {
        'task': 'collectors.tasks.run_my_collector',
        'schedule': 300.0,  # every 5 minutes
    },
}
```

---

## Best Practices
- **Resource Management:** Always use context managers for DB/API connections.
- **Error Handling:** Use try/except in `collect()` and log all errors.
- **Logging:** Use `self.logger` for all logs.
- **Singleton Enforcement:** Celery ensures only one task instance per schedule, but avoid global state.
- **Testing:** Test collectors in staging before production.
- **Monitoring:** Set up logging and alerting for failures and resource usage.

---

## Running the Framework

1. **Start Redis:**
   ```sh
   redis-server
   ```
2. **Start Celery Worker:**
   ```sh
   celery -A celery_app.app worker --loglevel=info
   ```
3. **Start Celery Beat Scheduler:**
   ```sh
   celery -A celery_app.app beat --loglevel=info
   ```

---

## Extending the Framework
- Use `BaseCollector` for all new collectors.
- Register each as a Celery task.
- Add to `beat_schedule` for periodic execution.
- Follow best practices for reliability and maintainability. 