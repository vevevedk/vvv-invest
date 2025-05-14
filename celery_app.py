from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv
from collectors.darkpool_tasks import run_darkpool_collector, backfill_qqq_trades

# Load environment variables
load_dotenv()

# Create Celery app
app = Celery('darkpool_collector')

# Configure Celery
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Configure beat schedule
app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'celery_app.run_darkpool_collector_task',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
}

# Register tasks
@app.task(name='celery_app.run_darkpool_collector_task')
def run_darkpool_collector_task():
    return run_darkpool_collector()

@app.task(name='celery_app.backfill_qqq_trades_task')
def backfill_qqq_trades_task():
    return backfill_qqq_trades()
