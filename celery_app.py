from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv
from collectors.darkpool_tasks import run_darkpool_collector, backfill_qqq_trades
from collectors.news_tasks import run_news_collector

# Load environment variables
load_dotenv()

# Create Celery app
app = Celery('tasks', broker='redis://localhost:6379/0')

# Configure Celery
app.conf.update(
    broker_url=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    result_backend=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_routes={
        'celery_app.run_darkpool_collector_task': {'queue': 'darkpool_collector'},
        'celery_app.backfill_qqq_trades_task': {'queue': 'darkpool_collector'},
        'celery_app.run_news_collector_task': {'queue': 'news_collector'},
    },
    beat_schedule={
        'run-darkpool-collector': {
            'task': 'celery_app.run_darkpool_collector_task',
            'schedule': crontab(minute='*/5'),  # Every 5 minutes
        },
        'run-news-collector': {
            'task': 'celery_app.run_news_collector_task',
            'schedule': crontab(minute='*/5'),  # Every 5 minutes
        },
    }
)

# Register tasks
@app.task(name='celery_app.run_darkpool_collector_task')
def run_darkpool_collector_task():
    return run_darkpool_collector()

@app.task(name='celery_app.backfill_qqq_trades_task')
def backfill_qqq_trades_task():
    return backfill_qqq_trades()

@app.task(name='celery_app.run_news_collector_task')
def run_news_collector_task():
    return run_news_collector()

# No need for autodiscover since we're explicitly importing the task 