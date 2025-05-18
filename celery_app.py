from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv
from collectors.darkpool_tasks import run_darkpool_collector, backfill_qqq_trades
from collectors.news_tasks import run_news_collector

# Load environment variables
load_dotenv()

# Create Celery apps
darkpool_app = Celery('darkpool_collector')
news_app = Celery('news_collector')

# Common Celery configuration
celery_config = {
    'broker_url': 'redis://localhost:6379/0',
    'result_backend': 'redis://localhost:6379/0',
    'task_serializer': 'json',
    'accept_content': ['json'],
    'result_serializer': 'json',
    'timezone': 'UTC',
    'enable_utc': True,
}

# Configure darkpool app
darkpool_app.conf.update(celery_config)
darkpool_app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'celery_app.run_darkpool_collector_task',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
}

# Configure news app
news_app.conf.update(celery_config)
news_app.conf.beat_schedule = {
    'run-news-collector-every-5-mins': {
        'task': 'celery_app.run_news_collector_task',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
}

# Register darkpool tasks
@darkpool_app.task(name='celery_app.run_darkpool_collector_task')
def run_darkpool_collector_task():
    return run_darkpool_collector()

@darkpool_app.task(name='celery_app.backfill_qqq_trades_task')
def backfill_qqq_trades_task():
    return backfill_qqq_trades()

# Register news tasks
@news_app.task(name='celery_app.run_news_collector_task')
def run_news_collector_task():
    return run_news_collector()
