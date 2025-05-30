from celery import Celery
from celery.schedules import crontab
from config.celery.celery_config import *

app = Celery('news_collector')

# Configure Celery with common settings
app.conf.update(
    broker_url=BROKER_URL,
    result_backend=RESULT_BACKEND,
    task_serializer=TASK_SERIALIZER,
    accept_content=ACCEPT_CONTENT,
    result_serializer=RESULT_SERIALIZER,
    timezone=TIMEZONE,
    enable_utc=ENABLE_UTC,
    broker_connection_retry_on_startup=BROKER_CONNECTION_RETRY_ON_STARTUP,
    task_queues=TASK_QUEUES,
)

# Import tasks
from collectors.news_tasks import run_news_collector

# Configure beat schedule
app.conf.beat_schedule = {
    'run-news-collector-every-5-mins': {
        'task': 'collectors.news_tasks.run_news_collector',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'options': {'queue': 'news_queue'},
    },
} 