from celery import Celery
from celery.schedules import crontab

app = Celery('news_collector')

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

# Import tasks
from collectors.news_tasks import run_news_collector

# Configure beat schedule
app.conf.beat_schedule = {
    'run-news-collector-every-15-mins': {
        'task': 'collectors.news_tasks.run_news_collector',
        'schedule': crontab(minute='*/15'),  # Every 15 minutes
    },
} 