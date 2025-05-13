from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create Celery app
app = Celery('collectors')

# Configure Celery
app.conf.update(
    broker_url=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    result_backend=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Import tasks
from collectors.news_tasks import run_news_collector

# Register tasks
run_news_collector = app.task(run_news_collector)

# Configure beat schedule
app.conf.beat_schedule = {
    'collect-news-every-5-minutes': {
        'task': 'collectors.news_tasks.run_news_collector',
        'schedule': crontab(minute='*/5'),
    },
}

# No need for autodiscover since we're explicitly importing the task 