from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv

# Load environment variables
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

# Initialize Celery
app = Celery('collectors',
             broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
             backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'))

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/New_York',
    enable_utc=True,
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    worker_prefetch_multiplier=1,  # Process one task at a time
    task_acks_late=True,  # Only acknowledge task after completion
    task_reject_on_worker_lost=True,  # Requeue task if worker dies
)

# Configure periodic tasks
app.conf.beat_schedule = {
    'collect-dark-pool-trades': {
        'task': 'tasks.collect_dark_pool_trades',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'options': {'queue': 'dark_pool'}
    },
    'collect-news': {
        'task': 'tasks.collect_news',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'options': {'queue': 'news'}
    }
} 