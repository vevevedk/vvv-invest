from celery import Celery
from celery.schedules import crontab
import logging
from config.env_config import LOG_LEVEL, LOG_DIR
from config.celery.celery_config import *

# Set up logging
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'celery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create Celery app
app = Celery('darkpool_collector')

# Configure app with common settings
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

# Configure beat schedule
app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'collectors.darkpool_tasks.run_darkpool_collector',
        'schedule': crontab(minute='*/5'),
        'options': {'queue': 'dark_pool_queue'},
        'kwargs': {'hours': 24}  # Collect trades for the last 24 hours
    },
    'run-news-collector-every-5-mins': {
        'task': 'collectors.news.newscollector.run_news_collector',
        'schedule': crontab(minute='*/5'),
        'options': {'queue': 'news_queue'},
        'kwargs': {'minutes': 10}  # Collect news from last 10 minutes
    },
}

# Import tasks after app configuration
from collectors.darkpool_tasks import run_darkpool_collector
from collectors.news.newscollector import run_news_collector

# Register darkpool tasks
@app.task(name='collectors.darkpool_tasks.run_darkpool_collector')
def run_darkpool_collector_task(hours: int = 24):
    logger.info(f"Starting darkpool collector task for last {hours} hours")
    try:
        result = run_darkpool_collector(hours=hours)
        logger.info(f"Darkpool collector task completed with status: {result['status']}")
        return result
    except Exception as e:
        logger.error(f"Error in darkpool collector task: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}

# Register news tasks
@app.task(name='collectors.news.newscollector.run_news_collector')
def run_news_collector_task(minutes: int = 10):
    logger.info(f"Starting news collector task for last {minutes} minutes")
    try:
        result = run_news_collector(minutes=minutes)
        logger.info(f"News collector task completed with status: {result['status']}")
        return result
    except Exception as e:
        logger.error(f"Error in news collector task: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}
