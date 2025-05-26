from celery import Celery
from celery.schedules import crontab
import logging
from flow_analysis.config.env_config import (
    CELERY_CONFIG,
    UW_API_TOKEN,
    LOG_LEVEL,
    LOG_DIR
)

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

# Create Celery apps
darkpool_app = Celery('darkpool_collector')
news_app = Celery('news_collector')

# Configure both apps with common settings
for app in [darkpool_app, news_app]:
    app.conf.update(CELERY_CONFIG)

# Configure darkpool app
darkpool_app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'celery_app.run_darkpool_collector_task',
        'schedule': crontab(minute='*/5'),
        'options': {'queue': 'darkpool_queue'},
    },
}

# Configure news app
news_app.conf.beat_schedule = {
    'run-news-collector-every-5-mins': {
        'task': 'celery_app.run_news_collector_task',
        'schedule': crontab(minute='*/5'),
        'options': {'queue': 'news_queue'},
    },
}

# Import tasks after app configuration
from collectors.darkpool_tasks import run_darkpool_collector
from collectors.news_tasks import run_news_collector

# Register darkpool tasks
@darkpool_app.task(name='celery_app.run_darkpool_collector_task')
def run_darkpool_collector_task():
    logger.info("Starting darkpool collector task")
    try:
        result = run_darkpool_collector()
        logger.info("Darkpool collector task completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in darkpool collector task: {str(e)}", exc_info=True)
        raise

# Register news tasks
@news_app.task(name='celery_app.run_news_collector_task')
def run_news_collector_task():
    logger.info("Starting news collector task")
    try:
        result = run_news_collector()
        logger.info("News collector task completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in news collector task: {str(e)}", exc_info=True)
        raise
