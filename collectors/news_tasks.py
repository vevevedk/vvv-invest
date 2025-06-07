import logging
from celery import shared_task
from collectors.news.newscollector import run_news_collector as improved_run_news_collector

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@shared_task(name='collectors.news_tasks.run_news_collector')
def run_news_collector():
    """Run the news collector task."""
    try:
        logger.info("Starting news collector...")
        result = improved_run_news_collector()
        logger.info("News collector completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in news collector: {str(e)}", exc_info=True)
        raise 