import logging
from celery import shared_task
from collectors.news_collector import NewsCollector

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@shared_task(name='collectors.news_tasks.run_news_collector')
def run_news_collector():
    """Run the news collector task."""
    try:
        logger.info("Starting news collector...")
        collector = NewsCollector(is_production=True)
        collector.collect()
        logger.info("News collector completed successfully")
    except Exception as e:
        logger.error(f"Error in news collector: {str(e)}", exc_info=True)
        raise 