import logging
from collectors.news_collector import NewsCollector

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_news_collector():
    """Run the news collector to fetch and process news articles."""
    try:
        logger.info("Starting news collector...")
        collector = NewsCollector()
        collector.run()
        logger.info("News collector completed successfully")
    except Exception as e:
        logger.error(f"Error in news collector: {str(e)}", exc_info=True)
        raise 