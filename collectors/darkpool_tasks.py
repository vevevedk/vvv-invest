import logging
from collectors.darkpool_collector import DarkPoolCollector

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_darkpool_collector():
    """Run the dark pool collector to fetch and process dark pool trades."""
    try:
        logger.info("Starting dark pool collector...")
        collector = DarkPoolCollector()
        collector.run()
        logger.info("Dark pool collector completed successfully")
    except Exception as e:
        logger.error(f"Error in dark pool collector: {str(e)}", exc_info=True)
        raise

def backfill_qqq_trades():
    """Backfill QQQ trades from a specific date range."""
    try:
        logger.info("Starting QQQ trades backfill...")
        collector = DarkPoolCollector()
        collector.backfill_qqq_trades()
        logger.info("QQQ trades backfill completed successfully")
    except Exception as e:
        logger.error(f"Error in QQQ trades backfill: {str(e)}", exc_info=True)
        raise 