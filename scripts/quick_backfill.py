#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from collectors.darkpool.darkpoolcollector import DarkPoolCollector
from collectors.news.newscollector import NewsCollector
from config.db_config import get_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_quick_backfill():
    """Run a quick backfill to get recent data."""
    try:
        # Initialize collectors
        darkpool_collector = DarkPoolCollector()
        news_collector = NewsCollector()
        
        # Get data from last 30 minutes
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=30)
        
        logger.info(f"Fetching data from {start_time} to {end_time}")
        
        # Collect dark pool data
        logger.info("\nCollecting dark pool data...")
        darkpool_collector.fetch_data(start_time=start_time, end_time=end_time)
        
        # Collect news data
        logger.info("\nCollecting news data...")
        news_collector.fetch_data(start_time=start_time, end_time=end_time)
        
        logger.info("\nBackfill completed successfully!")
        
    except Exception as e:
        logger.error(f"Backfill failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_quick_backfill() 