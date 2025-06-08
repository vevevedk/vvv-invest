#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from collectors.news.newscollector import NewsCollector
from collectors.utils.logging_config import setup_logging

# Set up logging
logger = setup_logging('news_backfill_test', 'news_backfill_test.log')

def test_backfill():
    """Test the optimized news collector with a 2-day backfill."""
    try:
        # Initialize collector
        collector = NewsCollector()
        
        # Set up date range for 2-day backfill
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=2)
        
        logger.info(f"Starting test backfill from {start_date} to {end_date}")
        
        # Run backfill
        collector.backfill(start_date=start_date, end_date=end_date)
        
        # Get summary of collected data
        df = collector.get_all_headlines()
        if not df.empty:
            logger.info("\nBackfill Summary:")
            logger.info(f"Total articles collected: {len(df)}")
            logger.info(f"Date range: {df['created_at'].min()} to {df['created_at'].max()}")
            logger.info(f"Unique sources: {df['source'].nunique()}")
            logger.info(f"Articles by source:\n{df['source'].value_counts()}")
            logger.info(f"Articles by date:\n{df['created_at'].dt.date.value_counts().sort_index()}")
        else:
            logger.warning("No articles were collected during the backfill")
            
    except Exception as e:
        logger.error(f"Test backfill failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_backfill() 