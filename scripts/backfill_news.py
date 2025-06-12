#!/usr/bin/env python3

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
import traceback

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from collectors.news.newscollector import NewsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill/news_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def backfill_news(start_date: str = None, end_date: str = None, days: int = None):
    """Backfill news headlines for a given date range or number of days.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        days (int, optional): Number of days to backfill from today
    """
    try:
        # If days is specified, calculate date range
        if days is not None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Validate dates
        if start_date:
            datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create collector and run backfill
        collector = NewsCollector()
        collector.collect(start_date=start_date, end_date=end_date)
        
    except ValueError as e:
        logger.error(f"Invalid date format: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during backfill: {str(e)}")
        sys.exit(1)

def main():
    """Command-line interface for news backfill."""
    import argparse
    
    parser = argparse.ArgumentParser(description='News Headlines Backfill Tool')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--days', type=int, help='Number of days to backfill from today')
    group.add_argument('--date-range', nargs=2, metavar=('START_DATE', 'END_DATE'),
                      help='Date range to backfill (YYYY-MM-DD YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.days:
        backfill_news(days=args.days)
    else:
        start_date, end_date = args.date_range
        backfill_news(start_date=start_date, end_date=end_date)

if __name__ == "__main__":
    main() 