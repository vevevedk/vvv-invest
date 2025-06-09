#!/usr/bin/env python3

import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text
import argparse

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.darkpool_collector import DarkPoolCollector
from collectors.news.newscollector import NewsCollector
from config.db_config import get_db_config
from flow_analysis.config.watchlist import SYMBOLS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Reduce verbosity of requests and urllib3
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def export_darkpool_trades(db_config, start_time, end_time):
    """Export dark pool trades for the specified time range to CSV."""
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        
        # Query to get trades in time range
        query = text("""
            SELECT *
            FROM trading.darkpool_trades
            WHERE executed_at >= :start_time
            AND executed_at <= :end_time
            ORDER BY executed_at DESC
        """)
        
        # Execute query and convert to DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"start_time": start_time, "end_time": end_time}
            )
        
        # Create exports directory if it doesn't exist
        os.makedirs("exports", exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exports/darkpool_trades_{timestamp}.csv"
        
        # Export to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} dark pool trades to {filename}")
        
        return len(df)
        
    except Exception as e:
        logger.error(f"Error exporting dark pool trades: {str(e)}")
        return 0

def export_news_headlines(db_config, start_time, end_time):
    """Export news headlines for the specified time range to CSV."""
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        
        # Query to get headlines in time range
        query = text("""
            SELECT *
            FROM trading.news_headlines
            WHERE created_at >= :start_time
            AND created_at <= :end_time
            ORDER BY created_at DESC
        """)
        
        # Execute query and convert to DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"start_time": start_time, "end_time": end_time}
            )
        
        # Create exports directory if it doesn't exist
        os.makedirs("exports", exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exports/news_headlines_{timestamp}.csv"
        
        # Export to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} news headlines to {filename}")
        
        return len(df)
        
    except Exception as e:
        logger.error(f"Error exporting news headlines: {str(e)}")
        return 0

def main():
    """Run backfill for both collectors and export data."""
    parser = argparse.ArgumentParser(description='Backfill data collectors')
    parser.add_argument('--days', type=int, default=7,
                      help='Number of days to look back (default: 7)')
    parser.add_argument('--symbols', nargs='+', default=SYMBOLS,
                      help='List of symbols to collect (default: all watchlist symbols)')
    args = parser.parse_args()
    
    start_time = time.time()
    logger.info("Starting backfill sequence")
    
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time_range = end_time - timedelta(days=args.days)
        
        # Run dark pool collector backfill
        logger.info("Starting dark pool collector backfill")
        darkpool_start = time.time()
        darkpool_collector = DarkPoolCollector()
        darkpool_results = darkpool_collector.backfill_trades(symbols=args.symbols, hours=args.days * 24)
        darkpool_duration = time.time() - darkpool_start
        logger.info(f"Dark pool collector backfill completed in {darkpool_duration:.2f} seconds")
        
        # Export dark pool trades
        logger.info("Exporting dark pool trades")
        export_start = time.time()
        num_darkpool_trades = export_darkpool_trades(db_config, start_time_range, end_time)
        export_duration = time.time() - export_start
        logger.info(f"Exported {num_darkpool_trades} dark pool trades in {export_duration:.2f} seconds")
        
        # Run news collector backfill
        logger.info("Starting news collector backfill")
        news_start = time.time()
        news_collector = NewsCollector()
        news_collector.collect(
            start_date=start_time_range.strftime('%Y-%m-%d'),
            end_date=end_time.strftime('%Y-%m-%d')
        )
        news_duration = time.time() - news_start
        logger.info(f"News collector backfill completed in {news_duration:.2f} seconds")
        
        # Export news headlines
        logger.info("Exporting news headlines")
        export_start = time.time()
        num_news_headlines = export_news_headlines(db_config, start_time_range, end_time)
        export_duration = time.time() - export_start
        logger.info(f"Exported {num_news_headlines} news headlines in {export_duration:.2f} seconds")
        
        # Calculate total duration
        total_duration = time.time() - start_time
        logger.info(f"Total backfill completed in {total_duration:.2f} seconds")
        
        # Print summary
        print("\nBackfill Summary:")
        print("-" * 50)
        print("Dark Pool Trades:")
        for symbol, count in darkpool_results.items():
            status = f"{count} trades" if count >= 0 else "Failed"
            print(f"{symbol}: {status}")
        print(f"\nTotal Dark Pool Trades: {num_darkpool_trades}")
        print(f"Total News Headlines: {num_news_headlines}")
        print("-" * 50)
        
    except Exception as e:
        logger.error(f"Error in backfill sequence: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 