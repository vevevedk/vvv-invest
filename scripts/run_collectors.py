#!/usr/bin/env python3

import os
import sys
import logging
import time
import argparse
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set environment file to local
os.environ['ENV_FILE'] = '.env.local'

from collectors.darkpool.darkpool_collector import DarkPoolCollector
from collectors.news.newscollector import NewsCollector
from config.db_config import get_db_config
from flow_analysis.config.watchlist import SYMBOLS

# Configure logging to be less verbose
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Reduce verbosity of requests and urllib3
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run data collectors')
    parser.add_argument('--env', choices=['local', 'prod'], default='local',
                      help='Environment to run against (local or prod)')
    return parser.parse_args()

def export_darkpool_trades(db_config, hours=24):
    """Export dark pool trades from the last N hours to CSV."""
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        # Query to get trades from last N hours
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

def export_news_headlines(db_config, hours=24):
    """Export news headlines from the last N hours to CSV."""
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        
        # Query to get headlines from last N hours
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
    """Run collectors in sequence and export data."""
    args = parse_args()
    
    # Set environment file based on argument
    env_file = '.env.local' if args.env == 'local' else '.env.prod'
    os.environ['ENV_FILE'] = env_file
    logger.info(f"Using environment file: {env_file}")
    
    start_time = time.time()
    logger.info("Starting collector run sequence")
    
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Run dark pool collector
        logger.info("Starting dark pool collector")
        darkpool_start = time.time()
        darkpool_collector = DarkPoolCollector()
        darkpool_collector.collect_recent_trades(symbols=SYMBOLS, hours=24)
        darkpool_duration = time.time() - darkpool_start
        logger.info(f"Dark pool collector completed in {darkpool_duration:.2f} seconds")
        
        # Export dark pool trades
        logger.info("Exporting dark pool trades")
        export_start = time.time()
        num_darkpool_trades = export_darkpool_trades(db_config)
        export_duration = time.time() - export_start
        logger.info(f"Exported {num_darkpool_trades} dark pool trades in {export_duration:.2f} seconds")
        
        # Run news collector
        logger.info("Starting news collector")
        news_start = time.time()
        news_collector = NewsCollector()
        news_collector.collect()
        news_duration = time.time() - news_start
        logger.info(f"News collector completed in {news_duration:.2f} seconds")
        
        # Export news headlines
        logger.info("Exporting news headlines")
        export_start = time.time()
        num_news_headlines = export_news_headlines(db_config)
        export_duration = time.time() - export_start
        logger.info(f"Exported {num_news_headlines} news headlines in {export_duration:.2f} seconds")
        
        # Calculate total duration
        total_duration = time.time() - start_time
        logger.info(f"Total run completed in {total_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in collector run sequence: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 