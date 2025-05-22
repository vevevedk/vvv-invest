"""
Backfill historical dark pool trades from Friday evening until now.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
import logging
from typing import Optional, Dict, List, Any
import requests
from requests.exceptions import RequestException
import argparse
from dotenv import load_dotenv
import os

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Parse command-line arguments for env file
parser = argparse.ArgumentParser()
parser.add_argument('--env-file', default='.env', help='Path to environment file')
args = parser.parse_args()

# Set ENV_FILE for downstream imports
os.environ['ENV_FILE'] = args.env_file

# Load environment variables from the specified file
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.watchlist import SYMBOLS

# Load DB config from environment
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

from flow_analysis.scripts.darkpool_collector import DarkPoolCollector

def backfill_trades(start_time: datetime, end_time: datetime) -> None:
    """Backfill trades between start_time and end_time."""
    collector = DarkPoolCollector()
    collector.db_conn = None  # Reset connection
    collector.DB_CONFIG = DB_CONFIG  # Use loaded DB config
    collector.connect_db()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    current_time = start_time
    while current_time < end_time:
        try:
            # Make API request
            endpoint = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}"
            response = collector._make_request(endpoint)
            
            if response is None or not response.get('data'):
                logger.warning("No trades data received")
                time.sleep(REQUEST_RATE_LIMIT)
                continue
            
            # Process trades
            trades = collector._process_trades(response['data'])
            
            # Filter trades by time
            trades = trades[trades['executed_at'] >= current_time]
            trades = trades[trades['executed_at'] < current_time + timedelta(minutes=5)]
            
            if not trades.empty:
                # Save to database
                collector.save_trades_to_db(trades)
                logger.info(f"Saved {len(trades)} trades for {current_time}")
            
            # Move to next time window
            current_time += timedelta(minutes=5)
            
            # Rate limiting
            time.sleep(REQUEST_RATE_LIMIT)
            
        except Exception as e:
            logger.error(f"Error processing trades: {str(e)}")
            time.sleep(REQUEST_RATE_LIMIT)
            continue

def main():
    """Main entry point."""
    # Set start time to May 2nd, 16:00 UTC
    start_time = datetime(2025, 5, 2, 16, 0, 0, tzinfo=pytz.UTC)
    # Set end time to yesterday at 16:00 UTC
    now = datetime.now(pytz.UTC)
    yesterday = now - timedelta(days=1)
    end_time = yesterday.replace(hour=16, minute=0, second=0, microsecond=0)

    print(f"Backfilling trades from {start_time} to {end_time}")
    backfill_trades(start_time, end_time)

if __name__ == '__main__':
    main() 