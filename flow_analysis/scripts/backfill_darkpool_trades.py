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

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.watchlist import SYMBOLS
from flow_analysis.scripts.darkpool_collector import DarkPoolCollector

def backfill_trades(start_time: datetime, end_time: datetime) -> None:
    """Backfill trades between start_time and end_time."""
    collector = DarkPoolCollector()
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
    # Calculate time range
    now = datetime.now(pytz.UTC)
    friday_evening = now - timedelta(days=3)  # Assuming it's Monday
    friday_evening = friday_evening.replace(hour=16, minute=0, second=0, microsecond=0)
    
    print(f"Backfilling trades from {friday_evening} to {now}")
    backfill_trades(friday_evening, now)

if __name__ == '__main__':
    main() 