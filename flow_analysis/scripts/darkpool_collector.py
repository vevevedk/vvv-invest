"""
Dark Pool Trade Collector
Continuously collects dark pool trades throughout the trading day.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import pytz
from typing import Optional, Dict, List
import psycopg2
from psycopg2.extras import execute_values
import requests
import argparse

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from config.db_config import DB_CONFIG, SCHEMA_NAME, TABLE_NAME
from config.watchlist import MARKET_OPEN, MARKET_CLOSE

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DarkPoolCollector:
    def __init__(self, data_dir: Optional[Path] = None):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.data_dir = data_dir or (project_root / "data/raw/darkpool")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.last_request_time = 0
        self.collected_tracking_ids = set()
        
        # Set timezone to US/Eastern for market hours
        self.eastern = pytz.timezone('US/Eastern')
        
        # Initialize database connection
        self.db_conn = None
        self.connect_db()
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def _rate_limit(self):
        """Implement rate limiting to prevent API throttling"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1.0 / REQUEST_RATE_LIMIT:
            time.sleep(1.0 / REQUEST_RATE_LIMIT - time_since_last_request)
        self.last_request_time = time.time()
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic"""
        try:
            self._rate_limit()
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            return None
            
    def _validate_response(self, data: Dict) -> bool:
        """Validate API response data"""
        if not isinstance(data, dict):
            logger.error("Invalid response format: not a dictionary")
            return False
        if "data" not in data:
            logger.error("Invalid response format: missing 'data' field")
            return False
        return True
        
    def _process_trades(self, trades_data: List[Dict]) -> pd.DataFrame:
        """Process raw trades data into a DataFrame"""
        if not trades_data:
            logger.warning("No trades data received from API")
            return pd.DataFrame()
            
        logger.info(f"Received {len(trades_data)} trades from API")
        
        # Convert to DataFrame
        trades = pd.DataFrame(trades_data)
        logger.info(f"DataFrame created with columns: {trades.columns.tolist()}")
        
        # Add collection timestamp
        trades['collection_time'] = datetime.now(self.eastern)
        
        # Remove any trades we've already collected
        if 'tracking_id' in trades.columns:
            initial_count = len(trades)
            trades = trades[~trades['tracking_id'].isin(self.collected_tracking_ids)]
            self.collected_tracking_ids.update(trades['tracking_id'].tolist())
            logger.info(f"Removed {initial_count - len(trades)} duplicate trades")
            
        logger.info(f"Returning {len(trades)} unique trades for processing")
        return trades
        
    def collect_trades(self) -> pd.DataFrame:
        """Collect the latest dark pool trades"""
        endpoint = f"{self.base_url}{DARKPOOL_RECENT_ENDPOINT}"
        
        # If market is closed, get yesterday's data for testing
        if not self.is_market_open():
            yesterday = datetime.now(self.eastern) - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")
            logger.info(f"Market is closed, fetching yesterday's data ({date_str}) for testing")
        else:
            date_str = datetime.now(self.eastern).strftime("%Y-%m-%d")
            
        params = {
            "limit": 200,  # Maximum allowed
            "date": date_str
        }
        
        data = self._make_request(endpoint, params)
        if data and self._validate_response(data):
            return self._process_trades(data["data"])
        return pd.DataFrame()
        
    def save_trades_to_db(self, trades: pd.DataFrame) -> None:
        """Save trades to PostgreSQL database"""
        if trades.empty:
            logger.warning("No trades to save - DataFrame is empty")
            return
            
        try:
            # Ensure database connection is active
            if self.db_conn.closed:
                logger.info("Database connection closed, reconnecting...")
                self.connect_db()
                
            with self.db_conn.cursor() as cur:
                # Prepare data for insertion
                records = trades.to_dict('records')
                logger.info(f"Preparing to insert {len(records)} trades")
                
                # Define columns for insertion
                columns = [
                    'tracking_id', 'symbol', 'size', 'price', 'volume',
                    'premium', 'executed_at', 'nbbo_ask', 'nbbo_bid',
                    'market_center', 'sale_cond_codes', 'collection_time'
                ]
                
                # Create the insert query
                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} 
                    ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT (tracking_id) DO NOTHING
                """
                
                # Prepare values for insertion
                values = []
                for record in records:
                    try:
                        value = (
                            record.get('tracking_id'),
                            record.get('symbol'),
                            record.get('size'),
                            record.get('price'),
                            record.get('volume'),
                            record.get('premium'),
                            record.get('executed_at'),
                            record.get('nbbo_ask'),
                            record.get('nbbo_bid'),
                            record.get('market_center'),
                            record.get('sale_cond_codes'),
                            record.get('collection_time')
                        )
                        values.append(value)
                    except Exception as e:
                        logger.error(f"Error preparing record for insertion: {str(e)}")
                        logger.error(f"Problematic record: {record}")
                        continue
                
                if not values:
                    logger.error("No valid values prepared for insertion")
                    return
                
                logger.info(f"Executing insert query with {len(values)} values")
                # Execute the insert
                execute_values(cur, insert_query, values)
                self.db_conn.commit()
                
                # Verify the insertion
                cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}")
                total_count = cur.fetchone()[0]
                logger.info(f"Total trades in database after insertion: {total_count}")
                
        except Exception as e:
            logger.error(f"Error saving trades to database: {str(e)}")
            logger.error(f"Database error details: {str(e)}")
            self.db_conn.rollback()
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        # Get current time in Eastern timezone
        current_time = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if current_time.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return False
            
        # Check if it's within market hours
        market_open = current_time.replace(
            hour=MARKET_OPEN.hour,
            minute=MARKET_OPEN.minute,
            second=0,
            microsecond=0
        )
        market_close = current_time.replace(
            hour=MARKET_CLOSE.hour,
            minute=MARKET_CLOSE.minute,
            second=0,
            microsecond=0
        )
        
        return market_open <= current_time <= market_close
        
    def run(self, collection_interval: int = 300):  # 5 minutes in seconds
        """Run the collector continuously during market hours"""
        logger.info("Starting dark pool trade collector...")
        
        while True:
            logger.info("Collecting trades...")
            trades = self.collect_trades()
            if not trades.empty:
                self.save_trades_to_db(trades)
            else:
                logger.warning("No new trades collected")
                
            # Wait for the next collection interval
            time.sleep(collection_interval)
            
    def __del__(self):
        """Clean up database connection"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

def main():
    # Add argument parsing
    parser = argparse.ArgumentParser(description='Dark Pool Trade Collector')
    parser.add_argument('--historical', action='store_true', help='Fetch data from last trading day (April 17th)')
    args = parser.parse_args()

    collector = DarkPoolCollector()
    
    if args.historical:
        # Override collect_trades method temporarily for historical data
        def historical_collect():
            endpoint = f"{collector.base_url}{DARKPOOL_RECENT_ENDPOINT}"
            params = {
                "limit": 200,
                "date": "2025-04-17"  # Thursday before Good Friday
            }
            logger.info(f"Fetching historical data for 2025-04-17")
            data = collector._make_request(endpoint, params)
            if data and collector._validate_response(data):
                return collector._process_trades(data["data"])
            return pd.DataFrame()
        
        # Collect and save historical data once
        trades = historical_collect()
        if not trades.empty:
            collector.save_trades_to_db(trades)
            logger.info("Historical data collection complete")
        else:
            logger.warning("No historical trades collected")
    else:
        # Normal continuous collection
        collector.run()

if __name__ == "__main__":
    main() 