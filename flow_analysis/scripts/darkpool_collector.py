"""
Dark Pool Trade Collector
Continuously collects dark pool trades throughout the trading day.
"""

import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, time
import pandas as pd
from pathlib import Path
import pytz
from typing import Optional, Dict, List, Any
import psycopg2
from psycopg2.extras import execute_values
import requests
import argparse
import time as time_module
from requests.exceptions import RequestException

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME, TABLE_NAME
from flow_analysis.config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS

class DatabaseLogHandler(logging.Handler):
    """Custom logging handler that writes logs to the database."""
    
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def emit(self, record):
        if self.conn.closed:
            print("Warning: Database connection is closed, reconnecting...")
            return
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trading.collector_logs (timestamp, level, message)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        datetime.fromtimestamp(record.created).astimezone(),
                        record.levelname,
                        self.format(record)
                    )
                )
                self.conn.commit()
        except Exception as e:
            print(f"Error writing to database log: {str(e)}")
            pass  # Still avoid recursion

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "darkpool_collector.log"

# Configure logging with proper rotation and formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add error handler for uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

class DarkPoolCollector:
    """Collect dark pool trades from the API and save them to the database."""
    
    def __init__(self, db_conn=None):
        """Initialize the collector."""
        self.db_conn = db_conn
        self.market_tz = pytz.timezone('US/Eastern')
        self.rate_limit = 1.0  # seconds between requests
        self._last_request_time = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up the logger for the collector."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Add file handler
        file_handler = RotatingFileHandler(
            log_dir / "darkpool_collector.log",
            maxBytes=10_000_000,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def connect_db(self) -> None:
        """Connect to the database."""
        try:
            if self.db_conn is None or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise

    def _rate_limit(self) -> None:
        """Enforce rate limiting between API requests."""
        current_time = time_module.time()
        if hasattr(self, '_last_request_time') and self._last_request_time is not None:
            time_since_last_request = current_time - self._last_request_time
            if time_since_last_request < self.rate_limit:
                sleep_time = self.rate_limit - time_since_last_request
                time_module.sleep(sleep_time)
        self._last_request_time = time_module.time()

    def _validate_response(self, response_data: Dict) -> bool:
        """Validate the API response data."""
        if not isinstance(response_data, dict):
            self.logger.error("Invalid response format: not a dictionary")
            return False
        if 'data' not in response_data:
            self.logger.error("Invalid response format: missing 'data' key")
            return False
        if not isinstance(response_data['data'], list):
            self.logger.error("Invalid response format: 'data' is not a list")
            return False
        return True

    def _make_request(self, endpoint: str) -> Optional[Dict]:
        """Make an API request with retry logic."""
        max_retries = 3
        retry_count = 0
        backoff_time = 1.0  # Initial backoff time in seconds
        
        while retry_count < max_retries:
            try:
                self._rate_limit()
                response = requests.get(endpoint, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                try:
                    data = response.json()
                except ValueError as e:
                    self.logger.error(f"Invalid JSON response: {str(e)}")
                    return None
                if not self._validate_response(data):
                    raise ValueError("Invalid response data")
                return data
            except (requests.exceptions.RequestException, ValueError) as e:
                retry_count += 1
                if retry_count == max_retries:
                    self.logger.error(f"Failed to make request after {max_retries} retries: {str(e)}")
                    return None
                
                self.logger.warning(f"Request failed (attempt {retry_count}/{max_retries}): {str(e)}")
                time_module.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
        return None

    def collect_trades(self) -> pd.DataFrame:
        """Collect recent dark pool trades."""
        try:
            endpoint = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}"
            response = self._make_request(endpoint)
            
            if response is None or not response.get('data'):
                self.logger.warning("No trades data received")
                return pd.DataFrame()
            
            trades = self._process_trades(response['data'])
            self.logger.info(f"Collected {len(trades)} trades")
            return trades
        except Exception as e:
            self.logger.error(f"Error collecting trades: {str(e)}")
            raise

    def _process_trades(self, trades_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process raw trades data into a DataFrame."""
        try:
            trades = pd.DataFrame(trades_data)
            if trades.empty:
                return trades

            # Map API column names to our expected column names
            column_mapping = {
                'tracking_id': 'tracking_id',
                'ticker': 'symbol',
                'price': 'price',
                'size': 'size',
                'executed_at': 'executed_at',
                'market_center': 'market_center',
                'sale_cond_codes': 'sale_cond_codes',
                'nbbo_ask': 'nbbo_ask',
                'nbbo_bid': 'nbbo_bid'
            }
            trades = trades.rename(columns=column_mapping)

            # Filter for target symbols only
            trades = trades[trades['symbol'].isin(SYMBOLS)]

            # Convert data types
            trades['price'] = pd.to_numeric(trades['price'], errors='coerce')
            trades['size'] = pd.to_numeric(trades['size'], errors='coerce')
            trades['nbbo_ask'] = pd.to_numeric(trades['nbbo_ask'], errors='coerce')
            trades['nbbo_bid'] = pd.to_numeric(trades['nbbo_bid'], errors='coerce')
            trades['executed_at'] = pd.to_datetime(trades['executed_at'], errors='coerce')

            # Calculate derived fields
            trades['premium'] = trades['price'] * trades['size']
            trades['nbbo_mid'] = (trades['nbbo_ask'] + trades['nbbo_bid']) / 2

            # Validate required columns
            required_columns = [
                'tracking_id', 'symbol', 'price', 'size', 'executed_at',
                'sale_cond_codes', 'market_center', 'nbbo_ask', 'nbbo_bid',
                'premium'
            ]
            missing_columns = [col for col in required_columns if col not in trades.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Filter out invalid rows
            trades = trades.dropna(subset=required_columns)
            
            self.logger.info(f"Processed {len(trades)} trades successfully")
            return trades
        except Exception as e:
            self.logger.error(f"Error processing trades: {str(e)}")
            raise

    def save_trades_to_db(self, trades: pd.DataFrame) -> None:
        """Save processed trades to the database."""
        if trades.empty:
            self.logger.warning("No trades to save - DataFrame is empty")
            return

        try:
            if self.db_conn is None or self.db_conn.closed:
                self.logger.warning("Database connection is closed, reconnecting...")
                self.connect_db()
                if self.db_conn.closed:
                    raise Exception("Failed to reconnect to database")

            with self.db_conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute("CREATE SCHEMA IF NOT EXISTS trading;")
                
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trading.darkpool_trades (
                        id SERIAL PRIMARY KEY,
                        tracking_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price NUMERIC NOT NULL,
                        size INTEGER NOT NULL,
                        executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        sale_cond_codes TEXT NOT NULL,
                        market_center TEXT NOT NULL,
                        nbbo_ask NUMERIC NOT NULL,
                        nbbo_bid NUMERIC NOT NULL,
                        premium NUMERIC NOT NULL,
                        collection_time TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                self.db_conn.commit()

                # Add collection time
                trades['collection_time'] = datetime.now()

                # Prepare data for insertion
                columns = [
                    'tracking_id', 'symbol', 'price', 'size', 'executed_at',
                    'sale_cond_codes', 'market_center', 'nbbo_ask', 'nbbo_bid',
                    'premium', 'collection_time'
                ]
                values = [tuple(row) for row in trades[columns].values]

                # Insert trades using execute_values for better performance
                execute_values(
                    cur,
                    """
                    INSERT INTO trading.darkpool_trades (
                        tracking_id, symbol, price, size, executed_at,
                        sale_cond_codes, market_center, nbbo_ask, nbbo_bid,
                        premium, collection_time
                    ) VALUES %s
                    ON CONFLICT (tracking_id) DO NOTHING
                    """,
                    values
                )
                self.db_conn.commit()
                self.logger.info(f"Successfully saved {len(trades)} trades to database")
        except Exception as e:
            if not self.db_conn.closed:
                self.db_conn.rollback()
            self.logger.error(f"Error saving trades to database: {str(e)}")
            raise

    def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        try:
            now = datetime.now(self.market_tz)
            current_time = now.time()
            current_day = now.weekday()

            # Check if it's a weekday
            if current_day >= 5:  # 5 = Saturday, 6 = Sunday
                return False

            # Check if current time is within market hours
            market_open = time(9, 30)  # 9:30 AM
            market_close = time(16, 0)  # 4:00 PM

            return market_open <= current_time < market_close
        except Exception as e:
            self.logger.error(f"Error checking market hours: {str(e)}")
            return False

    def get_next_market_open(self) -> datetime:
        """Get the next market open time."""
        try:
            now = datetime.now(self.market_tz)
            current_time = now.time()
            current_day = now.weekday()

            # Start with today's market open time
            next_open = datetime.combine(now.date(), MARKET_OPEN)
            next_open = self.market_tz.localize(next_open)

            # If we're past today's market open or it's a weekend,
            # move to the next business day
            if current_time >= MARKET_OPEN or current_day >= 5:
                days_to_add = 1
                if current_day == 5:  # Saturday
                    days_to_add = 2
                elif current_day == 6:  # Sunday
                    days_to_add = 1
                next_open += timedelta(days=days_to_add)

            return next_open
        except Exception as e:
            self.logger.error(f"Error calculating next market open: {str(e)}")
            # Return next day at market open as a fallback
            next_day = datetime.now(self.market_tz) + timedelta(days=1)
            return datetime.combine(next_day.date(), MARKET_OPEN)

    def run(self) -> None:
        """Run the collector continuously."""
        try:
            while True:
                if self.is_market_open():
                    trades = self.collect_trades()
                    if not trades.empty:
                        self.save_trades_to_db(trades)
                    time_module.sleep(self.rate_limit)
                else:
                    next_open = self.get_next_market_open()
                    wait_time = (next_open - datetime.now(self.market_tz)).total_seconds()
                    self.logger.info(f"Market is closed. Waiting until {next_open} to resume collection")
                    time_module.sleep(wait_time)
        except KeyboardInterrupt:
            self.logger.info("Collector stopped by user")
        except Exception as e:
            self.logger.error(f"Error in collector run loop: {str(e)}")
            raise

    def __del__(self):
        """Clean up resources on deletion."""
        if hasattr(self, 'db_conn') and self.db_conn is not None and not self.db_conn.closed:
            self.db_conn.close()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Dark Pool Trade Collector')
    parser.add_argument('--historical', action='store_true',
                       help='Collect historical data instead of real-time')
    args = parser.parse_args()

    try:
        collector = DarkPoolCollector()
        collector.connect_db()

        if args.historical:
            historical_collect()
        else:
            collector.run()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 