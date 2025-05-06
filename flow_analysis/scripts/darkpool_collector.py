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
from flow_analysis.config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS, MARKET_HOLIDAYS

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
            
        # Log response structure for debugging
        if response_data['data']:
            sample_trade = response_data['data'][0]
            self.logger.info(f"Sample trade keys: {list(sample_trade.keys())}")
            self.logger.info(f"Sample trade data types: {[(k, type(v)) for k, v in sample_trade.items()]}")
            
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
                self.logger.info("No trades data received")
                return trades

            initial_count = len(trades)
            self.logger.info(f"Processing {initial_count} trades")

            # Debug logging for QQQ trades
            qqq_trades = trades[trades['ticker'] == 'QQQ']
            if not qqq_trades.empty:
                self.logger.info(f"Found {len(qqq_trades)} QQQ trades in raw data")
                self.logger.info(f"QQQ trade sample: {qqq_trades.iloc[0].to_dict()}")
            else:
                self.logger.warning("No QQQ trades found in raw data")

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
                'nbbo_bid': 'nbbo_bid',
                'volume': 'volume',
                'premium': 'premium',
                'nbbo_ask_quantity': 'nbbo_ask_quantity',
                'nbbo_bid_quantity': 'nbbo_bid_quantity',
                'ext_hour_sold_codes': 'ext_hour_sold_codes',
                'trade_code': 'trade_code',
                'trade_settlement': 'trade_settlement',
                'canceled': 'canceled'
            }
            trades = trades.rename(columns=column_mapping)
            self.logger.info(f"After column mapping: {len(trades)} trades")

            # Filter for target symbols only
            trades = trades[trades['symbol'].isin(SYMBOLS)]
            self.logger.info(f"After symbol filtering: {len(trades)} trades")
            
            # Debug logging for QQQ trades after filtering
            qqq_trades = trades[trades['symbol'] == 'QQQ']
            if not qqq_trades.empty:
                self.logger.info(f"Found {len(qqq_trades)} QQQ trades after filtering")
                self.logger.info(f"QQQ trade sample after filtering: {qqq_trades.iloc[0].to_dict()}")
            else:
                self.logger.warning("No QQQ trades found after filtering")

            if len(trades) < initial_count:
                symbol_counts = trades['symbol'].value_counts()
                self.logger.info(f"Trades per symbol: {symbol_counts.to_dict()}")

            # Convert data types
            numeric_columns = ['price', 'size', 'nbbo_ask', 'nbbo_bid', 'volume', 'premium',
                             'nbbo_ask_quantity', 'nbbo_bid_quantity']
            for col in numeric_columns:
                if col in trades.columns:
                    trades[col] = pd.to_numeric(trades[col], errors='coerce')

            trades['executed_at'] = pd.to_datetime(trades['executed_at'], errors='coerce')
            if 'canceled' in trades.columns:
                trades['canceled'] = trades['canceled'].fillna(False)

            # Log any NaN values after conversion
            nan_counts = trades.isna().sum()
            if nan_counts.any():
                self.logger.warning(f"NaN values after conversion: {nan_counts[nan_counts > 0].to_dict()}")

            # Calculate derived fields if not present
            if 'premium' not in trades.columns:
                trades['premium'] = trades['price'] * trades['size']

            # Log trade statistics
            self.logger.info(f"Final trade counts by symbol: {trades['symbol'].value_counts().to_dict()}")
            self.logger.info(f"Average trade size by symbol: {trades.groupby('symbol')['size'].mean().to_dict()}")
            self.logger.info(f"Average premium by symbol: {trades.groupby('symbol')['premium'].mean().to_dict()}")

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
                        tracking_id BIGINT NOT NULL UNIQUE,
                        symbol VARCHAR(10) NOT NULL,
                        price NUMERIC NOT NULL,
                        size INTEGER NOT NULL,
                        volume NUMERIC,
                        premium NUMERIC,
                        executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        nbbo_ask NUMERIC,
                        nbbo_bid NUMERIC,
                        nbbo_ask_quantity INTEGER,
                        nbbo_bid_quantity INTEGER,
                        market_center VARCHAR(10),
                        sale_cond_codes TEXT,
                        ext_hour_sold_codes TEXT,
                        trade_code TEXT,
                        trade_settlement TEXT,
                        canceled BOOLEAN,
                        collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                self.db_conn.commit()

                # Add collection time
                trades['collection_time'] = datetime.now()

                # Prepare data for insertion
                columns = [
                    'tracking_id', 'symbol', 'price', 'size', 'volume', 'premium',
                    'executed_at', 'nbbo_ask', 'nbbo_bid', 'nbbo_ask_quantity',
                    'nbbo_bid_quantity', 'market_center', 'sale_cond_codes',
                    'ext_hour_sold_codes', 'trade_code', 'trade_settlement',
                    'canceled', 'collection_time'
                ]
                
                # Filter columns that exist in the DataFrame
                existing_columns = [col for col in columns if col in trades.columns]
                values = [tuple(row) for row in trades[existing_columns].values]

                # Insert trades using execute_values for better performance
                execute_values(
                    cur,
                    f"""
                    INSERT INTO trading.darkpool_trades (
                        {', '.join(existing_columns)}
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
            current_date = now.date()

            # Check if it's a weekday
            if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
                self.logger.info("Market is closed - weekend")
                return False

            # Check if it's a holiday
            if current_date in MARKET_HOLIDAYS:
                self.logger.info("Market is closed - holiday")
                return False

            # Check if current time is within market hours
            is_open = MARKET_OPEN <= current_time < MARKET_CLOSE
            
            if not is_open:
                self.logger.info(f"Market is closed - current ET time: {now.strftime('%H:%M:%S')}")
            
            return is_open
        except Exception as e:
            self.logger.error(f"Error checking market hours: {str(e)}")
            return False

    def get_next_market_open(self) -> datetime:
        """Get the next market open time."""
        try:
            now = datetime.now(self.market_tz)
            current_time = now.time()
            current_date = now.date()

            # Start with today's market open time
            next_open = datetime.combine(current_date, MARKET_OPEN)
            next_open = self.market_tz.localize(next_open)

            # If we're past today's market open or it's a weekend/holiday,
            # move to the next business day
            if current_time >= MARKET_OPEN or now.weekday() >= 5 or current_date in MARKET_HOLIDAYS:
                days_to_add = 1
                while True:
                    next_date = current_date + timedelta(days=days_to_add)
                    if next_date.weekday() < 5 and next_date not in MARKET_HOLIDAYS:
                        break
                    days_to_add += 1
                next_open = datetime.combine(next_date, MARKET_OPEN)
                next_open = self.market_tz.localize(next_open)

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
                    time_module.sleep(min(wait_time, 300))  # Sleep at most 5 minutes between checks
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
        collector.run()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 