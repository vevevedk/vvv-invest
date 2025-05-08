#!/usr/bin/env python3

import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import pytz
from typing import Optional, Dict, List, Tuple, Union
import psycopg2
from psycopg2.extras import execute_values
import requests
import argparse
import json
from collections import deque
from threading import Lock
import numpy as np
from scipy.stats import norm
import math
from sqlalchemy import create_engine

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, 
    OPTION_CONTRACTS_ENDPOINT, OPTION_FLOW_ENDPOINT,
    EXPIRY_BREAKDOWN_ENDPOINT, DEFAULT_HEADERS, 
    REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME
from flow_analysis.config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS, MARKET_HOLIDAYS, EASTERN

# Constants
MIN_PREMIUM = 25000  # Increased minimum premium to $25k to focus on significant flows
BATCH_SIZE = 100  # Number of records to insert at once

# Rate limiting constants
MAX_REQUESTS_PER_MINUTE = 60  # Maximum requests per minute
MAX_REQUESTS_PER_HOUR = 3000  # Maximum requests per hour
RATE_LIMIT_WINDOW = 3600  # Time window for rate limiting (1 hour)
MIN_REQUEST_INTERVAL = 1.0  # Minimum time between requests in seconds
BACKOFF_FACTOR = 2  # Factor to increase backoff time
MAX_BACKOFF = 300  # Maximum backoff time in seconds (5 minutes)
REQUEST_HISTORY_SIZE = 3600  # Size of request history (1 hour worth of seconds)

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "options_flow_collector.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection setup
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060'
}

# Create database URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Create engine with SSL required
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'sslmode': 'require'
    }
)

# Calculate timestamp for 24 hours ago
twenty_four_hours_ago = datetime.now() - timedelta(hours=24)

# Query dark pool trades from last 24 hours with enhanced metrics
query = """
SELECT 
    t.*,
    date_trunc('hour', t.executed_at) as trade_hour,
    t.price - t.nbbo_bid as price_impact,
    (t.price - t.nbbo_bid) / t.nbbo_bid as price_impact_pct,
    CASE 
        WHEN t.size >= 10000 THEN 'Block Trade'
        WHEN t.premium >= 0.02 THEN 'High Premium'
        ELSE 'Regular'
    END as trade_type,
    count(*) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as trades_per_hour,
    sum(t.size) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as volume_per_hour
FROM trading.darkpool_trades t
WHERE t.executed_at >= :cutoff_time
ORDER BY t.executed_at DESC
"""

# Fetch trades
print("Fetching dark pool trades from last 24 hours...")
trades_df = pd.read_sql_query(
    query, 
    engine, 
    params={'cutoff_time': twenty_four_hours_ago}
)

# Convert timestamp columns
trades_df['executed_at'] = pd.to_datetime(trades_df['executed_at'])
trades_df['collection_time'] = pd.to_datetime(trades_df['collection_time'])
trades_df['trade_hour'] = pd.to_datetime(trades_df['trade_hour'])

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Generate filename with current timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'data/darkpool_trades_24h_{timestamp}.csv'

# Save to CSV
trades_df.to_csv(filename, index=False)
print(f"\nSaved {len(trades_df)} trades to {filename}")

# Print summary statistics
print("\nTrade summary by symbol:")
print(trades_df.groupby('symbol').agg({
    'size': ['count', 'sum', 'mean'],
    'premium': ['mean', 'max'],
    'price_impact_pct': 'mean'
}).round(2))

print("\nDate range of trades:")
print(f"Earliest trade: {trades_df['executed_at'].min()}")
print(f"Latest trade: {trades_df['executed_at'].max()}")
print(f"Total number of trades: {len(trades_df)}")
print(f"Total volume: {trades_df['size'].sum():,.0f}")

# Additional time-based analysis
print("\nHourly trade distribution:")
hourly_stats = trades_df.groupby(trades_df['executed_at'].dt.hour).agg({
    'size': ['count', 'sum'],
    'premium': 'mean'
}).round(2)
print(hourly_stats)

class RateLimiter:
    def __init__(self):
        self.request_times = deque(maxlen=REQUEST_HISTORY_SIZE)
        self.lock = Lock()
        self.current_backoff = MIN_REQUEST_INTERVAL
        self.last_request_time = 0
        
    def _clean_old_requests(self, current_time: float) -> None:
        """Remove requests older than the rate limit window"""
        while self.request_times and (current_time - self.request_times[0]) > RATE_LIMIT_WINDOW:
            self.request_times.popleft()
            
    def _get_requests_in_window(self, window: int, current_time: float) -> int:
        """Get number of requests in the specified window"""
        return sum(1 for t in self.request_times if current_time - t <= window)
        
    def wait_if_needed(self) -> None:
        """Wait if necessary to comply with rate limits"""
        with self.lock:
            current_time = time.time()
            self._clean_old_requests(current_time)
            
            # Check rate limits
            requests_last_minute = self._get_requests_in_window(60, current_time)
            requests_last_hour = len(self.request_times)
            
            if requests_last_minute >= MAX_REQUESTS_PER_MINUTE or requests_last_hour >= MAX_REQUESTS_PER_HOUR:
                sleep_time = max(
                    60 - (current_time - self.request_times[-MAX_REQUESTS_PER_MINUTE] if requests_last_minute >= MAX_REQUESTS_PER_MINUTE else 0),
                    3600 - (current_time - self.request_times[0] if requests_last_hour >= MAX_REQUESTS_PER_HOUR else 0)
                )
                logger.warning(f"Rate limit approaching, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
            # Ensure minimum interval between requests
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.current_backoff:
                time.sleep(self.current_backoff - time_since_last)
            
            # Record this request
            self.request_times.append(time.time())
            self.last_request_time = time.time()
            
    def update_backoff(self, success: bool) -> None:
        """Update backoff time based on request success"""
        if success:
            # On success, gradually reduce backoff time
            self.current_backoff = max(MIN_REQUEST_INTERVAL, self.current_backoff / BACKOFF_FACTOR)
        else:
            # On failure, increase backoff time
            self.current_backoff = min(MAX_BACKOFF, self.current_backoff * BACKOFF_FACTOR)
            logger.warning(f"Increased backoff time to {self.current_backoff:.2f} seconds")

class OptionsFlowCollector:
    """Collects options flow data for specified symbols."""
    
    def __init__(self, db_config: Dict[str, str], api_key: str):
        """Initialize the options flow collector.
        
        Args:
            db_config: Database configuration dictionary
            api_key: API key for data access
        """
        if not api_key:
            raise ValueError("API key is required")
            
        self.db_config = db_config
        self.api_key = api_key
        self.eastern = pytz.timezone('US/Eastern')
        
        # Constants for filtering options
        self.MIN_VOLUME = 5  # Reduced from 10
        self.MIN_OPEN_INTEREST = 25  # Reduced from 50
        self.MAX_DTE = 60  # Increased from 45
        self.MIN_DELTA = 0.02  # Reduced from 0.05
        self.MAX_BID_ASK_SPREAD_PCT = 0.35  # Increased from 0.25
        
        # Rate limiting constants
        self.REQUESTS_PER_MINUTE = 60
        self.REQUEST_INTERVAL = 60.0 / self.REQUESTS_PER_MINUTE
        self.last_request_time = 0
        
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.rate_limiter = RateLimiter()
        
        # Initialize database connection
        self.db_conn = None
        self.connect_db()
        
        # Set up logging
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
            log_dir / "options_flow_collector.log",
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
                self.db_conn = psycopg2.connect(**self.db_config)
                self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise

    def _rate_limit(self) -> None:
        """Enforce rate limiting between API requests."""
        self.rate_limiter.wait_if_needed()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic."""
        max_retries = 3
        retry_count = 0
        backoff_time = 1.0  # Initial backoff time in seconds
        
        while retry_count < max_retries:
            try:
                self._rate_limit()
                self.logger.info(f"Making request to: {endpoint}")
                response = requests.get(
                    endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                data = response.json()
                self.rate_limiter.update_backoff(True)
                return data
            except (requests.exceptions.RequestException, ValueError) as e:
                retry_count += 1
                self.rate_limiter.update_backoff(False)
                if retry_count == max_retries:
                    self.logger.error(f"Failed to make request after {max_retries} retries: {str(e)}")
                    return None
                
                self.logger.warning(f"Request failed (attempt {retry_count}/{max_retries}): {str(e)}")
                time.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
        return None

    def get_expiry_breakdown(self, symbol: str) -> Optional[Dict]:
        """Get expiry breakdown for a symbol."""
        endpoint = f"{self.base_url}{EXPIRY_BREAKDOWN_ENDPOINT.format(ticker=symbol)}"
        return self._make_request(endpoint)

    def get_option_contracts(self, symbol: str) -> Optional[Dict]:
        """Get option contracts for a symbol."""
        endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT.format(ticker=symbol)}"
        return self._make_request(endpoint)

    def get_option_flow(self, flow_id: str) -> Optional[Dict]:
        """Get option flow details for a specific flow ID."""
        endpoint = f"{self.base_url}{OPTION_FLOW_ENDPOINT.format(id=flow_id)}"
        return self._make_request(endpoint)

    def _process_flow_data(self, flow_data: Dict) -> pd.DataFrame:
        """Process raw flow data into a DataFrame."""
        try:
            if not flow_data or 'data' not in flow_data:
                self.logger.warning("No flow data received")
                return pd.DataFrame()

            flows = pd.DataFrame(flow_data['data'])
            if flows.empty:
                return flows

            # Add collection timestamp
            flows['collection_time'] = datetime.now(self.eastern)

            # Calculate derived fields
            flows['premium'] = flows['price'] * flows['size']
            flows['dte'] = (pd.to_datetime(flows['expiration']) - pd.to_datetime(flows['executed_at'])).dt.days

            # Filter flows based on criteria
            flows = flows[
                (flows['premium'] >= MIN_PREMIUM) &
                (flows['dte'] <= self.MAX_DTE) &
                (flows['volume'] >= self.MIN_VOLUME) &
                (flows['open_interest'] >= self.MIN_OPEN_INTEREST) &
                (flows['delta'].abs() >= self.MIN_DELTA)
            ]

            # Calculate bid-ask spread percentage
            flows['bid_ask_spread_pct'] = (flows['ask'] - flows['bid']) / flows['bid']
            flows = flows[flows['bid_ask_spread_pct'] <= self.MAX_BID_ASK_SPREAD_PCT]

            # Log flow statistics
            self.logger.info(f"Processed {len(flows)} flows")
            self.logger.info(f"Flows by symbol: {flows['symbol'].value_counts().to_dict()}")
            self.logger.info(f"Average premium by symbol: {flows.groupby('symbol')['premium'].mean().to_dict()}")

            return flows
        except Exception as e:
            self.logger.error(f"Error processing flow data: {str(e)}")
            raise

    def save_flows_to_db(self, flows: pd.DataFrame) -> None:
        """Save processed flows to the database."""
        if flows.empty:
            self.logger.warning("No flows to save - DataFrame is empty")
            return

        try:
            if self.db_conn is None or self.db_conn.closed:
                self.logger.warning("Database connection is closed, reconnecting...")
                self.connect_db()

            with self.db_conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute("CREATE SCHEMA IF NOT EXISTS trading;")
                
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trading.options_flow (
                        id SERIAL PRIMARY KEY,
                        flow_id VARCHAR(50) NOT NULL UNIQUE,
                        symbol VARCHAR(10) NOT NULL,
                        strike NUMERIC NOT NULL,
                        expiration DATE NOT NULL,
                        option_type VARCHAR(4) NOT NULL,
                        price NUMERIC NOT NULL,
                        size INTEGER NOT NULL,
                        premium NUMERIC NOT NULL,
                        executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        volume INTEGER,
                        open_interest INTEGER,
                        delta NUMERIC,
                        gamma NUMERIC,
                        theta NUMERIC,
                        vega NUMERIC,
                        implied_volatility NUMERIC,
                        bid NUMERIC,
                        ask NUMERIC,
                        bid_ask_spread_pct NUMERIC,
                        dte INTEGER,
                        collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                self.db_conn.commit()
                self.logger.info("Database schema and table verified")

                # Prepare data for insertion
                columns = [
                    'flow_id', 'symbol', 'strike', 'expiration', 'option_type',
                    'price', 'size', 'premium', 'executed_at', 'volume',
                    'open_interest', 'delta', 'gamma', 'theta', 'vega',
                    'implied_volatility', 'bid', 'ask', 'bid_ask_spread_pct',
                    'dte', 'collection_time'
                ]
                
                # Filter columns that exist in the DataFrame
                existing_columns = [col for col in columns if col in flows.columns]
                values = [tuple(row) for row in flows[existing_columns].values]

                # Log the SQL we're about to execute
                self.logger.info(f"Executing insert with columns: {existing_columns}")
                self.logger.info(f"Number of rows to insert: {len(values)}")

                # Insert flows using execute_values for better performance
                execute_values(
                    cur,
                    f"""
                    INSERT INTO trading.options_flow (
                        {', '.join(existing_columns)}
                    ) VALUES %s
                    ON CONFLICT (flow_id) DO NOTHING
                    """,
                    values
                )
                self.db_conn.commit()
                
                # Verify the insert by counting rows
                cur.execute("""
                    SELECT symbol, COUNT(*) as count 
                    FROM trading.options_flow 
                    WHERE collection_time >= NOW() - INTERVAL '1 minute'
                    GROUP BY symbol
                """)
                recent_counts = dict(cur.fetchall())
                self.logger.info(f"Recent flows saved by symbol: {recent_counts}")
                
                self.logger.info(f"Successfully saved {len(flows)} flows to database")
        except Exception as e:
            if not self.db_conn.closed:
                self.db_conn.rollback()
            self.logger.error(f"Error saving flows to database: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            self.logger.error(f"Error details: {str(e)}")
            raise

    def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        try:
            # Get current time in Eastern timezone
            now = datetime.now(self.eastern)
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
                self.logger.info(f"Market is closed - current ET time: {current_time.strftime('%H:%M:%S')}")
            else:
                self.logger.info("Market is open")
            
            return is_open
        except Exception as e:
            self.logger.error(f"Error checking market hours: {str(e)}")
            return False

    def get_next_market_open(self) -> datetime:
        """Get the next market open time."""
        try:
            now = datetime.now(self.eastern)
            current_time = now.time()
            current_date = now.date()

            # Start with today's market open time
            next_open = datetime.combine(current_date, MARKET_OPEN)
            next_open = self.eastern.localize(next_open)

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
                next_open = self.eastern.localize(next_open)

            return next_open
        except Exception as e:
            self.logger.error(f"Error calculating next market open: {str(e)}")
            # Return next day at market open as a fallback
            next_day = datetime.now(self.eastern) + timedelta(days=1)
            return datetime.combine(next_day.date(), MARKET_OPEN)

    def run(self) -> None:
        """Run the collector continuously."""
        try:
            while True:
                if self.is_market_open():
                    for symbol in SYMBOLS:
                        try:
                            # Get expiry breakdown
                            expiry_data = self.get_expiry_breakdown(symbol)
                            if not expiry_data:
                                continue

                            # Get option contracts
                            contracts_data = self.get_option_contracts(symbol)
                            if not contracts_data:
                                continue

                            # Get option flow
                            flow_data = self.get_option_flow(symbol)
                            if not flow_data:
                                continue

                            # Process and save flows
                            flows = self._process_flow_data(flow_data)
                            if not flows.empty:
                                self.save_flows_to_db(flows)

                        except Exception as e:
                            self.logger.error(f"Error processing symbol {symbol}: {str(e)}")
                            continue

                    time.sleep(self.REQUEST_INTERVAL)
                else:
                    next_open = self.get_next_market_open()
                    wait_time = (next_open - datetime.now(self.eastern)).total_seconds()
                    self.logger.info(f"Market is closed. Waiting until {next_open} to resume collection")
                    time.sleep(min(wait_time, 300))  # Sleep at most 5 minutes between checks
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
    collector = OptionsFlowCollector(DB_CONFIG, UW_API_TOKEN)
    collector.run()

if __name__ == "__main__":
    main()
