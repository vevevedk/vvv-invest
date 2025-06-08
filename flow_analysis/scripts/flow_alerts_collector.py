#!/usr/bin/env python3

import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
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
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
env_file = '.env'  # Default to .env
load_dotenv(env_file)
logger = logging.getLogger(__name__)
logger.info(f"Using environment file: {env_file}")

from flow_analysis.config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, 
    OPTION_CONTRACTS_ENDPOINT, OPTION_FLOW_ENDPOINT,
    EXPIRY_BREAKDOWN_ENDPOINT, DEFAULT_HEADERS, 
    REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.db_config import get_db_config, SCHEMA_NAME
from flow_analysis.config.watchlist import SYMBOLS
from collectors.utils.market_utils import is_market_open, get_next_market_open

# Constants
MIN_PREMIUM = 25000  # Minimum premium for significant flows
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
log_file = log_dir / "flow_alerts_collector.log"

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

class FlowAlertsCollector:
    """Collects flow alerts data for specified symbols."""
    
    def __init__(self, db_config: Dict[str, str], api_key: str):
        """Initialize the flow alerts collector.
        
        Args:
            db_config: Database configuration dictionary
            api_key: API key for data access
        """
        if not api_key:
            raise ValueError("API key is required")
            
        self.db_config = db_config
        self.api_key = api_key
        self.eastern = pytz.timezone('US/Eastern')
        
        # Set up logging first
        self.logger = self._setup_logger()
        
        # Constants for filtering alerts
        self.MIN_VOLUME = 5  # Minimum volume for significant flows
        self.MIN_OPEN_INTEREST = 25  # Minimum open interest
        self.MAX_DTE = 60  # Maximum days to expiration
        self.MIN_DELTA = 0.02  # Minimum delta
        self.MAX_BID_ASK_SPREAD_PCT = 0.35  # Maximum bid-ask spread percentage
        
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
        
    def _setup_logger(self) -> logging.Logger:
        """Set up and return a logger instance."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Create handlers
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "flow_alerts_collector.log"
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=5
        )
        console_handler = logging.StreamHandler()
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        console_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    def connect_db(self) -> None:
        """Establish database connection."""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def _rate_limit(self) -> None:
        """Apply rate limiting to API requests."""
        self.rate_limiter.wait_if_needed()
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with rate limiting and error handling."""
        self._rate_limit()
        
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=self.headers,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            self.rate_limiter.update_backoff(True)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.rate_limiter.update_backoff(False)
            self.logger.error(f"API request failed: {str(e)}")
            return None
            
    def get_flow_alerts(self, symbol: str) -> Optional[Dict]:
        """Get flow alerts for a specific symbol."""
        endpoint = "/option-trades/flow-alerts"  # Correct endpoint from API docs
        params = {
            "ticker_symbol": symbol,
            "min_premium": MIN_PREMIUM,  # Use our minimum premium constant
            "min_volume": self.MIN_VOLUME,
            "min_open_interest": self.MIN_OPEN_INTEREST,
            "max_dte": self.MAX_DTE,
            "limit": 200,  # Maximum allowed by API
            "is_call": True,  # Include calls
            "is_put": True,  # Include puts
            "is_ask_side": True,  # Include ask side
            "is_bid_side": True,  # Include bid side
            "all_opening": True  # Only include opening trades
        }
        return self._make_request(endpoint, params)
        
    def _process_alert_data(self, alert_data: Dict) -> pd.DataFrame:
        """Process raw alert data into a DataFrame."""
        try:
            if not alert_data or 'data' not in alert_data:
                self.logger.warning("No alert data received")
                return pd.DataFrame()

            alerts = pd.DataFrame(alert_data['data'])
            if alerts.empty:
                return alerts

            # Add collection timestamp
            alerts['collection_time'] = datetime.now(self.eastern)

            # Map fields exactly as they come from the API
            alerts['symbol'] = alerts['ticker']
            alerts['timestamp'] = pd.to_datetime(alerts['created_at']).dt.tz_convert('UTC')
            alerts['alert_type'] = alerts['alert_rule']
            alerts['price'] = alerts['price']
            alerts['size'] = alerts['total_size']
            alerts['premium'] = pd.to_numeric(alerts['total_premium'], errors='coerce')
            alerts['expiration'] = pd.to_datetime(alerts['expiry']).dt.tz_localize('UTC')
            alerts['strike'] = pd.to_numeric(alerts['strike'], errors='coerce')
            alerts['option_type'] = alerts['type']
            alerts['volume'] = alerts['volume']
            alerts['open_interest'] = alerts['open_interest']
            alerts['volume_oi_ratio'] = alerts['volume_oi_ratio']
            
            # Calculate additional fields
            alerts['dte'] = (alerts['expiration'] - alerts['timestamp']).dt.days
            
            # Calculate bid/ask spread percentage if available
            if 'total_ask_side_prem' in alerts and 'total_bid_side_prem' in alerts and 'total_premium' in alerts:
                alerts['total_ask_side_prem'] = pd.to_numeric(alerts['total_ask_side_prem'], errors='coerce')
                alerts['total_bid_side_prem'] = pd.to_numeric(alerts['total_bid_side_prem'], errors='coerce')
                alerts['total_premium'] = pd.to_numeric(alerts['total_premium'], errors='coerce')
                alerts['bid_ask_spread_pct'] = (
                    (alerts['total_ask_side_prem'] - alerts['total_bid_side_prem']) / 
                    alerts['total_premium'] * 100
                ).fillna(0)
            else:
                alerts['bid_ask_spread_pct'] = 0.0

            # Set default values for fields not provided by API
            alerts['delta'] = 0.0
            alerts['bid'] = 0.0
            alerts['ask'] = 0.0

            # Log alert statistics
            self.logger.info(f"Processed {len(alerts)} alerts")
            self.logger.info(f"Alerts by symbol: {alerts['symbol'].value_counts().to_dict()}")
            self.logger.info(f"Average premium by symbol: {alerts.groupby('symbol')['premium'].mean().to_dict()}")

            return alerts
        except Exception as e:
            self.logger.error(f"Error processing alert data: {str(e)}")
            raise
            
    def save_alerts_to_db(self, alerts: pd.DataFrame) -> None:
        """Save processed alerts to the database."""
        if alerts.empty:
            self.logger.warning("No alerts to save")
            return
            
        try:
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS flow_alerts (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                size INTEGER NOT NULL,
                premium DECIMAL(12,2) NOT NULL,
                expiration DATE NOT NULL,
                strike DECIMAL(10,2) NOT NULL,
                option_type VARCHAR(4) NOT NULL,
                delta DECIMAL(5,2) NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                bid DECIMAL(10,2) NOT NULL,
                ask DECIMAL(10,2) NOT NULL,
                bid_ask_spread_pct DECIMAL(5,2) NOT NULL,
                collection_time TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self.db_conn.cursor() as cur:
                cur.execute(create_table_query)
                self.db_conn.commit()
                
            # Prepare data for insertion
            alerts_data = alerts.to_dict('records')
            
            # Insert data in batches
            with self.db_conn.cursor() as cur:
                for i in range(0, len(alerts_data), BATCH_SIZE):
                    batch = alerts_data[i:i + BATCH_SIZE]
                    execute_values(
                        cur,
                        """
                        INSERT INTO flow_alerts (
                            symbol, timestamp, alert_type, price, size, premium,
                            expiration, strike, option_type, delta, volume,
                            open_interest, bid, ask, bid_ask_spread_pct,
                            collection_time
                        ) VALUES %s
                        """,
                        [(
                            alert['symbol'],
                            alert['timestamp'],
                            alert['alert_type'],
                            alert['price'],
                            alert['size'],
                            alert['premium'],
                            alert['expiration'],
                            alert['strike'],
                            alert['option_type'],
                            alert['delta'],
                            alert['volume'],
                            alert['open_interest'],
                            alert['bid'],
                            alert['ask'],
                            alert['bid_ask_spread_pct'],
                            alert['collection_time']
                        ) for alert in batch]
                    )
                    self.db_conn.commit()
                    
            self.logger.info(f"Successfully saved {len(alerts)} alerts to database")
            
        except Exception as e:
            self.logger.error(f"Error saving alerts to database: {str(e)}")
            self.db_conn.rollback()
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        return is_market_open()
        
    def get_next_market_open(self) -> datetime:
        """Get the next market open time."""
        return get_next_market_open()
        
    def collect(self, start_date=None, end_date=None) -> None:
        """Collect flow alerts for the specified date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Collecting flow alerts from {start_date} to {end_date}")
        
        try:
            for symbol in SYMBOLS:
                self.logger.info(f"Collecting flow alerts for {symbol}")
                
                # Get flow alerts
                alert_data = self.get_flow_alerts(symbol)
                if not alert_data:
                    self.logger.warning(f"No alert data received for {symbol}")
                    continue
                    
                # Process alerts
                alerts = self._process_alert_data(alert_data)
                if not alerts.empty:
                    # Filter by date range
                    alerts = alerts[
                        (alerts['timestamp'] >= start_date) &
                        (alerts['timestamp'] <= end_date)
                    ]
                    if not alerts.empty:
                        # Save to database
                        self.save_alerts_to_db(alerts)
                    
            self.logger.info("Flow alerts collection completed for all symbols")
        except Exception as e:
            self.logger.error(f"Error in flow alerts collection: {str(e)}")
            raise
            
    def run(self) -> None:
        """Run the flow alerts collector."""
        self.logger.info("Starting flow alerts collector")
        
        try:
            for symbol in SYMBOLS:
                self.logger.info(f"Collecting flow alerts for {symbol}")
                
                # Get flow alerts
                alert_data = self.get_flow_alerts(symbol)
                if not alert_data:
                    self.logger.warning(f"No alert data received for {symbol}")
                    continue
                    
                # Process alerts
                alerts = self._process_alert_data(alert_data)
                if not alerts.empty:
                    # Save to database
                    self.save_alerts_to_db(alerts)
                    
            self.logger.info("Flow alerts collection completed for all symbols")
        except Exception as e:
            self.logger.error(f"Error in flow alerts collector: {str(e)}")
            time.sleep(60)  # Sleep for 1 minute before retrying
            
    def __del__(self):
        """Clean up resources."""
        if self.db_conn:
            self.db_conn.close()
            
    def backfill(self, start_date=None, end_date=None, days=7):
        """Backfill flow alerts for the specified date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=days)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Backfilling flow alerts from {start_date} to {end_date}")
        self.collect(start_date=start_date, end_date=end_date)
            
def main():
    """Main entry point when script is run directly."""
    parser = argparse.ArgumentParser(description='Collect flow alerts data')
    parser.add_argument('--env', type=str, default='prod', help='Environment to use (local, prod)')
    parser.add_argument('--symbol', type=str, help='Symbol to collect data for')
    args = parser.parse_args()
    
    # Load environment variables for direct script execution
    env_file = f'.env.{args.env}'
    load_dotenv(env_file)
    
    # Initialize collector
    collector = FlowAlertsCollector(get_db_config(), UW_API_TOKEN)
    
    if args.symbol:
        collector.collect(symbol=args.symbol)
    else:
        collector.run()

if __name__ == "__main__":
    main() 