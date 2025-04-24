#!/usr/bin/env python3

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import psycopg2
from psycopg2 import sql
from pathlib import Path
from threading import Lock
from collections import deque

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.watchlist import SYMBOLS, MARKET_OPEN, MARKET_CLOSE
from flow_analysis.config.api_config import (
    UW_BASE_URL, DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT,
    EXPIRY_BREAKDOWN_ENDPOINT, OPTION_CONTRACTS_ENDPOINT, OPTION_FLOW_ENDPOINT
)
from flow_analysis.db.connection import get_db_connection

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
MAX_RETRIES = 3
BATCH_SIZE = 1000
MIN_PREMIUM = 10000  # Minimum premium to collect ($10k)

class TokenBucket:
    """Token bucket algorithm for rate limiting"""
    def __init__(self, capacity: int, fill_rate: float):
        self.capacity = capacity  # Maximum number of tokens
        self.fill_rate = fill_rate  # Tokens per second
        self.tokens = capacity  # Current token count
        self.last_update = time.time()
        self.lock = Lock()
    
    def _add_tokens(self):
        """Add tokens based on time elapsed"""
        now = time.time()
        time_passed = now - self.last_update
        new_tokens = time_passed * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_update = now
    
    def consume(self, tokens: int = 1) -> float:
        """Consume tokens and return wait time if needed"""
        with self.lock:
            self._add_tokens()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            
            # Calculate wait time needed for enough tokens
            missing_tokens = tokens - self.tokens
            wait_time = missing_tokens / self.fill_rate
            return wait_time

class CircuitBreaker:
    """Circuit breaker pattern to prevent overwhelming the API"""
    def __init__(self, failure_threshold: int, reset_timeout: int, half_open_timeout: int):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        self.failures = deque(maxlen=failure_threshold)
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.last_state_change = time.time()
        self.lock = Lock()
    
    def record_failure(self):
        """Record a failure and potentially open the circuit"""
        with self.lock:
            now = time.time()
            self.failures.append(now)
            
            if (len(self.failures) >= self.failure_threshold and 
                (now - self.failures[0]) <= self.reset_timeout):
                self.state = "OPEN"
                self.last_state_change = now
                logger.warning("Circuit breaker opened due to too many failures")
    
    def record_success(self):
        """Record a success and potentially close the circuit"""
        with self.lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failures.clear()
                logger.info("Circuit breaker closed after successful request")
    
    def can_execute(self) -> bool:
        """Check if a request can be executed"""
        with self.lock:
            now = time.time()
            if self.state == "CLOSED":
                return True
            elif self.state == "OPEN":
                if now - self.last_state_change >= self.reset_timeout:
                    self.state = "HALF-OPEN"
                    self.last_state_change = now
                    logger.info("Circuit breaker entering half-open state")
                    return True
                return False
            else:  # HALF-OPEN
                if now - self.last_state_change >= self.half_open_timeout:
                    return True
                return False

class OptionsFlowCollector:
    def __init__(self):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.eastern = pytz.timezone('US/Eastern')
        self.db_conn = get_db_connection()
        self.last_request_time = 0
        
        # Initialize rate limiter (100 requests per minute = 1.67 per second)
        self.rate_limiter = TokenBucket(capacity=10, fill_rate=1.67)
        
        # Initialize circuit breaker (5 failures within 60s opens circuit for 300s)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            reset_timeout=60,
            half_open_timeout=300
        )
        
        self._validate_db_connection()
    
    def _validate_db_connection(self) -> None:
        """Validate database connection and permissions"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.execute("SELECT has_table_privilege('collector', 'trading.options_flow', 'INSERT')")
                if not cur.fetchone()[0]:
                    raise PermissionError("Collector user does not have INSERT permission on trading.options_flow")
        except psycopg2.Error as e:
            logger.error(f"Database connection validation failed: {str(e)}")
            raise
    
    def _rate_limit(self):
        """Implement rate limiting using token bucket algorithm"""
        wait_time = self.rate_limiter.consume()
        if wait_time > 0:
            logger.debug(f"Rate limiting: sleeping for {wait_time:.2f} seconds")
            time.sleep(wait_time)

    @retry(stop=stop_after_attempt(MAX_RETRIES),
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic and circuit breaker"""
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is open")
        
        try:
            self._rate_limit()
            logger.info(f"Making request to endpoint: {endpoint}")
            logger.info(f"Request params: {params}")
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            
            logger.info(f"Response status code: {response.status_code}")
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit hit. Waiting {retry_after} seconds before retrying...")
                self.circuit_breaker.record_failure()
                time.sleep(retry_after)
                raise requests.exceptions.HTTPError("Rate limit exceeded")
            
            response.raise_for_status()
            self.circuit_breaker.record_success()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response'):
                if e.response.status_code == 429:
                    self.circuit_breaker.record_failure()
                    logger.error("Rate limit exceeded. Consider reducing request frequency or contacting support for increased limits.")
                logger.error(f"Response text: {e.response.text}")
            raise
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Request failed: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            raise

    def _validate_flow_data(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Validate flow data before saving to database"""
        if df.empty:
            return True, "Empty dataframe"
        
        required_columns = [
            'symbol', 'strike', 'expiry', 'flow_type',
            'premium', 'contract_size', 'iv_rank', 'collected_at'
        ]
        
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"
        
        # Check data types
        try:
            df['strike'] = pd.to_numeric(df['strike'])
            df['premium'] = pd.to_numeric(df['premium'])
            df['contract_size'] = pd.to_numeric(df['contract_size'])
            df['iv_rank'] = pd.to_numeric(df['iv_rank'])
            df['collected_at'] = pd.to_datetime(df['collected_at'])
        except Exception as e:
            return False, f"Data type conversion failed: {str(e)}"
        
        # Check for invalid values
        if df['strike'].le(0).any():
            return False, "Invalid strike price (<= 0)"
        if df['premium'].lt(0).any():
            return False, "Invalid premium (< 0)"
        if df['contract_size'].le(0).any():
            return False, "Invalid contract size (<= 0)"
        if df['iv_rank'].lt(0).any() or df['iv_rank'].gt(100).any():
            return False, "Invalid IV rank (not between 0 and 100)"
        
        return True, "Validation successful"
    
    def _process_flow(self, flow_data: List[Dict]) -> pd.DataFrame:
        """Process raw flow data into a DataFrame"""
        if not flow_data:
            return pd.DataFrame()
        
        try:
            df = pd.DataFrame(flow_data)
            
            # Map API fields to our database schema
            df['symbol'] = df['underlying_symbol']
            df['strike'] = pd.to_numeric(df['strike'])
            df['flow_type'] = df['option_type']
            df['contract_size'] = df['size']
            df['iv_rank'] = pd.to_numeric(df['implied_volatility']) * 100  # Convert to percentage
            df['collected_at'] = pd.to_datetime(df['executed_at'])
            df['premium'] = pd.to_numeric(df['premium'])
            
            # Select final columns
            df = df[[
                'symbol',
                'strike',
                'expiry',
                'flow_type',
                'premium',
                'contract_size',
                'iv_rank',
                'collected_at'
            ]]
            
            return df
        except Exception as e:
            logger.error(f"Error processing flow data: {str(e)}")
            return pd.DataFrame()
    
    @retry(stop=stop_after_attempt(MAX_RETRIES),
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def _save_flow_to_db(self, df: pd.DataFrame) -> None:
        """Save processed flow data to database in batches"""
        if df.empty:
            return
        
        # Validate data before saving
        is_valid, validation_msg = self._validate_flow_data(df)
        if not is_valid:
            logger.error(f"Data validation failed: {validation_msg}")
            return
        
        try:
            with self.db_conn.cursor() as cur:
                # Prepare the insert statement
                insert_query = sql.SQL("""
                    INSERT INTO trading.options_flow (
                        symbol, strike, expiry, flow_type, premium,
                        contract_size, iv_rank, collected_at
                    ) VALUES %s
                    ON CONFLICT (symbol, strike, expiry, flow_type, collected_at)
                    DO UPDATE SET
                        premium = EXCLUDED.premium,
                        contract_size = EXCLUDED.contract_size,
                        iv_rank = EXCLUDED.iv_rank
                """)
                
                # Process in batches
                for i in range(0, len(df), BATCH_SIZE):
                    batch = df.iloc[i:i+BATCH_SIZE]
                    values = [tuple(row) for row in batch.itertuples(index=False)]
                    
                    # Execute batch insert
                    psycopg2.extras.execute_values(
                        cur,
                        insert_query,
                        values,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    
                    self.db_conn.commit()
                    logger.info(f"Inserted batch of {len(batch)} records")
            
            logger.info(f"Successfully inserted {len(df)} options flow records")
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}")
            self.db_conn.rollback()
            raise

    def get_expiry_dates(self, symbol: str, date: Optional[str] = None) -> List[str]:
        """Get all expiry dates for a symbol"""
        endpoint = f"{self.base_url}{EXPIRY_BREAKDOWN_ENDPOINT}".format(ticker=symbol)
        params = {"date": date} if date else {}
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return []
        
        return [exp["expires"] for exp in data["data"]]

    def get_option_contracts(self, symbol: str, expiry: str) -> List[str]:
        """Get all option contracts for a symbol and expiry date"""
        endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker=symbol)
        params = {
            "expiry": expiry,
            "exclude_zero_vol_chains": True  # Only get contracts with volume
        }
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return []
        
        return [contract["option_symbol"] for contract in data["data"]]

    def get_flow_data(self, contract_id: str, date: Optional[str] = None) -> pd.DataFrame:
        """Get flow data for a specific option contract"""
        endpoint = f"{self.base_url}{OPTION_FLOW_ENDPOINT}".format(id=contract_id)
        params = {
            "date": date if date else None,
            "min_premium": MIN_PREMIUM
        }
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return pd.DataFrame()
        
        return self._process_flow(data["data"])

    def collect_flow(self, historical_date: Optional[datetime] = None) -> pd.DataFrame:
        """Collect options flow data from API"""
        date_str = historical_date.strftime("%Y-%m-%d") if historical_date else None
        all_flow_data = []
        
        for symbol in SYMBOLS:
            logger.info(f"Collecting flow data for {symbol}")
            
            # Get expiry dates
            expiry_dates = self.get_expiry_dates(symbol, date_str)
            logger.info(f"Found {len(expiry_dates)} expiry dates for {symbol}")
            
            for expiry in expiry_dates:
                # Get option contracts
                contracts = self.get_option_contracts(symbol, expiry)
                logger.info(f"Found {len(contracts)} contracts for {symbol} expiry {expiry}")
                
                for contract in contracts:
                    # Get flow data
                    flow_data = self.get_flow_data(contract, date_str)
                    if not flow_data.empty:
                        all_flow_data.append(flow_data)
                        logger.info(f"Collected {len(flow_data)} flow records for contract {contract}")
        
        if all_flow_data:
            combined_flow = pd.concat(all_flow_data, ignore_index=True)
            self._save_flow_to_db(combined_flow)
            return combined_flow
        
        return pd.DataFrame()

    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        current_time = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if current_time.weekday() >= 5:
            logger.info(f"Market closed - weekend ({current_time.strftime('%A')})")
            return False
        
        # Check if it's within market hours
        market_open = current_time.replace(
            hour=int(MARKET_OPEN.split(':')[0]),
            minute=int(MARKET_OPEN.split(':')[1]),
            second=0,
            microsecond=0
        )
        market_close = current_time.replace(
            hour=int(MARKET_CLOSE.split(':')[0]),
            minute=int(MARKET_CLOSE.split(':')[1]),
            second=0,
            microsecond=0
        )
        
        is_open = market_open <= current_time <= market_close
        if not is_open:
            logger.info(f"Market closed - outside trading hours (Current: {current_time.strftime('%H:%M')} ET)")
        return is_open
    
    def get_next_market_open(self) -> datetime:
        """Get the next market open time"""
        current_time = datetime.now(self.eastern)
        
        # Start with current day's market open
        next_open = current_time.replace(
            hour=int(MARKET_OPEN.split(':')[0]),
            minute=int(MARKET_OPEN.split(':')[1]),
            second=0,
            microsecond=0
        )
        
        # If we're past today's market open, move to next business day
        if current_time >= next_open:
            next_open += timedelta(days=1)
        
        # Skip weekends
        while next_open.weekday() >= 5:
            next_open += timedelta(days=1)
        
        return next_open
    
    def run(self, collection_interval: int = 300, historical_date: Optional[datetime] = None):
        """Run the collector once, exiting if market is closed"""
        logger.info("Starting options flow collector...")
        
        if historical_date:
            historical_date = historical_date.replace(tzinfo=self.eastern)
            logger.info(f"Starting historical data collection for {historical_date.strftime('%Y-%m-%d')}")
            self.collect_flow(historical_date)
            return
        
        if not self.is_market_open():
            logger.info("Market is closed. Exiting.")
            return
        
        try:
            logger.info("Collecting options flow...")
            flow = self.collect_flow()
            if flow.empty:
                logger.info("No new options flow collected")
        except Exception as e:
            logger.error(f"Error in collector run: {str(e)}")
            raise

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Collect options flow data')
    parser.add_argument('--historical', action='store_true',
                      help='Collect historical data for a specific date')
    parser.add_argument('--date', type=str,
                      help='Date for historical data collection (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # Run collector
    collector = OptionsFlowCollector()
    
    if args.historical:
        if not args.date:
            historical_date = datetime.now(pytz.timezone('US/Eastern')) - timedelta(days=1)
        else:
            historical_date = datetime.strptime(args.date, '%Y-%m-%d')
        collector.run(historical_date=historical_date)
    else:
        collector.run()

if __name__ == '__main__':
    main() 