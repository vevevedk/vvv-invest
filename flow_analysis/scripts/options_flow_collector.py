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
from typing import Optional, Dict, List, Tuple
import psycopg2
from psycopg2.extras import execute_values
import requests
import argparse
import json

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, 
    OPTION_CONTRACTS_ENDPOINT, OPTION_FLOW_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from config.db_config import DB_CONFIG, SCHEMA_NAME
from config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS

# Constants
MIN_PREMIUM = 25000  # Increased minimum premium to $25k to focus on significant flows
BATCH_SIZE = 100  # Number of records to insert at once

# Additional filtering constants
MIN_VOLUME = 50  # Minimum volume threshold
MIN_OPEN_INTEREST = 100  # Minimum open interest
MAX_DTE = 45  # Maximum days to expiration
MIN_DELTA = 0.15  # Minimum absolute delta value
MAX_BID_ASK_SPREAD_PCT = 0.15  # Maximum bid-ask spread as percentage of mid price

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

class OptionsFlowCollector:
    def __init__(self):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.last_request_time = 0
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
        max_retries = 3
        for attempt in range(max_retries):
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
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Max retries reached for endpoint: {endpoint}")
                    return None
                    
    def _filter_contracts(self, contracts: List[Dict]) -> List[Dict]:
        """Apply additional filtering to option contracts"""
        now = datetime.now(self.eastern)
        filtered_contracts = []
        
        for contract in contracts:
            try:
                # Calculate days to expiration
                expiry = datetime.strptime(contract['expiration_date'], '%Y-%m-%d').replace(tzinfo=self.eastern)
                dte = (expiry - now).days
                
                # Calculate bid-ask spread percentage
                bid = float(contract.get('bid', 0) or 0)
                ask = float(contract.get('ask', 0) or 0)
                mid_price = (bid + ask) / 2 if bid > 0 and ask > 0 else 0
                spread_pct = (ask - bid) / mid_price if mid_price > 0 else float('inf')
                
                # Apply filters
                if all([
                    dte <= MAX_DTE,  # Not too far out
                    int(contract.get('volume', 0) or 0) >= MIN_VOLUME,  # Sufficient volume
                    int(contract.get('open_interest', 0) or 0) >= MIN_OPEN_INTEREST,  # Sufficient open interest
                    abs(float(contract.get('delta', 0) or 0)) >= MIN_DELTA,  # Significant delta
                    spread_pct <= MAX_BID_ASK_SPREAD_PCT,  # Reasonable spread
                ]):
                    filtered_contracts.append(contract)
                    
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"Error filtering contract {contract.get('option_symbol', 'unknown')}: {str(e)}")
                continue
                
        logger.info(f"Filtered {len(contracts)} contracts down to {len(filtered_contracts)} contracts")
        return filtered_contracts
                    
    def get_option_contracts(self, symbol: str) -> List[Dict]:
        """Get active option contracts for a symbol"""
        endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker=symbol)
        params = {
            "exclude_zero_dte": True,  # Skip same-day expiry
            "vol_greater_oi": True     # Only contracts with volume > OI
        }
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return []
            
        # Apply additional filtering
        return self._filter_contracts(data["data"])
        
    def get_flow_data(self, contract_id: str) -> pd.DataFrame:
        """Get flow data for a specific option contract"""
        endpoint = f"{self.base_url}{OPTION_FLOW_ENDPOINT}".format(id=contract_id)
        params = {
            "min_premium": MIN_PREMIUM
        }
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return pd.DataFrame()
            
        # Log raw data for debugging
        logger.info(f"Raw API response structure: {json.dumps(data['data'][0] if data['data'] else {}, indent=2)}")
        
        # Convert to DataFrame and process
        df = pd.DataFrame(data["data"])
        if df.empty:
            return df
            
        # Log available columns
        logger.info(f"Available columns in API response: {df.columns.tolist()}")
        
        # Add calculated fields
        df['timestamp'] = pd.to_datetime(df['executed_at'])
        
        # Convert premium to numeric before comparison
        df['premium'] = pd.to_numeric(df['premium'], errors='coerce')
        df['is_significant'] = df['premium'] >= MIN_PREMIUM
        
        # Map API response columns to our expected columns
        column_mapping = {
            'ticker': 'symbol',
            'size': 'contract_size',
            'strike_price': 'strike',
            'option_type': 'option_type',
            'implied_volatility': 'implied_volatility',
            'delta': 'delta',
            'underlying_price': 'underlying_price',
            'expiration_date': 'expiry'
        }
        
        # Create missing columns with None/NaN if they don't exist
        for new_col in ['symbol', 'contract_size', 'strike', 'option_type', 
                       'implied_volatility', 'delta', 'underlying_price', 'expiry']:
            if new_col not in df.columns:
                df[new_col] = None
        
        # Map columns from API response
        for api_col, our_col in column_mapping.items():
            if api_col in df.columns:
                df[our_col] = df[api_col]
        
        # Ensure we have all required columns
        required_columns = [
            'symbol', 'timestamp', 'expiry', 'strike', 'option_type',
            'premium', 'contract_size', 'implied_volatility', 'delta',
            'underlying_price', 'is_significant'
        ]
        
        # Log actual columns for debugging
        logger.info(f"Final columns before selection: {df.columns.tolist()}")
        
        # Select only the columns we need, filling missing ones with None
        result_df = pd.DataFrame(columns=required_columns)
        for col in required_columns:
            if col in df.columns:
                result_df[col] = df[col]
            else:
                logger.warning(f"Missing column {col} in API response")
                result_df[col] = None
        
        return result_df
        
    def save_flow_signals(self, df: pd.DataFrame) -> None:
        """Save flow signals to database"""
        if df.empty:
            return
            
        try:
            with self.db_conn.cursor() as cur:
                # Prepare data for insertion
                values = [tuple(row) for row in df.itertuples(index=False)]
                
                # Insert query
                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.options_flow_signals (
                        symbol, timestamp, expiry, strike, option_type,
                        premium, contract_size, implied_volatility, delta,
                        underlying_price, is_significant
                    ) VALUES %s
                    ON CONFLICT (symbol, timestamp, strike, option_type) DO UPDATE
                    SET
                        premium = EXCLUDED.premium,
                        contract_size = EXCLUDED.contract_size,
                        implied_volatility = EXCLUDED.implied_volatility,
                        delta = EXCLUDED.delta,
                        underlying_price = EXCLUDED.underlying_price,
                        is_significant = EXCLUDED.is_significant
                """
                
                # Execute batch insert
                execute_values(cur, insert_query, values, page_size=BATCH_SIZE)
                self.db_conn.commit()
                
                logger.info(f"Successfully saved {len(df)} flow signals")
                
        except Exception as e:
            logger.error(f"Error saving flow signals: {str(e)}")
            self.db_conn.rollback()
            raise
            
    def update_market_sentiment(self, symbol: str, interval: str = '5min') -> None:
        """Update market sentiment metrics"""
        try:
            with self.db_conn.cursor() as cur:
                # Calculate sentiment metrics
                query = f"""
                    INSERT INTO {SCHEMA_NAME}.market_sentiment (
                        symbol, timestamp, interval,
                        call_volume, put_volume,
                        call_premium, put_premium,
                        net_delta, avg_iv,
                        bullish_flow, bearish_flow,
                        sentiment_score
                    )
                    SELECT
                        symbol,
                        date_trunc(%s, timestamp) as interval_timestamp,
                        %s as interval,
                        SUM(CASE WHEN option_type = 'call' THEN contract_size ELSE 0 END) as call_volume,
                        SUM(CASE WHEN option_type = 'put' THEN contract_size ELSE 0 END) as put_volume,
                        SUM(CASE WHEN option_type = 'call' THEN premium ELSE 0 END) as call_premium,
                        SUM(CASE WHEN option_type = 'put' THEN premium ELSE 0 END) as put_premium,
                        SUM(delta * contract_size) as net_delta,
                        AVG(implied_volatility) as avg_iv,
                        SUM(CASE WHEN option_type = 'call' AND is_significant THEN premium ELSE 0 END) as bullish_flow,
                        SUM(CASE WHEN option_type = 'put' AND is_significant THEN premium ELSE 0 END) as bearish_flow,
                        (SUM(CASE WHEN option_type = 'call' THEN premium ELSE 0 END) - 
                         SUM(CASE WHEN option_type = 'put' THEN premium ELSE 0 END)) /
                        NULLIF(SUM(premium), 0) as sentiment_score
                    FROM {SCHEMA_NAME}.options_flow_signals
                    WHERE symbol = %s
                    AND timestamp >= NOW() - INTERVAL '1 day'
                    GROUP BY symbol, date_trunc(%s, timestamp)
                    ON CONFLICT (symbol, timestamp, interval) DO UPDATE
                    SET
                        call_volume = EXCLUDED.call_volume,
                        put_volume = EXCLUDED.put_volume,
                        call_premium = EXCLUDED.call_premium,
                        put_premium = EXCLUDED.put_premium,
                        net_delta = EXCLUDED.net_delta,
                        avg_iv = EXCLUDED.avg_iv,
                        bullish_flow = EXCLUDED.bullish_flow,
                        bearish_flow = EXCLUDED.bearish_flow,
                        sentiment_score = EXCLUDED.sentiment_score
                """
                
                cur.execute(query, (interval, interval, symbol, interval))
                self.db_conn.commit()
                logger.info(f"Updated market sentiment for {symbol}")
                
        except Exception as e:
            logger.error(f"Error updating market sentiment: {str(e)}")
            self.db_conn.rollback()
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        current_time = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if current_time.weekday() >= 5:
            return False
            
        # Parse market hours
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
        
        return market_open <= current_time <= market_close
        
    def collect_flow(self, symbol: str) -> None:
        """Collect options flow data for a symbol"""
        logger.info(f"Collecting options flow for {symbol}")
        
        try:
            # Get active contracts
            contracts = self.get_option_contracts(symbol)
            logger.info(f"Found {len(contracts)} active contracts for {symbol}")
            
            all_flow_data = []
            for contract in contracts:
                contract_id = contract['option_symbol']
                flow_data = self.get_flow_data(contract_id)
                if not flow_data.empty:
                    all_flow_data.append(flow_data)
                    
            if all_flow_data:
                # Combine all flow data
                combined_flow = pd.concat(all_flow_data, ignore_index=True)
                
                # Save to database
                self.save_flow_signals(combined_flow)
                
                # Update sentiment metrics
                self.update_market_sentiment(symbol)
                
                logger.info(f"Successfully collected and processed flow data for {symbol}")
            else:
                logger.info(f"No flow data found for {symbol}")
                
        except Exception as e:
            logger.error(f"Error collecting flow for {symbol}: {str(e)}")
            raise
            
    def run(self):
        """Run one collection cycle"""
        if not self.is_market_open():
            logger.info("Market is closed")
            return
            
        logger.info("Starting options flow collection")
        start_time = time.time()
        
        for symbol in SYMBOLS:
            try:
                self.collect_flow(symbol)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue
                
        duration = time.time() - start_time
        logger.info(f"Collection cycle completed in {duration:.2f} seconds")
        
    def __del__(self):
        """Clean up database connection"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

def main():
    parser = argparse.ArgumentParser(description='Options Flow Collector')
    parser.add_argument('--symbol', type=str, help='Single symbol to collect')
    args = parser.parse_args()
    
    collector = OptionsFlowCollector()
    
    if args.symbol:
        # Collect for single symbol
        collector.collect_flow(args.symbol)
    else:
        # Run normal collection cycle
        collector.run()

if __name__ == "__main__":
    main()
