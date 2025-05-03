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
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic and rate limiting"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Wait for rate limiter
                self.rate_limiter.wait_if_needed()
                
                # Make request
                response = requests.get(
                    endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                
                # Update rate limiter based on response
                if response.status_code == 429:  # Too Many Requests
                    self.rate_limiter.update_backoff(False)
                    if attempt < max_retries - 1:
                        logger.warning(f"Rate limited on attempt {attempt + 1}/{max_retries}, backing off...")
                        continue
                else:
                    self.rate_limiter.update_backoff(True)
                    
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(self.rate_limiter.current_backoff)
                else:
                    logger.error(f"Max retries reached for endpoint: {endpoint}")
                    return None
                    
    def _calculate_delta(self, contract: Dict, underlying_price: float) -> float:
        """Calculate the delta for an option contract.
        
        Args:
            contract: Option contract details
            underlying_price: Current price of the underlying asset
            
        Returns:
            float: Delta value
        """
        # Extract contract details
        option_type = contract.get('option_type', 'call')
        strike = float(contract.get('strike', 0))
        days_to_expiry = float(contract.get('days_to_expiry', 0))
        volatility = float(contract.get('volatility', 0))
        risk_free_rate = float(contract.get('risk_free_rate', 0))
        
        # Calculate time to expiry in years
        time_to_expiry = days_to_expiry / 365.0
        
        # Calculate d1 and d2 for Black-Scholes
        d1 = (np.log(underlying_price / strike) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)
        
        # Calculate delta based on option type
        if option_type.lower() == 'call':
            delta = norm.cdf(d1)
        else:  # put
            delta = -norm.cdf(-d1)
            
        return delta

    def _get_market_conditions(self) -> Dict[str, float]:
        """Get current market conditions for dynamic filtering.
        
        Returns:
            Dict containing market condition metrics:
            - vix: Current VIX level
            - spy_volume: Current SPY volume
            - spy_volume_ratio: Current SPY volume / average volume
        """
        try:
            # Get VIX data using option contracts endpoint
            vix_endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker='VIX')
            vix_response = self._make_request(vix_endpoint)
            vix_data = vix_response.get('data', [])
            vix = float(vix_data[0].get('implied_volatility', 0)) if vix_data else 0.0
            
            # Get SPY data using option contracts endpoint
            spy_endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker='SPY')
            spy_response = self._make_request(spy_endpoint)
            spy_data = spy_response.get('data', [])
            
            if spy_data:
                spy_volume = float(spy_data[0].get('volume', 0))
                open_interest = float(spy_data[0].get('open_interest', 0))
                volume_ratio = spy_volume / open_interest if open_interest > 0 else 0.0
            else:
                spy_volume = 0.0
                volume_ratio = 0.0
            
            return {
                'vix': vix,
                'spy_volume': spy_volume,
                'spy_volume_ratio': volume_ratio
            }
            
        except Exception as e:
            logger.error(f"Error getting market conditions: {str(e)}")
            return {
                'vix': 0.0,
                'spy_volume': 0.0,
                'spy_volume_ratio': 0.0
            }

    def _filter_contracts(self, contracts: List[Dict]) -> List[Dict]:
        """Filter option contracts based on dynamic criteria.
        
        Args:
            contracts: List of option contracts
            
        Returns:
            List of filtered contracts
        """
        # Get current market conditions
        market_conditions = self._get_market_conditions()
        logger.info(f"Market conditions for filtering: {market_conditions}")
        
        # Log initial contract count
        logger.info(f"Received {len(contracts)} contracts before filtering")
        
        # Log sample of first few contracts for inspection
        logger.info("Sample of first 3 contracts:")
        for i, contract in enumerate(contracts[:3]):
            logger.info(f"Contract {i+1}:")
            logger.info(f"- Symbol: {contract.get('option_symbol', 'unknown')}")
            logger.info(f"- Volume: {contract.get('volume', 'N/A')}")
            logger.info(f"- Open Interest: {contract.get('open_interest', 'N/A')}")
            logger.info(f"- Implied Volatility: {contract.get('implied_volatility', 'N/A')}")
            logger.info(f"- NBBO Bid: {contract.get('nbbo_bid', 'N/A')}")
            logger.info(f"- NBBO Ask: {contract.get('nbbo_ask', 'N/A')}")
            logger.info(f"- Expiry: {contract.get('expiry', 'N/A')}")
            logger.info(f"- Strike: {contract.get('strike', 'N/A')}")
            logger.info(f"- Type: {contract.get('option_type', 'N/A')}")
            logger.info(f"- Avg Price: {contract.get('avg_price', 'N/A')}")
            logger.info(f"- Last Price: {contract.get('last_price', 'N/A')}")
        
        # Adjust filtering criteria based on market conditions
        vix_multiplier = 1.0 + (market_conditions['vix'] - 20) / 100  # Adjust for VIX level
        volume_multiplier = 1.0 + (1.0 - market_conditions['spy_volume_ratio'])  # Adjust for market volume
        
        # Dynamic thresholds - further relaxed
        min_volume = 1
        min_oi = 5    # Reduced from 10
        max_spread = 1.0  # Increased from 0.5 to 100%
        min_iv = 0.01  # Minimum implied volatility
        
        logger.info(f"Filtering criteria:")
        logger.info(f"- Min Volume: {min_volume}")
        logger.info(f"- Min Open Interest: {min_oi}")
        logger.info(f"- Max Spread: {max_spread}")
        logger.info(f"- Min Implied Volatility: {min_iv}")
        
        filtered = []
        for contract in contracts:
            # Log contract details for debugging
            logger.debug(f"Processing contract: {contract.get('option_symbol', 'unknown')}")
            
            # Extract and convert required fields with fallbacks
            try:
                volume = float(contract.get('volume', 0))
                open_interest = float(contract.get('open_interest', 0))
                implied_vol = float(contract.get('implied_volatility', 0))
                nbbo_bid = float(contract.get('nbbo_bid', 0))
                nbbo_ask = float(contract.get('nbbo_ask', 0))
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting numeric fields for contract {contract.get('option_symbol', 'unknown')}: {str(e)}")
                continue
            
            # Skip if volume or open interest is too low
            if volume < min_volume:
                logger.debug(f"Skipping contract - volume {volume} < {min_volume}")
                continue
                
            if open_interest < min_oi:
                logger.debug(f"Skipping contract - OI {open_interest} < {min_oi}")
                continue
                
            # Skip if implied volatility is too low
            if implied_vol < min_iv:
                logger.debug(f"Skipping contract - implied vol {implied_vol} < {min_iv}")
                continue
                
            # Skip if bid-ask spread is too wide
            if nbbo_bid > 0 and nbbo_ask > 0:
                spread_pct = (nbbo_ask - nbbo_bid) / nbbo_bid
                if spread_pct > max_spread:
                    logger.debug(f"Skipping contract - spread {spread_pct:.2%} > {max_spread:.2%}")
                    continue
            elif nbbo_bid == 0 and nbbo_ask > 0:
                # Special case for zero bid - use last price if available
                last_price = float(contract.get('last_price', 0))
                if last_price > 0:
                    spread_pct = (nbbo_ask - last_price) / last_price
                    if spread_pct > max_spread:
                        logger.debug(f"Skipping contract - zero bid spread {spread_pct:.2%} > {max_spread:.2%}")
                        continue
                else:
                    logger.debug("Skipping contract - zero bid and no last price")
                    continue
                    
            # Add liquidity score
            contract['liquidity_score'] = self._calculate_liquidity_score(contract)
            
            # Skip if liquidity score is too low
            if contract['liquidity_score'] < 0.05:  # Reduced from 0.1
                logger.debug(f"Skipping contract - liquidity score {contract['liquidity_score']:.2f} < 0.05")
                continue
                
            filtered.append(contract)
            logger.debug("Contract passed all filters")
            
        logger.info(f"Filtered to {len(filtered)} contracts")
        return filtered

    def get_option_contracts(self, symbol: str, expiry: Optional[str] = None, date: Optional[datetime] = None, min_premium: Optional[float] = None) -> List[Dict]:
        """Get active option contracts for a symbol.
        
        Args:
            symbol: Symbol to get contracts for
            expiry: Optional expiry date to filter by (YYYY-MM-DD)
            date: Optional trading date to get historical data for
            min_premium: Optional minimum premium to filter trades by
            
        Returns:
            List of option contracts
        """
        endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker=symbol)
        params = {
            "exclude_zero_dte": False,  # Changed to include same-day expiry
            "vol_greater_oi": False     # Changed to include all contracts
        }
        
        # Add expiry filter if provided
        if expiry:
            params["expiry"] = expiry
            
        # Add date filter if provided
        if date:
            params["date"] = date.strftime("%Y-%m-%d")
            
        # Add min_premium filter if provided
        if min_premium is not None:
            params["min_premium"] = min_premium
            
        logger.info(f"Requesting contracts for {symbol} with params: {params}")
        data = self._make_request(endpoint, params)
        
        if not data:
            logger.warning("No data received from API")
            return []
            
        if "data" not in data:
            logger.warning(f"Unexpected API response format: {data}")
            return []
            
        # Log raw response for debugging
        logger.debug(f"Raw API response: {data}")
        
        # Extract contracts from response
        contracts = data["data"]
        
        # Parse option symbols to extract additional information
        for contract in contracts:
            option_symbol = contract.get('option_symbol', '')
            if option_symbol:
                # Parse option symbol format: SYMBOLYYMMDDC/PSTRIKE
                # Example: SPY250501C00562000
                try:
                    contract['expiry'] = f"20{option_symbol[3:5]}-{option_symbol[5:7]}-{option_symbol[7:9]}"
                    contract['option_type'] = 'call' if option_symbol[9] == 'C' else 'put'
                    contract['strike'] = float(option_symbol[10:]) / 1000
                except Exception as e:
                    logger.warning(f"Error parsing option symbol {option_symbol}: {str(e)}")
        
        # Apply additional filtering
        return self._filter_contracts(contracts)

    def get_expiry_dates(self, symbol: str, date: Optional[datetime] = None) -> List[str]:
        """Get active expiry dates for a symbol.
        
        Args:
            symbol: Symbol to get expiry dates for
            date: Optional trading date to get historical data for
            
        Returns:
            List of expiry dates in YYYY-MM-DD format
        """
        endpoint = f"{self.base_url}{EXPIRY_BREAKDOWN_ENDPOINT}".format(ticker=symbol)
        params = {}
        if date:
            params["date"] = date.strftime("%Y-%m-%d")
            
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            return []
            
        # Log the response structure for debugging
        logger.debug(f"Expiry breakdown response: {data}")
        
        # Extract expiry dates from the response
        expiry_dates = []
        for item in data["data"]:
            if isinstance(item, dict) and "expires" in item:
                expiry_dates.append(item["expires"])
            elif isinstance(item, str):
                # Some APIs might return just the dates directly
                expiry_dates.append(item)
                
        logger.info(f"Found {len(expiry_dates)} expiry dates for {symbol}: {expiry_dates}")
        return sorted(expiry_dates)

    def get_flow_data(self, contract_id: str, historical_date: Optional[datetime] = None, min_premium: Optional[float] = None) -> List[Dict]:
        """Get flow data for a specific contract.
        
        Args:
            contract_id: Option contract ID in ISO format (e.g., TSLA230526P00167500)
            historical_date: Optional date to get historical data for
            min_premium: Optional minimum premium to filter trades by
            
        Returns:
            List of flow data dictionaries with enhanced analysis
        """
        try:
            # Make API request
            endpoint = f"{self.base_url}{OPTION_FLOW_ENDPOINT}".format(id=contract_id)
            params = {}
            
            # Add date parameter if provided
            if historical_date:
                params["date"] = historical_date.strftime("%Y-%m-%d")
                
            # Add min_premium parameter if provided
            if min_premium is not None:
                params["min_premium"] = min_premium
                
            logger.info(f"Requesting flow data for contract {contract_id} with params: {params}")
            response = self._make_request(endpoint, params)
            
            if not response:
                logger.warning(f"No response received for contract {contract_id}")
                return []
                
            # Extract flow data
            flow_data = response.get('data', [])
            
            if not flow_data:
                logger.info(f"No flow data found for contract {contract_id}")
                return []
                
            logger.info(f"Found {len(flow_data)} flow records for contract {contract_id}")
            
            # Enhance flow data with analysis
            for flow in flow_data:
                # Calculate VWAP
                flow['vwap'] = self._calculate_vwap(flow.get('trades', []))
                
                # Analyze flow direction
                flow_direction = self._analyze_flow_direction(flow.get('trades', []))
                flow.update(flow_direction)
                
                # Analyze relative volume
                historical_volumes = self._get_historical_volumes(contract_id, flow['expiry'])
                relative_volume = self._analyze_relative_volume(flow['volume'], historical_volumes)
                flow.update(relative_volume)
                
                # Calculate liquidity score
                flow['liquidity_score'] = self._calculate_liquidity_score(flow)
            
            return flow_data
            
        except Exception as e:
            logger.error(f"Error getting flow data for contract {contract_id}: {str(e)}")
            return []

    def _calculate_vwap(self, trades: List[Dict]) -> float:
        """Calculate Volume Weighted Average Price for a list of trades.
        
        Args:
            trades: List of trade dictionaries with 'price' and 'size' keys
            
        Returns:
            float: VWAP value
        """
        if not trades:
            return 0.0
            
        total_volume = sum(trade['size'] for trade in trades)
        if total_volume == 0:
            return 0.0
            
        weighted_prices = sum(trade['price'] * trade['size'] for trade in trades)
        return weighted_prices / total_volume
        
    def _analyze_flow_direction(self, trades: List[Dict]) -> Dict[str, float]:
        """Analyze the direction of flow (buying vs selling pressure).
        
        Args:
            trades: List of trade dictionaries with 'price', 'size', and 'side' keys
            
        Returns:
            Dict containing flow metrics:
            - buy_volume: Total volume of buy trades
            - sell_volume: Total volume of sell trades
            - net_flow: Net flow (buy_volume - sell_volume)
            - flow_ratio: Ratio of buy volume to total volume
        """
        buy_volume = sum(trade['size'] for trade in trades if trade['side'] == 'buy')
        sell_volume = sum(trade['size'] for trade in trades if trade['side'] == 'sell')
        total_volume = buy_volume + sell_volume
        
        return {
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'net_flow': buy_volume - sell_volume,
            'flow_ratio': buy_volume / total_volume if total_volume > 0 else 0.0
        }
        
    def _analyze_relative_volume(self, current_volume: int, historical_volumes: List[int]) -> Dict[str, float]:
        """Analyze current volume relative to historical volumes.
        
        Args:
            current_volume: Current volume
            historical_volumes: List of historical volumes
            
        Returns:
            Dict containing relative volume metrics:
            - avg_volume: Average historical volume
            - volume_ratio: Current volume / average volume
            - percentile: Percentile of current volume in historical distribution
        """
        if not historical_volumes:
            return {
                'avg_volume': 0.0,
                'volume_ratio': 0.0,
                'percentile': 0.0
            }
            
        avg_volume = sum(historical_volumes) / len(historical_volumes)
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0.0
        
        # Calculate percentile
        sorted_volumes = sorted(historical_volumes)
        percentile = sum(1 for v in sorted_volumes if v <= current_volume) / len(sorted_volumes)
        
        return {
            'avg_volume': avg_volume,
            'volume_ratio': volume_ratio,
            'percentile': percentile
        }

    def collect_flow(self, symbol: str, historical_date: Optional[datetime] = None, min_premium: Optional[float] = None) -> None:
        """Collect flow data for a symbol.
        
        Args:
            symbol: Symbol to collect flow data for
            historical_date: Optional date to collect historical data for
            min_premium: Optional minimum premium to filter trades by
        """
        try:
            # Get expiry dates
            expiry_dates = self.get_expiry_dates(symbol, historical_date)
            if not expiry_dates:
                logger.info(f"No expiry dates found for {symbol}")
                return
                
            # Get option contracts for each expiry
            for expiry in expiry_dates:
                logger.info(f"Processing expiry {expiry} for {symbol}")
                contracts = self.get_option_contracts(symbol, expiry, historical_date, min_premium)
                if not contracts:
                    logger.info(f"No contracts found for {symbol} expiry {expiry}")
                    continue
                    
                logger.info(f"Found {len(contracts)} contracts for {symbol} expiry {expiry}")
                
                # Get flow data for each contract
                for contract in contracts:
                    contract_id = contract.get('option_symbol')
                    if not contract_id:
                        continue
                        
                    logger.info(f"Getting flow data for contract {contract_id}")
                    flow_data = self.get_flow_data(contract_id, historical_date, min_premium)
                    if not flow_data:
                        continue
                        
                    # Save flow signals
                    self.save_flow_signals(flow_data)
            
        except Exception as e:
            logger.error(f"Error collecting flow for {symbol}: {str(e)}")
            raise
        
    def save_flow_signals(self, signals: Union[pd.DataFrame, List[Dict]]) -> None:
        """Save flow signals to database.
        
        Args:
            signals: Flow signals to save (DataFrame or list of dictionaries)
        """
        try:
            # Convert list to DataFrame if necessary
            if isinstance(signals, list):
                df = pd.DataFrame(signals)
            else:
                df = signals
                
            if df.empty:
                logger.info("No signals to save")
                return
                
            # Save to database
            with self.db_conn.cursor() as cur:
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS flow_signals (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(10) NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        signal_type VARCHAR(20) NOT NULL,
                        signal_value FLOAT NOT NULL,
                        metadata JSONB
                    )
                """)
                
                # Insert signals
                execute_values(
                    cur,
                    """
                    INSERT INTO flow_signals (symbol, timestamp, signal_type, signal_value, metadata)
                    VALUES %s
                    """,
                    [
                        (
                            row['symbol'],
                            row['timestamp'],
                            row['signal_type'],
                            row['signal_value'],
                            json.dumps(row.get('metadata', {}))
                        )
                        for _, row in df.iterrows()
                    ]
                )
                
                self.db_conn.commit()
                logger.info(f"Saved {len(df)} flow signals to database")
                
        except Exception as e:
            logger.error(f"Error saving flow signals: {str(e)}")
            if self.db_conn:
                self.db_conn.rollback()
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open.
        
        Returns:
            bool: True if market is open, False otherwise
        """
        now = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return False
            
        # Check if it's a holiday
        if now.date() in [holiday.date() for holiday in MARKET_HOLIDAYS]:
            return False
            
        # Check if it's within market hours
        current_time = now.time()
        return MARKET_OPEN <= current_time <= MARKET_CLOSE
        
    def run(self, symbols: List[str], historical_date: Optional[datetime] = None, min_premium: Optional[float] = None) -> None:
        """Run the options flow collector for the specified symbols.
        
        Args:
            symbols: List of symbols to collect flow for
            historical_date: Optional date to collect historical data for
            min_premium: Optional minimum premium to filter trades by
        """
        for symbol in symbols:
            try:
                self.collect_flow(symbol, historical_date, min_premium)
            except Exception as e:
                logger.error(f"Error collecting flow for {symbol}: {str(e)}")
                continue
        
    def __del__(self):
        """Clean up database connection"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

    def _get_historical_volumes(self, symbol: str, expiry: str, lookback_days: int = 30) -> List[int]:
        """Get historical volumes for a symbol and expiry.
        
        Args:
            symbol: Symbol to get historical volumes for
            expiry: Option expiry date
            lookback_days: Number of days to look back
            
        Returns:
            List of historical volumes
        """
        try:
            query = """
                SELECT volume 
                FROM options_flow 
                WHERE symbol = %s 
                AND expiry = %s 
                AND timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY timestamp DESC
            """
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(query, (symbol, expiry, lookback_days))
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting historical volumes for {symbol} {expiry}: {str(e)}")
            return []
            
    def _calculate_liquidity_score(self, flow: Dict) -> float:
        """Calculate a liquidity score for an options flow.
        
        Args:
            flow: Flow data dictionary
            
        Returns:
            float: Liquidity score between 0 and 1
        """
        try:
            # Initialize score components
            volume_score = 0.0
            spread_score = 0.0
            open_interest_score = 0.0
            
            # Extract and convert numeric fields
            try:
                volume = float(flow.get('volume', 0))
                open_interest = float(flow.get('open_interest', 0))
                nbbo_bid = float(flow.get('nbbo_bid', 0))
                nbbo_ask = float(flow.get('nbbo_ask', 0))
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting numeric fields for liquidity score: {str(e)}")
                return 0.0
            
            # Volume score (0-0.4)
            volume_ratio = volume / open_interest if open_interest > 0 else 0.0
            volume_score = min(0.4, volume_ratio * 0.1)
            
            # Spread score (0-0.3)
            if nbbo_bid > 0:
                spread_pct = (nbbo_ask - nbbo_bid) / nbbo_bid
                spread_score = max(0.0, 0.3 - (spread_pct * 0.3))
                
            # Open interest score (0-0.3)
            oi_score = min(0.3, open_interest / 1000.0 * 0.1)
            
            # Calculate total score
            total_score = volume_score + spread_score + oi_score
            
            # Normalize to 0-1 range
            return min(1.0, max(0.0, total_score))
            
        except Exception as e:
            logger.error(f"Error calculating liquidity score: {str(e)}")
            return 0.0

    def health_check(self) -> Dict[str, bool]:
        """Perform health check of the collector.
        
        Returns:
            Dict containing health check results:
            - db_connected: Database connection status
            - api_accessible: API accessibility status
            - rate_limit_ok: Rate limit status
            - market_open: Market open status
        """
        health = {
            'db_connected': False,
            'api_accessible': False,
            'rate_limit_ok': False,
            'market_open': False
        }
        
        # Check database connection
        try:
            if self.db_conn and not self.db_conn.closed:
                with self.db_conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                health['db_connected'] = True
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            
        # Check API accessibility using option contracts endpoint
        try:
            endpoint = f"{self.base_url}{OPTION_CONTRACTS_ENDPOINT}".format(ticker='SPY')
            response = self._make_request(endpoint)
            health['api_accessible'] = response is not None and 'data' in response
        except Exception as e:
            logger.error(f"API health check failed: {str(e)}")
            
        # Check rate limit status
        try:
            requests_last_minute = self.rate_limiter._get_requests_in_window(60, time.time())
            health['rate_limit_ok'] = requests_last_minute < self.REQUESTS_PER_MINUTE
        except Exception as e:
            logger.error(f"Rate limit health check failed: {str(e)}")
            
        # Check market open status
        health['market_open'] = self.is_market_open()
        
        return health

def main():
    parser = argparse.ArgumentParser(description='Options Flow Collector')
    parser.add_argument('--symbol', type=str, help='Single symbol to collect')
    parser.add_argument('--health', action='store_true', help='Run health check only')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--min-premium', type=float, help='Minimum premium to filter trades by')
    args = parser.parse_args()
    
    # Set up debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    try:
        collector = OptionsFlowCollector(DB_CONFIG, UW_API_TOKEN)
        
        # Run health check if requested
        if args.health:
            health = collector.health_check()
            logger.info("Health check results:")
            for component, status in health.items():
                logger.info(f"{component}: {'OK' if status else 'FAILED'}")
            return
            
        # Check health before proceeding, but skip market_open check in debug mode
        health = collector.health_check()
        if not all(health.values()):
            logger.error("Health check failed. Issues found:")
            for component, status in health.items():
                if not status and (component != 'market_open' or not args.debug):
                    logger.error(f"- {component}")
            if not args.debug:
                return
            
        if args.symbol:
            # Collect for single symbol
            logger.info(f"Starting collection for {args.symbol}")
            try:
                collector.collect_flow(args.symbol, min_premium=args.min_premium)
                logger.info(f"Successfully collected data for {args.symbol}")
            except Exception as e:
                logger.error(f"Error collecting data for {args.symbol}: {str(e)}")
                raise
        else:
            # Run normal collection cycle
            logger.info("Starting collection cycle")
            try:
                collector.run(SYMBOLS, min_premium=args.min_premium)
                logger.info("Successfully completed collection cycle")
            except Exception as e:
                logger.error(f"Error during collection cycle: {str(e)}")
                raise
                
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        if 'collector' in locals():
            collector.__del__()

if __name__ == "__main__":
    main()
