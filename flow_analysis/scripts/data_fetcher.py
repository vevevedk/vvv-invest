"""
Dark Pool Data Fetcher
Fetches and processes dark pool trade data from Unusual Whales API
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from pathlib import Path
from typing import Optional, List, Dict, Any
from requests.exceptions import RequestException

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DARKPOOL_TICKER_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT,
    REQUEST_RATE_LIMIT
)
from flow_analysis.config.watchlist import (
    SYMBOLS, BLOCK_SIZE_THRESHOLD, PREMIUM_THRESHOLD,
    PRICE_IMPACT_THRESHOLD, MARKET_OPEN, MARKET_CLOSE
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DarkPoolDataFetcher:
    def __init__(self):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.raw_data_dir = project_root / "data/raw/darkpool"
        self.processed_data_dir = project_root / "data/processed"
        self.last_request_time = 0
        
        # Create directories if they don't exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)

    def _rate_limit(self):
        """Implement rate limiting to prevent API throttling"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1.0 / REQUEST_RATE_LIMIT:
            time.sleep(1.0 / REQUEST_RATE_LIMIT - time_since_last_request)
        self.last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> Optional[Dict]:
        """Make an API request with retry logic"""
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
            except RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Max retries reached for endpoint: {endpoint}")
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

    def fetch_recent_trades(self, limit: int = 100) -> pd.DataFrame:
        """Fetch recent dark pool trades for monitored symbols"""
        all_trades = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        for symbol in SYMBOLS:
            endpoint = f"{self.base_url}{DARKPOOL_TICKER_ENDPOINT}/{symbol}"
            params = {
                "limit": min(limit, 100),
                "date": yesterday
            }
            
            logger.info(f"Fetching trades for {symbol}")
            data = self._make_request(endpoint, params)
            
            if data and self._validate_response(data):
                all_trades.extend(data["data"])
            else:
                logger.warning(f"No valid data received for {symbol}")
        
        return self._process_trades_data(all_trades)

    def fetch_ticker_trades(self, symbol: str, date: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch dark pool trades for a specific symbol"""
        endpoint = f"{self.base_url}{DARKPOOL_TICKER_ENDPOINT}/{symbol}"
        params = {"date": date.strftime("%Y-%m-%d")} if date else None
        
        logger.info(f"Fetching trades for {symbol}")
        data = self._make_request(endpoint, params)
        
        if data and self._validate_response(data):
            return self._process_trades_data(data["data"])
        return pd.DataFrame()

    def fetch_historical_trades(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch historical dark pool trades for a specific symbol"""
        all_trades = []
        current_date = start_date
        
        while current_date <= end_date:
            trades = self.fetch_ticker_trades(symbol, current_date)
            if not trades.empty:
                all_trades.append(trades)
            current_date += timedelta(days=1)
        
        return pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    def _process_trades_data(self, trades_data: List[Dict]) -> pd.DataFrame:
        """Process raw trades data into a DataFrame with derived fields"""
        if not trades_data:
            return pd.DataFrame()
            
        trades = pd.DataFrame(trades_data)
        
        # Add derived fields
        trades["timestamp"] = pd.to_datetime(trades["executed_at"])
        trades["date"] = trades["timestamp"].dt.date
        trades["premium"] = trades["price"].astype(float) * trades["size"].astype(float)
        trades["nbbo_mid"] = (trades["nbbo_ask"].astype(float) + trades["nbbo_bid"].astype(float)) / 2
        trades["price_impact"] = abs(trades["price"].astype(float) - trades["nbbo_mid"]) / trades["nbbo_mid"]
        
        # Flag significant trades
        trades["is_block_trade"] = trades["size"] >= BLOCK_SIZE_THRESHOLD
        trades["is_high_premium"] = trades["premium"] >= PREMIUM_THRESHOLD
        trades["is_price_impact"] = trades["price_impact"] >= PRICE_IMPACT_THRESHOLD
        
        return trades

    def save_trades(self, trades: pd.DataFrame, date: Optional[datetime] = None) -> None:
        """Save trades to CSV file"""
        if trades.empty:
            logger.warning("No trades to save")
            return
            
        if date is None:
            date = datetime.now().date()
            
        filename = f"darkpool_trades_{date.strftime('%Y%m%d')}.csv"
        filepath = self.raw_data_dir / filename
        
        trades.to_csv(filepath, index=False)
        logger.info(f"Saved {len(trades)} trades to {filepath}")

    def process_trades(self, trades: pd.DataFrame) -> pd.DataFrame:
        """Process trades to identify unusual activity"""
        if trades.empty:
            return pd.DataFrame()
            
        # Group by ticker and 5-minute intervals
        trades["interval"] = trades["timestamp"].dt.floor("5T")
        grouped = trades.groupby(["ticker", "interval"]).agg({
            "size": ["sum", "count"],
            "premium": ["sum", "mean"],
            "price_impact": "mean",
            "is_block_trade": "sum",
            "is_high_premium": "sum",
            "is_price_impact": "sum"
        }).reset_index()
        
        # Flatten column names
        grouped.columns = ["_".join(col).strip("_") for col in grouped.columns.values]
        
        return grouped

def main():
    fetcher = DarkPoolDataFetcher()
    
    # Fetch recent trades
    logger.info("Fetching recent dark pool trades...")
    trades = fetcher.fetch_recent_trades()
    
    if not trades.empty:
        # Save raw trades
        fetcher.save_trades(trades)
        
        # Process and analyze trades
        analyzed = fetcher.process_trades(trades)
        
        # Log summary
        logger.info("\nTrade Summary:")
        logger.info(f"Total Trades: {len(trades)}")
        logger.info(f"Block Trades: {trades['is_block_trade'].sum()}")
        logger.info(f"High Premium Trades: {trades['is_high_premium'].sum()}")
        logger.info(f"Price Impact Trades: {trades['is_price_impact'].sum()}")
        
        for symbol in SYMBOLS:
            symbol_trades = trades[trades["ticker"] == symbol]
            if not symbol_trades.empty:
                logger.info(f"\n{symbol} Summary:")
                logger.info(f"Total Volume: {symbol_trades['size'].sum():,.0f}")
                logger.info(f"Total Premium: ${symbol_trades['premium'].sum():,.2f}")
                logger.info(f"Average Trade Size: {symbol_trades['size'].mean():,.0f}")

if __name__ == "__main__":
    main() 