"""
Options Data Fetcher
Fetches options data and strike prices from Unusual Whales API
"""

import os
import sys
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
    UW_API_TOKEN, UW_BASE_URL, OPTIONS_CHAIN_ENDPOINT,
    OPTIONS_STRIKES_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT,
    REQUEST_RATE_LIMIT
)
from config.watchlist import SYMBOLS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptionsDataFetcher:
    def __init__(self):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.raw_data_dir = project_root / "data/raw/options"
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

    def fetch_strike_prices(self, symbol: str, expiration: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch available strike prices for a symbol"""
        if expiration is None:
            expiration = datetime.now() + timedelta(days=30)  # Default to next month
            
        endpoint = f"{self.base_url}{OPTIONS_STRIKES_ENDPOINT}/{symbol}"
        params = {
            "expiration": expiration.strftime("%Y-%m-%d")
        }
        
        logger.info(f"Fetching strike prices for {symbol}")
        data = self._make_request(endpoint, params)
        
        if data and "strikes" in data:
            strikes = pd.DataFrame({
                "symbol": symbol,
                "strike": data["strikes"],
                "expiration": expiration
            })
            return strikes
        return pd.DataFrame()

    def fetch_options_chain(self, symbol: str, expiration: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch options chain data for a symbol"""
        if expiration is None:
            expiration = datetime.now() + timedelta(days=30)  # Default to next month
            
        endpoint = f"{self.base_url}{OPTIONS_CHAIN_ENDPOINT}/{symbol}"
        params = {
            "expiration": expiration.strftime("%Y-%m-%d")
        }
        
        logger.info(f"Fetching options chain for {symbol}")
        data = self._make_request(endpoint, params)
        
        if data and "chain" in data:
            chain = pd.DataFrame(data["chain"])
            chain["symbol"] = symbol
            chain["expiration"] = expiration
            return chain
        return pd.DataFrame()

    def fetch_all_strikes(self, symbols: List[str] = None) -> pd.DataFrame:
        """Fetch strike prices for all symbols"""
        if symbols is None:
            symbols = SYMBOLS
            
        all_strikes = []
        for symbol in symbols:
            strikes = self.fetch_strike_prices(symbol)
            if not strikes.empty:
                all_strikes.append(strikes)
                
        return pd.concat(all_strikes, ignore_index=True) if all_strikes else pd.DataFrame()

    def save_strikes(self, strikes: pd.DataFrame, date: Optional[datetime] = None) -> None:
        """Save strike prices to CSV file"""
        if strikes.empty:
            logger.warning("No strikes to save")
            return
            
        if date is None:
            date = datetime.now()
            
        filename = f"strike_prices_{date.strftime('%Y%m%d')}.csv"
        filepath = self.raw_data_dir / filename
        
        strikes.to_csv(filepath, index=False)
        logger.info(f"Saved {len(strikes)} strikes to {filepath}")

def main():
    fetcher = OptionsDataFetcher()
    
    # Fetch strike prices for all symbols
    logger.info("Fetching strike prices...")
    strikes = fetcher.fetch_all_strikes()
    
    if not strikes.empty:
        # Save raw strikes
        fetcher.save_strikes(strikes)
        
        # Log summary
        logger.info("\nStrike Summary:")
        for symbol in SYMBOLS:
            symbol_strikes = strikes[strikes["symbol"] == symbol]
            if not symbol_strikes.empty:
                logger.info(f"\n{symbol} Summary:")
                logger.info(f"Total Strikes: {len(symbol_strikes)}")
                logger.info(f"Strike Range: {symbol_strikes['strike'].min():.2f} - {symbol_strikes['strike'].max():.2f}")

if __name__ == "__main__":
    main() 