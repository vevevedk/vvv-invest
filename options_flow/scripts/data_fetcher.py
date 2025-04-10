"""
data_fetcher.py
This script fetches options data from Polygon API and saves it as CSV files.

Usage:
    python data_fetcher.py --date 2025-04-05
    python data_fetcher.py  # Uses today's date by default
"""

import os
import time
import argparse
import datetime
from pathlib import Path
import pandas as pd
import requests
from polygon import RESTClient
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import configuration
from options_flow.config.api_config import POLYGON_API_KEY, API_RATE_LIMIT, RAW_DATA_DIR, PROCESSED_DATA_DIR
from options_flow.config.watchlist import WATCHLIST

class OptionsDataFetcher:
    """Fetches options data from Polygon API"""
    
    def __init__(self, api_key):
        """Initialize the data fetcher"""
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v3"  # Updated to v3
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.rate_limit_remaining = API_RATE_LIMIT
        
        # Create data directories if they don't exist
        Path(RAW_DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(PROCESSED_DATA_DIR).mkdir(parents=True, exist_ok=True)
        
    def _handle_rate_limit(self):
        """Handle API rate limit by sleeping if necessary"""
        self.rate_limit_remaining -= 1
        if self.rate_limit_remaining <= 5:
            logger.info("Approaching rate limit, sleeping for 15 seconds")
            time.sleep(15)  # Reduced from 60 to 15 seconds
            self.rate_limit_remaining = API_RATE_LIMIT
    
    def fetch_options_contracts(self, ticker, expiration_date=None):
        """Fetch available options contracts for a given ticker"""
        self._handle_rate_limit()
        
        url = f"{self.base_url}/reference/options/contracts"
        params = {
            "underlying_ticker": ticker,
            "expiration_date": expiration_date,
            "limit": 1000
        }
        
        try:
            logger.info(f"Fetching options contracts for {ticker}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("results"):
                logger.warning(f"No contracts found for {ticker}")
                return []
                
            contracts = data.get("results", [])
            
            # Handle pagination
            next_url = data.get("next_url")
            while next_url:
                self._handle_rate_limit()
                response = requests.get(next_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                contracts.extend(data.get("results", []))
                next_url = data.get("next_url")
            
            logger.info(f"Found {len(contracts)} contracts for {ticker}")
            return contracts
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {ticker}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching options contracts for {ticker}: {e}")
            return []
    
    def fetch_options_trades(self, ticker, date):
        """Fetch options trades for a given ticker and date"""
        self._handle_rate_limit()
        
        # Format date as YYYY-MM-DD
        date_str = date.strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/trades/options/{ticker}"
        params = {
            "timestamp.gte": f"{date_str}T00:00:00Z",
            "timestamp.lt": f"{date_str}T23:59:59Z",
            "limit": 1000,
            "order": "timestamp"
        }
        
        try:
            logger.info(f"Fetching options trades for {ticker} on {date_str}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("results"):
                logger.warning(f"No trades found for {ticker} on {date_str}")
                return []
                
            trades = data.get("results", [])
            
            # Handle pagination
            next_url = data.get("next_url")
            while next_url:
                self._handle_rate_limit()
                response = requests.get(next_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                trades.extend(data.get("results", []))
                next_url = data.get("next_url")
            
            logger.info(f"Found {len(trades)} trades for {ticker} on {date_str}")
            return trades
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {ticker}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching options trades for {ticker} on {date_str}: {e}")
            return []
    
    def fetch_options_aggregates(self, ticker, date):
        """Fetch options aggregates for a given ticker and date"""
        self._handle_rate_limit()
        
        # Format date as YYYY-MM-DD
        date_str = date.strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/aggs/ticker/options/{ticker}"
        params = {
            "from": date_str,
            "to": date_str,
            "limit": 1000
        }
        
        try:
            logger.info(f"Fetching options aggregates for {ticker} on {date_str}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("results"):
                logger.warning(f"No aggregates found for {ticker} on {date_str}")
                return []
                
            aggregates = data.get("results", [])
            
            # Handle pagination
            next_url = data.get("next_url")
            while next_url:
                self._handle_rate_limit()
                response = requests.get(next_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                aggregates.extend(data.get("results", []))
                next_url = data.get("next_url")
            
            logger.info(f"Found {len(aggregates)} aggregates for {ticker} on {date_str}")
            return aggregates
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {ticker}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching options aggregates for {ticker} on {date_str}: {e}")
            return []
    
    def fetch_dark_pool_estimates(self, ticker, date):
        """Fetch dark pool estimates for a given ticker and date"""
        self._handle_rate_limit()
        
        # Format date as YYYY-MM-DD
        date_str = date.strftime("%Y-%m-%d")
        
        # First get aggregate data
        agg_url = f"{self.base_url}/aggs/ticker/{ticker}/range/1/minute/{date_str}/{date_str}"
        agg_params = {
            "adjusted": "true",
            "sort": "desc",
            "limit": 5000
        }
        
        try:
            logger.info(f"Fetching aggregate data for {ticker} on {date_str}")
            agg_response = requests.get(agg_url, headers=self.headers, params=agg_params)
            agg_response.raise_for_status()
            
            agg_data = agg_response.json()
            results = agg_data.get('results', [])
            
            if not results:
                logger.warning(f"No aggregate data found for {ticker} on {date_str}")
                return []
            
            # Calculate average volume
            volumes = [bar.get('v', 0) for bar in results]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            
            # Now get trades data
            trades_url = f"{self.base_url}/trades/{ticker}"
            trades_params = {
                "timestamp.gte": f"{date_str}T00:00:00Z",
                "timestamp.lt": f"{date_str}T23:59:59Z",
                "limit": 50000
            }
            
            logger.info(f"Fetching trades data for {ticker} on {date_str}")
            trades_response = requests.get(trades_url, headers=self.headers, params=trades_params)
            trades_response.raise_for_status()
            
            trades_data = trades_response.json()
            trades = trades_data.get('results', [])
            
            # Process trades to identify potential dark pool activity
            dark_pool_estimates = []
            
            for trade in trades:
                # Check for dark pool conditions
                conditions = trade.get('conditions', [])
                is_dark_pool = any(cond in ['4', '15', '16', '19', '21', '22', '23', '24', '25', '26', '27'] 
                                 for cond in conditions)
                
                if is_dark_pool:
                    dark_pool_estimates.append({
                        'ticker': ticker,
                        'price': trade.get('price'),
                        'size': trade.get('size'),
                        'timestamp': trade.get('timestamp'),
                        'conditions': conditions,
                        'value': trade.get('price', 0) * trade.get('size', 0)
                    })
            
            # If no dark pool trades found, estimate based on aggregate data
            if not dark_pool_estimates:
                for bar in results:
                    volume = bar.get('v', 0)
                    open_price = bar.get('o', 0)
                    close_price = bar.get('c', 0)
                    
                    # Simple heuristic for potential dark pool activity:
                    # 1. Volume significantly above average 
                    # 2. Small price change despite high volume
                    volume_ratio = volume / avg_volume if avg_volume > 0 else 0
                    price_change_pct = abs(close_price - open_price) / open_price * 100 if open_price > 0 else 0
                    
                    is_high_volume = volume_ratio > 1.5  # 50% above average volume
                    is_low_price_impact = price_change_pct < 0.1  # Less than 0.1% price change
                    
                    if is_high_volume and is_low_price_impact:
                        estimated_dark_pool_volume = round(volume * 0.6)  # Assume 60% of unusual volume is dark pool
                        
                        dark_pool_estimates.append({
                            'ticker': ticker,
                            'timestamp': bar.get('t'),
                            'total_volume': volume,
                            'estimated_dark_pool_volume': estimated_dark_pool_volume,
                            'open': open_price,
                            'close': close_price,
                            'price_change_pct': round(price_change_pct, 4),
                            'volume_ratio': round(volume_ratio, 2),
                            'is_estimate': True
                        })
            
            logger.info(f"Found {len(dark_pool_estimates)} dark pool estimates for {ticker} on {date_str}")
            return dark_pool_estimates
            
        except Exception as e:
            logger.error(f"Error fetching dark pool estimates for {ticker} on {date_str}: {e}")
            return []
    
    def fetch_all_data(self, date=None):
        """Fetch all data for watchlist tickers on a given date"""
        if date is None:
            date = datetime.datetime.now().date()
        
        date_str = date.strftime("%Y-%m-%d")
        logger.info(f"Fetching all data for {date_str}")
        
        # Create date-specific directories
        date_raw_dir = os.path.join(RAW_DATA_DIR, date_str)
        date_processed_dir = os.path.join(PROCESSED_DATA_DIR, date_str)
        Path(date_raw_dir).mkdir(parents=True, exist_ok=True)
        Path(date_processed_dir).mkdir(parents=True, exist_ok=True)
        
        # Fetch options contracts
        all_contracts = []
        for ticker in WATCHLIST:
            contracts = self.fetch_options_contracts(ticker)
            for contract in contracts:
                contract['underlying_ticker'] = ticker
            all_contracts.extend(contracts)
        
        if all_contracts:
            df_contracts = pd.DataFrame(all_contracts)
            df_contracts.to_csv(os.path.join(date_raw_dir, "options_contracts.csv"), index=False)
            logger.info(f"Saved {len(all_contracts)} options contracts")
        
        # Fetch options trades
        all_trades = []
        for ticker in WATCHLIST:
            trades = self.fetch_options_trades(ticker, date)
            for trade in trades:
                trade['underlying_ticker'] = ticker
            all_trades.extend(trades)
        
        if all_trades:
            df_trades = pd.DataFrame(all_trades)
            df_trades.to_csv(os.path.join(date_raw_dir, "options_trades.csv"), index=False)
            logger.info(f"Saved {len(all_trades)} options trades")
        
        # Fetch options aggregates
        all_aggregates = []
        for ticker in WATCHLIST:
            aggregates = self.fetch_options_aggregates(ticker, date)
            for agg in aggregates:
                agg['underlying_ticker'] = ticker
            all_aggregates.extend(aggregates)
        
        if all_aggregates:
            df_aggregates = pd.DataFrame(all_aggregates)
            df_aggregates.to_csv(os.path.join(date_raw_dir, "options_aggregates.csv"), index=False)
            logger.info(f"Saved {len(all_aggregates)} options aggregates")
        
        # Fetch dark pool estimates
        all_estimates = []
        for ticker in WATCHLIST:
            estimates = self.fetch_dark_pool_estimates(ticker, date)
            for est in estimates:
                est['ticker'] = ticker
            all_estimates.extend(estimates)
        
        if all_estimates:
            df_estimates = pd.DataFrame(all_estimates)
            df_estimates.to_csv(os.path.join(date_raw_dir, "dark_pool_estimates.csv"), index=False)
            logger.info(f"Saved {len(all_estimates)} dark pool estimates")
        
        logger.info(f"Completed data fetching for {date_str}")
        
        return {
            'contracts': all_contracts,
            'trades': all_trades,
            'aggregates': all_aggregates,
            'dark_pool': all_estimates
        }


def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(description="Fetch options data from Polygon API")
    parser.add_argument("--date", help="Date to fetch data for (YYYY-MM-DD)")
    args = parser.parse_args()
    
    if args.date:
        date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        date = datetime.datetime.now().date()
    
    fetcher = OptionsDataFetcher(POLYGON_API_KEY)
    fetcher.fetch_all_data(date)


if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)
    main()
