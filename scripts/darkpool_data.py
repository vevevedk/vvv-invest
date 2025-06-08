#!/usr/bin/env python3

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('darkpool_data.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from the correct .env file
env_file = os.getenv('ENV_FILE', '.env')
load_dotenv(env_file)
logger.info(f"Using environment file: {env_file}")

from flow_analysis.config.db_config import get_db_config
from flow_analysis.config.api_config import UW_API_TOKEN
from flow_analysis.config.watchlist import SYMBOLS
from collectors.utils.market_utils import is_market_open, get_next_market_open

class DarkPoolDataManager:
    def __init__(self):
        self.api_token = UW_API_TOKEN
        self.api_url_template = "https://api.unusualwhales.com/api/darkpool/{ticker}"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json"
        }
        
        # Set up requests session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504]  # HTTP status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Get database connection
        db_config = get_db_config()
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
        )
        
        # Create exports directory if it doesn't exist
        self.exports_dir = Path("exports")
        self.exports_dir.mkdir(exist_ok=True)

    def get_last_execution_time(self, symbol: str) -> datetime:
        """Get the most recent executed_at timestamp for a symbol from the database."""
        try:
            query = """
            SELECT MAX(executed_at) as last_execution
            FROM trading.darkpool_trades
            WHERE symbol = :symbol
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"symbol": symbol}).scalar()
                if result:
                    return result
                # If no trades found, return 24 hours ago
                return datetime.now(timezone.utc) - timedelta(hours=24)
        except Exception as e:
            logger.error(f"Error getting last execution time for {symbol}: {str(e)}")
            return datetime.now(timezone.utc) - timedelta(hours=24)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def fetch_trades(self, symbol: str, start_time: datetime, end_time: datetime) -> list:
        """Fetch trades for a symbol with retry logic."""
        api_url = self.api_url_template.format(ticker=symbol)
        params = {
            "limit": 500,
            "newer_than": start_time.isoformat(),
            "older_than": end_time.isoformat()
        }
        
        response = self.session.get(api_url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json().get("data", [])

    def collect_recent_trades(self):
        """Collect dark pool trades incrementally for all tickers in the watch list."""
        total_trades = 0
        try:
            # Check if market is open
            if not is_market_open():
                next_open = get_next_market_open()
                logger.info(f"Market is closed. Next market open: {next_open}")
                return 0

            end_time = datetime.now(timezone.utc)
            
            for symbol in SYMBOLS:
                # Get the last execution time for this symbol
                start_time = self.get_last_execution_time(symbol)
                logger.info(f"Fetching trades for {symbol} since {start_time.isoformat()}")
                
                try:
                    # Add delay between requests to avoid rate limits
                    time.sleep(2)  # 2 second delay between symbols
                    
                    trades = self.fetch_trades(symbol, start_time, end_time)
                    logger.info(f"Retrieved {len(trades)} trades for {symbol}")
                    
                    if trades:
                        df = pd.DataFrame(trades)
                        df = df.rename(columns={'ticker': 'symbol'})
                        df['collection_time'] = datetime.now(timezone.utc)
                        
                        with self.engine.connect() as conn:
                            for _, row in df.iterrows():
                                insert_query = """
                                INSERT INTO trading.darkpool_trades (
                                    tracking_id, symbol, price, size, volume, executed_at,
                                    premium, nbbo_bid, nbbo_ask, canceled, ext_hour_sold_codes,
                                    market_center, nbbo_bid_quantity, nbbo_ask_quantity,
                                    sale_cond_codes, trade_code, trade_settlement, collection_time
                                ) VALUES (
                                    :tracking_id, :symbol, :price, :size, :volume,
                                    :executed_at, :premium, :nbbo_bid, :nbbo_ask,
                                    :canceled, :ext_hour_sold_codes, :market_center,
                                    :nbbo_bid_quantity, :nbbo_ask_quantity,
                                    :sale_cond_codes, :trade_code, :trade_settlement,
                                    :collection_time
                                )
                                ON CONFLICT (tracking_id) DO NOTHING
                                """
                                try:
                                    conn.execute(text(insert_query), row.to_dict())
                                except Exception as e:
                                    logger.error(f"Error inserting trade {row.get('tracking_id', 'N/A')}: {str(e)}")
                                    continue
                            conn.commit()
                        total_trades += len(trades)
                except Exception as e:
                    logger.error(f"Error fetching trades for {symbol}: {str(e)}")
                    continue
                    
            logger.info(f"Saved {total_trades} new trades to database from all symbols")
            return total_trades
            
        except Exception as e:
            logger.error(f"Error collecting recent trades: {str(e)}")
            return total_trades

    def export_active_trades(self):
        """Export dark pool trades with DTE > 0."""
        try:
            # Query to get trades with DTE > 0
            query = """
            SELECT * FROM trading.darkpool_trades 
            WHERE executed_at > NOW() - INTERVAL '1 day'
            ORDER BY executed_at DESC
            """
            
            df = pd.read_sql(query, self.engine)
            
            if not df.empty:
                # Generate filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = self.exports_dir / f"darkpool_trades_active_{timestamp}.csv"
                
                # Export to CSV
                df.to_csv(filename, index=False)
                logger.info(f"Exported {len(df)} active trades to {filename}")
                return len(df)
            else:
                logger.warning("No active trades found")
                return 0
                
        except Exception as e:
            logger.error(f"Error exporting active trades: {str(e)}")
            return 0

def main():
    """Main entry point."""
    try:
        manager = DarkPoolDataManager()
        
        # Collect recent trades
        logger.info("Collecting recent dark pool trades...")
        num_collected = manager.collect_recent_trades()
        logger.info(f"Collected {num_collected} recent trades")
        
        # Export active trades
        logger.info("Exporting active dark pool trades...")
        num_exported = manager.export_active_trades()
        logger.info(f"Exported {num_exported} active trades")
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 