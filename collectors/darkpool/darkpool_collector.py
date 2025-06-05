#!/usr/bin/env python3

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
import pytz
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/darkpool_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DarkPoolCollector:
    def __init__(self, api_key=None, db_url=None):
        """Initialize the collector with API key and database URL."""
        # Debug: Print environment variables
        env_file = os.getenv('ENV_FILE', '.env')
        logger.info(f"Loading environment from: {env_file}")
        load_dotenv(env_file)
        
        # Debug: Check if API token exists
        api_key = api_key or os.getenv('UW_API_TOKEN')
        if api_key:
            logger.info("API token found (length: %d)", len(api_key))
        else:
            logger.error("API token not found in environment variables")
            logger.error("Available environment variables: %s", 
                        [k for k in os.environ.keys() if k.startswith('UW_') or k.startswith('DB_')])
            
        if not api_key:
            raise ValueError("API token is required")
            
        self.api_key = api_key
        
        # Construct database URL from components
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_sslmode = os.getenv('DB_SSLMODE', 'disable')
        
        if not all([db_user, db_password, db_host, db_port, db_name]):
            logger.error("Missing required database configuration")
            logger.error("DB_USER: %s", db_user)
            logger.error("DB_PASSWORD: %s", "***" if db_password else None)
            logger.error("DB_HOST: %s", db_host)
            logger.error("DB_PORT: %s", db_port)
            logger.error("DB_NAME: %s", db_name)
            raise ValueError("Database configuration is incomplete")
            
        self.db_url = db_url or f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        logger.info(f"Using database: {db_host}:{db_port}/{db_name}")
            
        self.engine = create_engine(self.db_url)
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        })
        
        # Production settings
        self.symbols = ['SPY', 'QQQ', 'TSLA']  # Core symbols to track
        self.lookback_minutes = 10  # Look back 10 minutes to ensure no missed trades
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
    def _get_existing_data_ranges(self, symbol):
        """Get time ranges where data already exists for a symbol."""
        query = """
        SELECT 
            date_trunc('hour', executed_at) as hour_start,
            date_trunc('hour', executed_at) + interval '1 hour' as hour_end
        FROM trading.darkpool_trades 
        WHERE symbol = :symbol
        GROUP BY date_trunc('hour', executed_at)
        ORDER BY hour_start
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol})
            return [(row[0].replace(tzinfo=pytz.UTC), row[1].replace(tzinfo=pytz.UTC)) 
                   for row in result]
    
    def _is_time_range_covered(self, start_time, end_time, existing_ranges):
        """Check if a time range is already covered by existing data."""
        current = start_time
        while current < end_time:
            hour_end = current + timedelta(hours=1)
            if not any(r[0] <= current < r[1] for r in existing_ranges):
                return False
            current = hour_end
        return True
    
    def _fetch_trades(self, symbol, start_time, end_time):
        """Fetch trades for a symbol within a time range with retries."""
        params = {
            'newer_than': start_time.isoformat(),
            'older_than': end_time.isoformat(),
            'limit': 500
        }
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    f'https://api.unusualwhales.com/api/darkpool/{symbol}',
                    params=params
                )
                response.raise_for_status()
                return response.json().get('data', [])
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {str(e)}")
                time.sleep(self.retry_delay)
    
    def _save_trades(self, trades):
        """Save trades to database."""
        if not trades:
            return
            
        query = """
        INSERT INTO trading.darkpool_trades (
            tracking_id, symbol, price, size, volume, premium,
            executed_at, nbbo_ask, nbbo_bid, nbbo_ask_quantity,
            nbbo_bid_quantity, market_center, sale_cond_codes,
            ext_hour_sold_codes, trade_code, trade_settlement,
            canceled, collection_time
        ) VALUES (
            :tracking_id, :symbol, :price, :size, :volume, :premium,
            :executed_at, :nbbo_ask, :nbbo_bid, :nbbo_ask_quantity,
            :nbbo_bid_quantity, :market_center, :sale_cond_codes,
            :ext_hour_sold_codes, :trade_code, :trade_settlement,
            :canceled, :collection_time
        ) ON CONFLICT (tracking_id) DO NOTHING
        """
        
        with self.engine.connect() as conn:
            for trade in trades:
                try:
                    conn.execute(text(query), {
                        'tracking_id': trade['tracking_id'],
                        'symbol': trade['ticker'],
                        'price': float(trade['price']),
                        'size': int(trade['size']),
                        'volume': int(trade['volume']),
                        'premium': float(trade['premium']),
                        'executed_at': datetime.fromisoformat(trade['executed_at'].replace('Z', '+00:00')),
                        'nbbo_ask': float(trade['nbbo_ask']) if trade['nbbo_ask'] else None,
                        'nbbo_bid': float(trade['nbbo_bid']) if trade['nbbo_bid'] else None,
                        'nbbo_ask_quantity': int(trade['nbbo_ask_quantity']) if trade['nbbo_ask_quantity'] else None,
                        'nbbo_bid_quantity': int(trade['nbbo_bid_quantity']) if trade['nbbo_bid_quantity'] else None,
                        'market_center': trade['market_center'],
                        'sale_cond_codes': trade['sale_cond_codes'],
                        'ext_hour_sold_codes': trade['ext_hour_sold_codes'],
                        'trade_code': trade['trade_code'],
                        'trade_settlement': trade['trade_settlement'],
                        'canceled': trade['canceled'],
                        'collection_time': datetime.now(pytz.UTC)
                    })
                except Exception as e:
                    logger.error(f"Error saving trade {trade['tracking_id']}: {str(e)}")
            conn.commit()
    
    def collect_trades(self):
        """Collect recent dark pool trades."""
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(minutes=self.lookback_minutes)
        
        logger.info(f"Collecting trades from {start_time} to {end_time}")
        
        total_trades = 0
        for symbol in self.symbols:
            try:
                # Check existing data
                existing_ranges = self._get_existing_data_ranges(symbol)
                if self._is_time_range_covered(start_time, end_time, existing_ranges):
                    logger.info(f"Skipping {symbol} - data already exists for this period")
                    continue
                
                # Fetch and save trades
                trades = self._fetch_trades(symbol, start_time, end_time)
                if trades:
                    self._save_trades(trades)
                    total_trades += len(trades)
                    logger.info(f"Saved {len(trades)} trades for {symbol}")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue
        
        logger.info(f"Collection complete. Total trades saved: {total_trades}")
        return total_trades
    
    def backfill_trades(self, hours=24):
        """Backfill historical trades."""
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(hours=hours)
        
        logger.info(f"Starting backfill from {start_time} to {end_time}")
        
        total_trades = 0
        for symbol in tqdm(self.symbols, desc="Processing symbols"):
            try:
                # Check existing data
                existing_ranges = self._get_existing_data_ranges(symbol)
                if self._is_time_range_covered(start_time, end_time, existing_ranges):
                    logger.info(f"Skipping {symbol} - data already exists for this period")
                    continue
                
                # Process in 2-hour windows to manage API credits
                current_end = end_time
                while current_end > start_time:
                    current_start = current_end - timedelta(hours=2)
                    if current_start < start_time:
                        current_start = start_time
                    
                    # Check if this window is already covered
                    if self._is_time_range_covered(current_start, current_end, existing_ranges):
                        current_end = current_start
                        continue
                    
                    # Fetch and save trades for this window
                    trades = self._fetch_trades(symbol, current_start, current_end)
                    if trades:
                        self._save_trades(trades)
                        total_trades += len(trades)
                        logger.info(f"Saved {len(trades)} trades for {symbol} in window {current_start} to {current_end}")
                    
                    current_end = current_start
                    
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue
        
        logger.info(f"Backfill complete. Total trades saved: {total_trades}")
        return total_trades

def main():
    parser = argparse.ArgumentParser(description='Dark Pool Trade Collector')
    parser.add_argument('--backfill', action='store_true', help='Run in backfill mode')
    parser.add_argument('--hours', type=int, default=24, help='Hours to backfill')
    args = parser.parse_args()
    
    # Load environment variables
    env_file = os.getenv('ENV_FILE', '.env')
    load_dotenv(env_file)
    
    try:
        collector = DarkPoolCollector()
        if args.backfill:
            collector.backfill_trades(hours=args.hours)
        else:
            collector.collect_trades()
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 