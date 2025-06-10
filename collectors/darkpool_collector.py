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
from collectors.utils.logging_config import (
    log_heartbeat, log_collector_summary, log_error, log_warning, log_info
)

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
    
    def _get_latest_executed_at(self, symbol):
        """Get the latest executed_at timestamp for a symbol from the database."""
        query = """
        SELECT MAX(executed_at) FROM trading.darkpool_trades WHERE symbol = :symbol
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol}).scalar()
            return result

    def collect_trades(self):
        """Collect recent dark pool trades, always fetching from the latest executed_at in the DB up to now."""
        end_time = datetime.now(pytz.UTC)
        log_heartbeat('darkpool', status='running')
        logger.info(f"Collecting trades up to {end_time}")
        total_trades = 0
        start = datetime.utcnow()
        for symbol in self.symbols:
            try:
                latest_executed_at = self._get_latest_executed_at(symbol)
                if latest_executed_at is not None:
                    # Add 1 second to avoid overlap
                    start_time = latest_executed_at + timedelta(seconds=1)
                else:
                    # If no data, fetch for the last N minutes
                    start_time = end_time - timedelta(minutes=self.lookback_minutes)
                logger.info(f"Collecting trades for {symbol} from {start_time} to {end_time}")
                trades = self._fetch_trades(symbol, start_time, end_time)
                if trades:
                    self._save_trades(trades)
                    total_trades += len(trades)
                    logger.info(f"Saved {len(trades)} trades for {symbol}")
                else:
                    logger.info(f"No new trades found for {symbol}")
            except Exception as e:
                logger.error(f"Error collecting trades for {symbol}: {str(e)}")
                log_error('darkpool', e, task_type='collect_trades', details={'symbol': symbol})
        end = datetime.utcnow()
        log_collector_summary(
            collector_name='darkpool',
            start_time=start,
            end_time=end,
            items_collected=total_trades,
            task_type='collect_trades',
            status='completed'
        )
        return total_trades
    
    def backfill_trades(self, hours: int = 24):
        """Backfill dark pool trades for the past N hours."""
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(hours=hours)
        log_heartbeat('darkpool', status='backfill')
        logger.info(f"Backfilling trades from {start_time} to {end_time}")
        total_trades = 0
        start = datetime.utcnow()
        for symbol in self.symbols:
            try:
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
                logger.error(f"Error backfilling trades for {symbol}: {str(e)}")
                log_error('darkpool', e, task_type='backfill_trades', details={'symbol': symbol})
        end = datetime.utcnow()
        log_collector_summary(
            collector_name='darkpool',
            start_time=start,
            end_time=end,
            items_collected=total_trades,
            task_type='backfill_trades',
            status='completed'
        )
        return total_trades

def main():
    parser = argparse.ArgumentParser(description='Dark Pool Trade Collector')
    parser.add_argument('--api-key', help='API key for authentication')
    parser.add_argument('--db-url', help='Database URL')
    parser.add_argument('--backfill', action='store_true', help='Run in backfill mode')
    parser.add_argument('--hours', type=int, default=24, help='Number of hours to look back (default: 24)')
    args = parser.parse_args()
    
    collector = DarkPoolCollector(args.api_key, args.db_url)
    if args.backfill:
        collector.backfill_trades(hours=args.hours)
    else:
        collector.collect_trades()

if __name__ == "__main__":
    main() 