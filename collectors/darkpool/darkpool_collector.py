#!/usr/bin/env python3

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
import argparse

from collectors.schema_validation import DarkPoolSchemaValidator
from config.db_config import get_db_config
from config.api_config import (
    UW_BASE_URL, DARKPOOL_TICKER_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
)
from flow_analysis.config.watchlist import SYMBOLS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/collector/darkpool_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DarkPoolCollector:
    def __init__(self):
        self.schema_validator = DarkPoolSchemaValidator()
        self.db_conn = None
        self.connect_db()

    def connect_db(self):
        """Connect to the database using configuration from db_config."""
        try:
            db_config = get_db_config()
            self.db_conn = psycopg2.connect(
                dbname=db_config['dbname'],
                user=db_config['user'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port'],
                sslmode=db_config['sslmode']
            )
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, endpoint: str) -> Optional[Dict]:
        """Make API request with retry logic."""
        try:
            logger.info(f"Making request to: {endpoint}")
            response = requests.get(
                endpoint,
                headers=DEFAULT_HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            if not data or "data" not in data:
                logger.warning(f"No data received from API: {data}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request: {str(e)}")
            raise

    def _process_trades(self, trades_data: List[Dict]) -> pd.DataFrame:
        """Process and validate trades data."""
        if not trades_data:
            return pd.DataFrame()

        # Convert to DataFrame
        trades_df = pd.DataFrame(trades_data)

        # Map 'ticker' to 'symbol' if 'symbol' is missing
        if 'symbol' not in trades_df.columns and 'ticker' in trades_df.columns:
            trades_df['symbol'] = trades_df['ticker']

        # Validate schema
        valid_trades = []
        for _, trade in trades_df.iterrows():
            try:
                # Basic validation of required fields
                required_fields = ['tracking_id', 'symbol', 'price', 'size', 'executed_at']
                if all(field in trade and trade[field] is not None for field in required_fields):
                    valid_trades.append(trade.to_dict())
                else:
                    logger.warning(f"Trade missing required fields: {trade.to_dict()}")
            except Exception as e:
                logger.warning(f"Error validating trade: {str(e)}")
                continue
        
        if not valid_trades:
            logger.warning("No valid trades found after schema validation")
            return pd.DataFrame()
            
        # Convert back to DataFrame
        valid_df = pd.DataFrame(valid_trades)
        
        # Convert timestamps
        valid_df['executed_at'] = pd.to_datetime(valid_df['executed_at'])
        if valid_df['executed_at'].dt.tz is None:
            valid_df['executed_at'] = valid_df['executed_at'].dt.tz_localize('UTC')
            
        # Convert numeric columns
        numeric_columns = ['price', 'size', 'volume', 'premium', 'nbbo_ask', 'nbbo_bid']
        for col in numeric_columns:
            if col in valid_df.columns:
                valid_df[col] = pd.to_numeric(valid_df[col], errors='coerce')
                
        return valid_df

    def save_trades_to_db(self, trades: pd.DataFrame) -> None:
        """Save processed trades to the database."""
        if trades.empty:
            logger.warning("No trades to save - DataFrame is empty")
            return

        try:
            if self.db_conn is None or self.db_conn.closed:
                logger.warning("Database connection is closed, reconnecting...")
                self.connect_db()

            # Ensure collection_time is set for all trades
            trades = trades.copy()
            trades['collection_time'] = datetime.now(pytz.UTC)

            with self.db_conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute("CREATE SCHEMA IF NOT EXISTS trading;")
                
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trading.darkpool_trades (
                        id SERIAL PRIMARY KEY,
                        tracking_id BIGINT NOT NULL UNIQUE,
                        symbol VARCHAR(10) NOT NULL,
                        price NUMERIC NOT NULL,
                        size INTEGER NOT NULL,
                        volume NUMERIC,
                        premium NUMERIC,
                        executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        nbbo_ask NUMERIC,
                        nbbo_bid NUMERIC,
                        nbbo_ask_quantity INTEGER,
                        nbbo_bid_quantity INTEGER,
                        market_center VARCHAR(10),
                        sale_cond_codes TEXT,
                        ext_hour_sold_codes TEXT,
                        trade_code TEXT,
                        trade_settlement TEXT,
                        canceled BOOLEAN,
                        collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Prepare data for insertion
                columns = [
                    'tracking_id', 'symbol', 'price', 'size', 'volume', 'premium',
                    'executed_at', 'nbbo_ask', 'nbbo_bid', 'nbbo_ask_quantity',
                    'nbbo_bid_quantity', 'market_center', 'sale_cond_codes',
                    'ext_hour_sold_codes', 'trade_code', 'trade_settlement',
                    'canceled', 'collection_time'
                ]
                
                # Filter columns that exist in the DataFrame
                existing_columns = [col for col in columns if col in trades.columns]
                values = [tuple(row) for row in trades[existing_columns].values]

                # Insert trades using execute_values
                execute_values(
                    cur,
                    f"""
                    INSERT INTO trading.darkpool_trades (
                        {', '.join(existing_columns)}
                    ) VALUES %s
                    ON CONFLICT (tracking_id) DO NOTHING
                    """,
                    values
                )
                self.db_conn.commit()
                
                logger.info(f"Successfully saved {len(trades)} trades to database")
                
        except Exception as e:
            logger.error(f"Error saving trades to database: {str(e)}")
            if self.db_conn:
                self.db_conn.rollback()
            raise

    def backfill_trades(self, symbols: List[str], hours: int = 24) -> Dict[str, int]:
        """Backfill trades for specified symbols for the last N hours."""
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(hours=hours)
        
        logger.info(f"Starting backfill from {start_time} to {end_time}")
        logger.info(f"Symbols to process: {', '.join(symbols)}")
        
        results = {symbol: 0 for symbol in symbols}
        
        for symbol in symbols:
            try:
                # Use the ticker endpoint for historical data
                endpoint = f"{UW_BASE_URL}{DARKPOOL_TICKER_ENDPOINT.format(ticker=symbol)}"
                response = self._make_request(endpoint)
                
                if response is None or not response.get('data'):
                    logger.warning(f"No trades data received for {symbol}")
                    continue
                
                # Process trades
                trades = self._process_trades(response['data'])
                
                # Filter trades by time
                trades = trades[trades['executed_at'] >= start_time]
                trades = trades[trades['executed_at'] < end_time]
                
                if not trades.empty:
                    self.save_trades_to_db(trades)
                    results[symbol] = len(trades)
                    logger.info(f"Saved {len(trades)} {symbol} trades")
                else:
                    logger.warning(f"No trades found for {symbol} in time window")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                results[symbol] = -1
                
            # Rate limiting
            time.sleep(1)
        
        return results

    def collect_recent_trades(self, symbols: List[str], hours: int = 24) -> Dict[str, int]:
        """Collect trades for specified symbols for the last N hours."""
        end_time = datetime.now(pytz.UTC)
        start_time = end_time - timedelta(hours=hours)
        
        logger.info(f"Collecting trades from {start_time} to {end_time}")
        logger.info(f"Symbols to process: {', '.join(symbols)}")
        
        results = {symbol: 0 for symbol in symbols}
        
        for symbol in symbols:
            try:
                # Use the ticker endpoint for historical data
                endpoint = f"{UW_BASE_URL}{DARKPOOL_TICKER_ENDPOINT.format(ticker=symbol)}"
                response = self._make_request(endpoint)
                
                if response is None or not response.get('data'):
                    logger.warning(f"No trades data received for {symbol}")
                    continue
                
                # Process trades
                trades = self._process_trades(response['data'])
                
                # Filter trades by time
                trades = trades[trades['executed_at'] >= start_time]
                trades = trades[trades['executed_at'] < end_time]
                
                if not trades.empty:
                    self.save_trades_to_db(trades)
                    results[symbol] = len(trades)
                    logger.info(f"Saved {len(trades)} {symbol} trades")
                else:
                    logger.warning(f"No trades found for {symbol} in time window")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                results[symbol] = -1
                
            # Rate limiting
            time.sleep(1)
        
        return results

def main():
    """Main function to run the collection process."""
    parser = argparse.ArgumentParser(description='Dark Pool Trade Collector')
    parser.add_argument('--symbols', nargs='+', default=SYMBOLS,
                      help='List of symbols to collect (default: all watchlist symbols)')
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours to look back (default: 24)')
    parser.add_argument('--backfill', action='store_true',
                      help='Run in backfill mode (default: False)')
    args = parser.parse_args()
    
    collector = DarkPoolCollector()
    
    if args.backfill:
        results = collector.backfill_trades(args.symbols, args.hours)
    else:
        results = collector.collect_recent_trades(args.symbols, args.hours)
    
    # Print summary
    print("\nCollection Summary:")
    print("-" * 50)
    for symbol, count in results.items():
        status = f"{count} trades" if count >= 0 else "Failed"
        print(f"{symbol}: {status}")
    print("-" * 50)

if __name__ == "__main__":
    main() 