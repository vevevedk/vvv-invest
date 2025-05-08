from datetime import datetime, timedelta
import pytz
import time
import os
import argparse
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from flow_analysis.scripts.darkpool_collector import DarkPoolCollector
from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_TICKER_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
)
import requests
from typing import Dict, List, Optional
import logging
from psycopg2.extras import execute_values

# Add the project root to the Python path
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Load environment variables from .env.prod file
load_dotenv('.env.prod')

# Verify required environment variables
required_vars = ['UW_API_TOKEN', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Database configuration
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'  # Force SSL mode for production
}

def test_db_connection():
    """Test database connection with detailed error handling."""
    try:
        print("Testing database connection...")
        print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']} as {DB_CONFIG['user']}")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to database!")
        conn.close()
        return True
    except OperationalError as e:
        print("\nDatabase connection failed!")
        print(f"Error: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Verify your IP address is whitelisted in Digital Ocean database settings")
        print("2. Check that the database credentials are correct")
        print("3. Ensure SSL mode is set to 'require'")
        print("4. Verify the database host and port are correct")
        return False
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        return False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DarkPoolBackfillCollector(DarkPoolCollector):
    def _make_request(self, endpoint: str) -> Optional[Dict]:
        """Make API request with improved error handling and logging."""
        try:
            logger.info(f"Making request to: {endpoint}")
            response = requests.get(
                endpoint,
                headers=DEFAULT_HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Received response with {len(data.get('data', []))} trades")
                return data
            else:
                logger.error(f"API request failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error making request: {str(e)}")
            return None

    def save_trades_to_db(self, trades: pd.DataFrame) -> None:
        """Save processed trades to the database."""
        if trades.empty:
            self.logger.warning("No trades to save - DataFrame is empty")
            return

        try:
            if self.db_conn is None or self.db_conn.closed:
                self.logger.warning("Database connection is closed, reconnecting...")
                self.connect_db()
                if self.db_conn.closed:
                    raise Exception("Failed to reconnect to database")

            # Log trade distribution before saving
            symbol_counts = trades['symbol'].value_counts()
            self.logger.info(f"Trades to save by symbol: {symbol_counts.to_dict()}")

            with self.db_conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute("CREATE SCHEMA IF NOT EXISTS trading;")
                
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'trading' 
                        AND table_name = 'darkpool_trades'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    # Create table if it doesn't exist with all columns
                    cur.execute("""
                        CREATE TABLE trading.darkpool_trades (
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
                    self.logger.info("Created new darkpool_trades table with all columns")
                else:
                    # Add new columns if they don't exist
                    new_columns = {
                        'nbbo_ask_quantity': 'INTEGER',
                        'nbbo_bid_quantity': 'INTEGER',
                        'trade_code': 'TEXT',
                        'trade_settlement': 'TEXT',
                        'ext_hour_sold_codes': 'TEXT',
                        'sale_cond_codes': 'TEXT',
                        'canceled': 'BOOLEAN'
                    }
                    
                    # Check existing columns
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'trading' 
                        AND table_name = 'darkpool_trades';
                    """)
                    existing_columns = {row[0] for row in cur.fetchall()}
                    
                    # Add missing columns
                    for col_name, col_type in new_columns.items():
                        if col_name not in existing_columns:
                            self.logger.info(f"Adding new column {col_name} ({col_type}) to table")
                            cur.execute(f"""
                                ALTER TABLE trading.darkpool_trades 
                                ADD COLUMN {col_name} {col_type};
                            """)
                            self.logger.info(f"Successfully added column {col_name}")
                    
                    self.db_conn.commit()
                    self.logger.info("Table structure updated with new columns")
                
                self.logger.info("Database schema and table verified")

                # Add collection time
                trades['collection_time'] = datetime.now(pytz.UTC)

                # Prepare data for insertion with all columns
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

                # Log the SQL we're about to execute
                self.logger.info(f"Executing insert with columns: {existing_columns}")
                self.logger.info(f"Number of rows to insert: {len(values)}")

                # Insert trades using execute_values for better performance
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
                
                # Verify the insert by counting rows
                cur.execute("""
                    SELECT symbol, COUNT(*) as count 
                    FROM trading.darkpool_trades 
                    WHERE collection_time >= NOW() - INTERVAL '1 minute'
                    GROUP BY symbol
                """)
                recent_counts = dict(cur.fetchall())
                self.logger.info(f"Recent trades saved by symbol: {recent_counts}")
                
                self.logger.info(f"Successfully saved {len(trades)} trades to database")
        except Exception as e:
            if not self.db_conn.closed:
                self.db_conn.rollback()
            self.logger.error(f"Error saving trades to database: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            self.logger.error(f"Error details: {str(e)}")
            raise

def backfill_symbol(collector, symbol, start_time, end_time):
    """Backfill trades for a specific symbol."""
    # Convert times to UTC for consistent comparison
    start_time = start_time.replace(tzinfo=pytz.UTC)
    end_time = end_time.replace(tzinfo=pytz.UTC)
    
    logger.info(f"\nStarting backfill for {symbol} from {start_time} to {end_time}")
    
    # Validate time window
    if (end_time - start_time).total_seconds() > 24 * 3600:  # 24 hours in seconds
        logger.warning("Time window exceeds 24 hours, adjusting to last 24 hours")
        start_time = end_time - timedelta(hours=24)
    
    current_time = start_time
    total_trades = 0
    retry_count = 0
    max_retries = 3
    
    while current_time < end_time:
        try:
            # Use the historical endpoint
            endpoint = f"{UW_BASE_URL}{DARKPOOL_TICKER_ENDPOINT.format(ticker=symbol)}"
            logger.info(f"Making request to: {endpoint}")
            
            response = collector._make_request(endpoint)
            
            if response is None:
                logger.error(f"API request failed for {symbol} at {current_time}")
                time.sleep(1)  # Rate limiting
                continue
                
            if not response.get('data'):
                logger.warning(f"No trades data received for {symbol} at {current_time}")
                logger.debug(f"API Response: {response}")
                time.sleep(1)  # Rate limiting
                continue
            
            # Process trades
            trades = collector._process_trades(response['data'])
            
            # Handle timezone conversion for executed_at
            if 'executed_at' in trades.columns:
                try:
                    # First convert to datetime if not already
                    trades['executed_at'] = pd.to_datetime(trades['executed_at'])
                    
                    # Check if timestamps are timezone-aware
                    if trades['executed_at'].dt.tz is None:
                        # If naive, localize to UTC
                        trades['executed_at'] = trades['executed_at'].dt.tz_localize('UTC')
                    else:
                        # If already timezone-aware, convert to UTC
                        trades['executed_at'] = trades['executed_at'].dt.tz_convert('UTC')
                        
                    logger.info(f"Successfully converted timestamps to UTC")
                except Exception as e:
                    logger.error(f"Error converting timestamps: {str(e)}")
                    continue
            
            # Filter trades by time - only keep trades from last 24 hours
            trades = trades[trades['executed_at'] >= start_time]
            trades = trades[trades['executed_at'] < end_time]
            
            if not trades.empty:
                # Save to database
                collector.save_trades_to_db(trades)
                total_trades += len(trades)
                logger.info(f"Saved {len(trades)} {symbol} trades for {current_time}")
                retry_count = 0  # Reset retry count on success
            else:
                logger.warning(f"No trades found for {symbol} in time window {current_time}")
                logger.debug(f"Raw API response data: {response['data'][:2] if response['data'] else 'No data'}")
            
            # Move to next time window
            current_time += timedelta(minutes=5)
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing trades for {symbol}: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            retry_count += 1
            if retry_count >= max_retries:
                logger.warning(f"Max retries reached for {symbol} at {current_time}, moving to next time window")
                current_time += timedelta(minutes=5)
                retry_count = 0
            time.sleep(1)
            continue
    
    logger.info(f"Backfill complete for {symbol}. Total trades saved: {total_trades}")
    return total_trades

def main():
    """Main function to run the backfill process."""
    # Initialize collector
    collector = DarkPoolBackfillCollector()
    
    # Define symbols and time window - 7 days
    symbols = ['SPY', 'QQQ', 'GLD']
    end_time = datetime.now(pytz.UTC)  # Use UTC time
    start_time = end_time - timedelta(days=7)  # Last 7 days
    
    logger.info(f"Starting 7-day backfill from {start_time} to {end_time}")
    logger.info("This will be done in 24-hour chunks to respect API limits")
    
    # Process each day separately to manage memory and API limits
    current_end = end_time
    results = {symbol: 0 for symbol in symbols}
    
    while current_end > start_time:
        current_start = max(current_end - timedelta(hours=24), start_time)
        logger.info(f"\nProcessing chunk: {current_start} to {current_end}")
        
        # Process each symbol for this time chunk
        for symbol in symbols:
            try:
                trades_count = backfill_symbol(collector, symbol, current_start, current_end)
                results[symbol] += trades_count
                logger.info(f"Completed {symbol} for period {current_start} to {current_end}: {trades_count} trades")
                # Add a small delay between symbols to respect rate limits
                time.sleep(2)
            except Exception as e:
                logger.error(f"Error processing {symbol} for period {current_start} to {current_end}: {str(e)}")
                continue
        
        # Move to next chunk
        current_end = current_start
        # Add a delay between chunks to respect rate limits
        time.sleep(5)
    
    # Print summary
    print("\nBackfill Summary (Last 7 Days):")
    print("-" * 50)
    for symbol, count in results.items():
        print(f"{symbol}: {count} trades")
    print("-" * 50)
    
    # Print time range processed
    print(f"\nTime Range Processed:")
    print(f"Start: {start_time}")
    print(f"End: {end_time}")

if __name__ == '__main__':
    main() 