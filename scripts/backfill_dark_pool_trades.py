import os
import sys
import time
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any
import backoff
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import requests
from requests.exceptions import RequestException
import argparse

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('backfill_dark_pool_trades.log')
    ]
)
logger = logging.getLogger(__name__)

# Parse command-line arguments for env file
parser = argparse.ArgumentParser()
parser.add_argument('--env-file', default='.env', help='Path to environment file')
args = parser.parse_args()

# Load environment variables from the specified file
load_dotenv(args.env_file)

# Database configuration
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# API configuration
API_KEY = os.getenv('API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.unusualwhales.com/api/v1')

# Create database engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Core ETFs to track
CORE_ETFS = ['SPY', 'QQQ', 'IWM', 'DIA', 'VIX']

# Progress tracking
progress_file = 'backfill_progress.txt'

def load_progress() -> Optional[datetime]:
    """Load the last processed timestamp from progress file."""
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                timestamp = f.read().strip()
                return datetime.fromisoformat(timestamp)
    except Exception as e:
        logger.error(f"Error loading progress: {e}")
    return None

def save_progress(timestamp: datetime):
    """Save the last processed timestamp to progress file."""
    try:
        with open(progress_file, 'w') as f:
            f.write(timestamp.isoformat())
    except Exception as e:
        logger.error(f"Error saving progress: {e}")

@backoff.on_exception(
    backoff.expo,
    (RequestException, SQLAlchemyError),
    max_tries=5,
    max_time=300
)
def fetch_trades(symbol: str, start_time: datetime, end_time: datetime) -> list:
    """Fetch trades from the API with exponential backoff."""
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    params = {
        'date': start_time.strftime('%Y-%m-%d'),
        'limit': 500  # Maximum limit per documentation
    }
    
    response = requests.get(
        f"{API_BASE_URL}/darkpool/{symbol}",
        headers=headers,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    
    return response.json()['data']

def process_trades(trades: list) -> int:
    """Process and store trades in the database."""
    processed_count = 0
    
    for trade in trades:
        try:
            # Filter for core ETFs
            if trade['symbol'] not in CORE_ETFS:
                continue
                
            # Insert trade into database
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO trading.darkpool_trades (
                            tracking_id, symbol, size, price, volume, premium,
                            executed_at, nbbo_ask, nbbo_bid, market_center,
                            sale_cond_codes, collection_time
                        ) VALUES (
                            :tracking_id, :symbol, :size, :price, :volume, :premium,
                            :executed_at, :nbbo_ask, :nbbo_bid, :market_center,
                            :sale_cond_codes, :collection_time
                        )
                        ON CONFLICT (tracking_id) DO NOTHING
                    """),
                    {
                        'tracking_id': trade['id'],
                        'symbol': trade['symbol'],
                        'size': trade['size'],
                        'price': trade['price'],
                        'volume': trade.get('volume', trade['size'] * trade['price']),
                        'premium': trade.get('premium', 0),
                        'executed_at': trade['timestamp'],
                        'nbbo_ask': trade.get('nbbo_ask', 0),
                        'nbbo_bid': trade.get('nbbo_bid', 0),
                        'market_center': trade.get('venue', 'UNKNOWN'),
                        'sale_cond_codes': trade.get('sale_conditions', ''),
                        'collection_time': datetime.now()
                    }
                )
                conn.commit()
                processed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing trade {trade.get('id', 'unknown')}: {e}")
            continue
            
    return processed_count

def main():
    # Start from Friday evening or last progress
    start_time = load_progress() or datetime.now() - timedelta(days=3)
    end_time = datetime.now()
    
    logger.info(f"Starting backfill from {start_time} to {end_time}")
    
    current_time = start_time
    total_processed = 0
    
    while current_time < end_time:
        try:
            # Process each symbol for the current date
            for symbol in CORE_ETFS:
                try:
                    # Fetch trades for the current symbol and date
                    trades = fetch_trades(symbol, current_time, current_time)
                    
                    # Process trades
                    processed = process_trades(trades)
                    total_processed += processed
                    
                    logger.info(f"Processed {processed} trades for {symbol} on {current_time.strftime('%Y-%m-%d')}")
                    
                except Exception as e:
                    logger.error(f"Error processing {symbol} for {current_time}: {e}")
                    continue
            
            # Save progress
            save_progress(current_time)
            
            # Move to next day
            current_time = current_time + timedelta(days=1)
            
            # Add a small delay to avoid rate limits
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing time window {current_time}: {e}")
            # Wait longer on error
            time.sleep(5)
            continue
    
    logger.info(f"Backfill completed. Total trades processed: {total_processed}")

if __name__ == "__main__":
    main() 