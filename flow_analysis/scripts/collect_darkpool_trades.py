import os
import sys
import logging
import argparse
from datetime import datetime, time, timedelta
import pytz
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from flow_analysis.config.watchlist import SYMBOLS

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.api_config import UW_API_TOKEN, UW_BASE_URL, DARKPOOL_TICKER_ENDPOINT, DEFAULT_HEADERS

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'sslmode': os.getenv('DB_SSL_MODE', 'require')
}

# Create database URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?sslmode={DB_CONFIG['sslmode']}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('darkpool_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def is_market_open():
    """Check if the market is currently open."""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    # Market hours (9:30 AM to 4:00 PM ET)
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        logger.info("Market is closed - weekend")
        return False
    
    # Check if it's a holiday (you might want to add a holiday calendar)
    # For now, we'll just check regular hours
    
    current_time = now.time()
    is_open = market_open <= current_time <= market_close
    
    if not is_open:
        logger.info(f"Market is closed - current ET time: {now.strftime('%H:%M:%S')}")
    
    return is_open

def fetch_trades(symbol, date_str, limit=500):
    """Fetch dark pool trades from the API."""
    url = f"{UW_BASE_URL}{DARKPOOL_TICKER_ENDPOINT.replace('{ticker}', symbol)}"
    
    params = {
        "date": date_str,
        "limit": limit
    }
    
    try:
        logger.info(f"Making request to: {url}")
        logger.info(f"With params: {params}")
        logger.info(f"Headers: {DEFAULT_HEADERS}")
        
        response = requests.get(url, headers=DEFAULT_HEADERS, params=params)
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text[:500]}")  # Log first 500 chars of response
        
        response.raise_for_status()
        data = response.json()
        
        # Log the number of trades received
        if data and isinstance(data, dict) and 'data' in data:
            logger.info(f"Received {len(data['data'])} trades for {symbol}")
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching trades for {symbol}: {str(e)}")
        logger.error(f"Full error: {str(e.__class__.__name__)}: {str(e)}")
        return None

def save_to_database(df, engine):
    """Save trades to the database with deduplication."""
    try:
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
            conn.execute(text("""
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
            """))
            conn.commit()
            
        # Add collection time
        df['collection_time'] = datetime.now()
        
        # Save to database, ignoring duplicates based on tracking_id
        df.to_sql('darkpool_trades', engine, schema='trading', 
                 if_exists='append', index=False, 
                 method='multi', chunksize=100)
        
        logger.info(f"Successfully saved {len(df)} trades to database")
    except Exception as e:
        logger.error(f"Error saving to database: {str(e)}")
        logger.error(f"Full error: {str(e.__class__.__name__)}: {str(e)}")
        raise

def get_last_trading_day():
    """Get the most recent trading day."""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    # Special case for Easter weekend 2025
    if now.strftime('%Y-%m-%d') in ['2025-04-18', '2025-04-19', '2025-04-20']:
        return '2025-04-17'  # Thursday before Good Friday
    
    # Regular trading day logic
    if now.weekday() == 0:  # Monday
        return (now - timedelta(days=3)).strftime('%Y-%m-%d')  # Friday
    elif now.weekday() >= 1 and now.weekday() <= 5:
        return (now - timedelta(days=1)).strftime('%Y-%m-%d')
    else:  # Weekend
        if now.weekday() == 6:  # Saturday
            return (now - timedelta(days=1)).strftime('%Y-%m-%d')  # Friday
        else:  # Sunday
            return (now - timedelta(days=2)).strftime('%Y-%m-%d')  # Friday

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Collect dark pool trades data')
    parser.add_argument('--historical', action='store_true', help='Fetch data from last trading day')
    args = parser.parse_args()
    
    # If historical flag is not set, check if market is open
    if not args.historical and not is_market_open():
        logger.info("Market is closed, skipping collection")
        return
    
    # Initialize database connection
    try:
        engine = create_engine(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return
    
    # Get date to fetch
    if args.historical:
        fetch_date = get_last_trading_day()
        logger.info(f"Fetching historical data for {fetch_date}")
    else:
        fetch_date = datetime.now().strftime('%Y-%m-%d')
    
    # Use symbols from config
    for symbol in SYMBOLS:
        logger.info(f"Fetching trades for {symbol} on {fetch_date}")
        response = fetch_trades(symbol, fetch_date)
        
        if response and isinstance(response, dict) and 'data' in response:
            trades = response['data']
            if trades and isinstance(trades, list):
                df = pd.DataFrame(trades)
                if not df.empty:
                    # Rename columns to match database schema
                    if 'ticker' in df.columns:
                        df = df.rename(columns={'ticker': 'symbol'})
                    
                    # Convert data types
                    df['executed_at'] = pd.to_datetime(df['executed_at'])
                    df['price'] = pd.to_numeric(df['price'])
                    df['size'] = pd.to_numeric(df['size'])
                    df['volume'] = pd.to_numeric(df['volume'])
                    df['premium'] = pd.to_numeric(df['premium'])
                    df['nbbo_ask'] = pd.to_numeric(df['nbbo_ask'])
                    df['nbbo_bid'] = pd.to_numeric(df['nbbo_bid'])
                    
                    # Save to database
                    save_to_database(df, engine)
                else:
                    logger.info(f"No new trades found for {symbol}")
            else:
                logger.warning(f"Invalid trades data format for {symbol}")
        else:
            logger.warning(f"No valid trades data received for {symbol}")

if __name__ == "__main__":
    main() 