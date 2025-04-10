import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('dark_pool_fetcher')

# Polygon API configuration
POLYGON_API_KEY = "imqq2tU79_8153YxqhLHcNy8jcCYtWQ1"  # Your API key
BASE_URL = "https://api.polygon.io"

# Tickers to fetch
TICKERS = ["SPY", "QQQ"]

# Directory to save CSV files
CSV_DIR = "csv"

# Aggregate data settings
AGG_TIMESPAN = "minute"  # Options: minute, hour, day, week, month, quarter, year

def ensure_directory_exists(directory):
    """Ensure the specified directory exists, create if it doesn't."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")

def get_today_date_str():
    """Get today's date as a string in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

def get_csv_filename():
    """Generate CSV filename with the last trading day's date."""
    last_trading_day = get_last_trading_day()
    date_str = last_trading_day.strftime("%Y-%m-%d")
    return os.path.join(CSV_DIR, f"dark_pool_estimates_{date_str}.csv")

def estimate_dark_pool_volume(ticker, from_date, to_date):
    """
    Estimates dark pool volume using aggregate data.
    
    This method uses price and volume patterns to estimate potential dark pool activity,
    since direct dark pool data requires a premium Polygon.io subscription.
    """
    headers = {
        "Authorization": f"Bearer {POLYGON_API_KEY}"
    }
    
    # Convert nanosecond timestamps to milliseconds (aggregate API uses milliseconds)
    from_date_ms = from_date // 1_000_000
    to_date_ms = to_date // 1_000_000
    
    # Format for our date range in YYYY-MM-DD format
    from_date_str = datetime.fromtimestamp(from_date_ms / 1000).strftime('%Y-%m-%d')
    to_date_str = datetime.fromtimestamp(to_date_ms / 1000).strftime('%Y-%m-%d')
    
    # Aggregates endpoint (available in basic subscriptions)
    agg_endpoint = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/{AGG_TIMESPAN}/{from_date_str}/{to_date_str}"
    
    params = {
        "adjusted": "true",
        "sort": "desc",
        "limit": 5000  # Maximum allowed
    }
    
    try:
        logger.info(f"Fetching aggregate data for {ticker} from {from_date_str} to {to_date_str}")
        response = requests.get(agg_endpoint, headers=headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Aggregate data error: {response.status_code} - {response.text[:200]}")
            return []
        
        data = response.json()
        results = data.get('results', [])
        
        if not results:
            logger.warning(f"No aggregate data found for {ticker}")
            return []
            
        # Log sample data structure (for the first result)
        if results:
            logger.info(f"Sample aggregate data: {json.dumps(results[0], indent=2)}")
            
        logger.info(f"Retrieved {len(results)} aggregate records for {ticker}")
        
        # Process aggregate data to estimate dark pool activity
        
        # Calculate average volume
        volumes = [bar.get('v', 0) for bar in results]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        
        # Look for bars with unusual volume (potential dark pool activity)
        dark_pool_estimates = []
        
        for bar in results:
            timestamp = bar.get('t', 0)
            volume = bar.get('v', 0)
            open_price = bar.get('o', 0)
            close_price = bar.get('c', 0)
            high_price = bar.get('h', 0)
            low_price = bar.get('l', 0)
            
            # Simple heuristic for potential dark pool activity:
            # 1. Volume significantly above average 
            # 2. Small price change despite high volume (common in dark pools)
            volume_ratio = volume / avg_volume if avg_volume > 0 else 0
            price_change_pct = abs(close_price - open_price) / open_price * 100 if open_price > 0 else 0
            
            # Define thresholds for dark pool estimation
            is_high_volume = volume_ratio > 1.5  # 50% above average volume
            is_low_price_impact = price_change_pct < 0.1  # Less than 0.1% price change
            
            if is_high_volume and is_low_price_impact:
                # This could be a dark pool trade
                estimated_dark_pool_volume = round(volume * 0.6)  # Assume 60% of unusual volume is dark pool
                
                dark_pool_estimates.append({
                    'ticker': ticker,
                    'timestamp': timestamp,
                    'total_volume': volume,
                    'estimated_dark_pool_volume': estimated_dark_pool_volume,
                    'open': open_price,
                    'close': close_price,
                    'high': high_price,
                    'low': low_price,
                    'price_change_pct': round(price_change_pct, 4),
                    'volume_ratio': round(volume_ratio, 2),
                    'is_dark_pool_estimate': True
                })
        
        logger.info(f"Estimated {len(dark_pool_estimates)} potential dark pool activities for {ticker}")
        return dark_pool_estimates
        
    except Exception as e:
        logger.error(f"Error fetching aggregate data for {ticker}: {e}")
        logger.error(traceback.format_exc())
        return []

def process_and_save_trades(trades, csv_path):
    """Process trades data and save/update CSV file."""
    if not trades:
        logger.warning("No dark pool estimates to process.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(trades)
    
    # Convert timestamp to datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Check if file exists to determine if we're updating
    file_exists = os.path.isfile(csv_path)
    
    if file_exists:
        try:
            # Read existing file
            existing_df = pd.read_csv(csv_path)
            
            # Convert timestamp column if it exists
            if 'timestamp' in existing_df.columns:
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            
            # Combine with new data and remove duplicates
            # For timestamps, we'll round to the nearest second for deduplication
            if 'timestamp' in df.columns:
                existing_df['timestamp'] = existing_df['timestamp'].dt.round('S')
                df['timestamp'] = df['timestamp'].dt.round('S')
            
            combined_df = pd.concat([existing_df, df])
            
            # Remove duplicates based on ticker and timestamp
            if 'timestamp' in combined_df.columns and 'ticker' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['ticker', 'timestamp'])
            else:
                combined_df = combined_df.drop_duplicates()
            
            combined_df.to_csv(csv_path, index=False)
            logger.info(f"Updated existing file: {csv_path} with {len(df)} new records")
        except Exception as e:
            logger.error(f"Error updating existing file: {e}")
            logger.error(traceback.format_exc())
            # Create new file as fallback
            df.to_csv(csv_path + '.new', index=False)
            logger.info(f"Created new file: {csv_path}.new with {len(df)} records")
    else:
        # Create new file
        df.to_csv(csv_path, index=False)
        logger.info(f"Created new file: {csv_path} with {len(df)} records")

def get_last_trading_day():
    """Get the last trading day (Monday-Friday). If today is weekend, return previous Friday."""
    today = datetime.now()
    if today.weekday() >= 5:  # Saturday (5) or Sunday (6)
        # Calculate days to subtract to get to Friday
        days_to_subtract = today.weekday() - 4  # 5->1, 6->2
        last_trading_day = today - timedelta(days=days_to_subtract)
    else:
        last_trading_day = today
    return last_trading_day

def main():
    """Main function to fetch and estimate dark pool trades."""
    # Check API key
    if not POLYGON_API_KEY:
        logger.error("Please set your Polygon API key in the script")
        return
        
    logger.info(f"Testing Polygon API connection...")
    test_url = f"{BASE_URL}/v3/reference/tickers/AAPL"
    headers = {"Authorization": f"Bearer {POLYGON_API_KEY}"}
    
    try:
        test_response = requests.get(test_url, headers=headers)
        logger.info(f"API connection test status: {test_response.status_code}")
        if test_response.status_code != 200:
            logger.error(f"API connection test failed: {test_response.text[:200]}")
            logger.error("Please check your API key and subscription level")
            return
    except Exception as e:
        logger.error(f"API connection test error: {e}")
        return
    
    # Ensure csv directory exists
    ensure_directory_exists(CSV_DIR)
    
    # Get the last trading day
    last_trading_day = get_last_trading_day()
    logger.info(f"Using trading day: {last_trading_day.strftime('%Y-%m-%d')}")
    
    # Get date range for the trading day
    start_of_day = int(datetime(last_trading_day.year, last_trading_day.month, last_trading_day.day).timestamp() * 1_000_000_000)
    end_of_day = int((datetime(last_trading_day.year, last_trading_day.month, last_trading_day.day) + timedelta(days=1) - timedelta(seconds=1)).timestamp() * 1_000_000_000)
    
    # Log the time range we're querying
    logger.info(f"Fetching data from {datetime.fromtimestamp(start_of_day/1_000_000_000)} to {datetime.fromtimestamp(end_of_day/1_000_000_000)}")
    logger.info(f"Using aggregate data estimation method for dark pool activity")
    
    all_trades = []
    
    # Fetch data for each ticker
    for ticker in TICKERS:
        logger.info(f"Processing {ticker}...")
        trades = estimate_dark_pool_volume(ticker, start_of_day, end_of_day)
        all_trades.extend(trades)
        
        # Respect API rate limits
        time.sleep(1)
    
    # Get the CSV filename with the correct date
    csv_path = get_csv_filename()
    logger.info(f"Saving data to: {csv_path}")
    
    # Process and save all trades
    process_and_save_trades(all_trades, csv_path)
    
    if all_trades:
        logger.info(f"Completed processing {len(all_trades)} estimated dark pool records for {', '.join(TICKERS)}")
    else:
        logger.warning(f"No dark pool estimates found for {', '.join(TICKERS)}")
        
    # Provide guidance based on results
    if not all_trades:
        logger.info("SUGGESTIONS:")
        logger.info("1. Check if market was open today")
        logger.info("2. Try a different date range")
        logger.info("3. Consider upgrading your Polygon.io subscription for direct access to trade data")
        logger.info("4. Verify your API key permissions")
    else:
        logger.info("NOTE: Data provided is an ESTIMATION of dark pool activity based on aggregate data.")
        logger.info("For precise dark pool data, a premium Polygon.io subscription is required.")

if __name__ == "__main__":
    main()