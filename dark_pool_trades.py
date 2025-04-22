import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DARKPOOL_ENDPOINT = "https://api.example.com/darkpool"
DEFAULT_HEADERS = {"Accept": "application/json"}
REQUEST_TIMEOUT = 30
REQUEST_RATE_LIMIT = 5  # requests per second
MAX_RETRIES = 3
BATCH_SIZE = 200  # API limit
TIME_CHUNK_HOURS = 4  # Process data in 4-hour chunks

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_trades_chunk(symbol: str, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
    """Fetch trades for a specific time chunk with retry logic."""
    params = {
        "symbol": symbol,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "limit": BATCH_SIZE
    }
    
    try:
        response = requests.get(
            f"{DARKPOOL_ENDPOINT}/recent",
            headers=DEFAULT_HEADERS,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        if not data or "data" not in data or not data["data"]:
            return None
            
        trades = pd.DataFrame(data["data"])
        if len(trades) == 0:
            return None
            
        # Convert and clean data
        trades['executed_at'] = pd.to_datetime(trades['executed_at'])
        numeric_columns = ['price', 'size', 'premium', 'price_impact']
        for col in numeric_columns:
            if col in trades.columns:
                trades[col] = pd.to_numeric(trades[col], errors='coerce')
                
        return trades
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching trades for {symbol} between {start_time} and {end_time}: {str(e)}")
        raise

def process_symbol_trades(symbol: str, target_date: datetime, force_refresh: bool) -> Optional[pd.DataFrame]:
    """Process trades for a single symbol."""
    csv_path = get_csv_path(target_date, symbol)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Check cache
    if not force_refresh and csv_path.exists():
        logger.info(f"Loading cached data for {symbol} from {csv_path}")
        return pd.read_csv(csv_path)
        
    logger.info(f"Fetching trades for {symbol} on {target_date.strftime('%Y-%m-%d')}")
    
    # Generate time chunks
    time_chunks = []
    current_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    while current_time < end_time:
        chunk_end = min(current_time + timedelta(hours=TIME_CHUNK_HOURS), end_time)
        time_chunks.append((current_time, chunk_end))
        current_time = chunk_end
    
    # Fetch trades for each time chunk
    all_trades = []
    seen_tracking_ids = set()
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_trades_chunk, symbol, start, end): (start, end)
            for start, end in time_chunks
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Fetching {symbol}"):
            start, end = futures[future]
            try:
                trades = future.result()
                if trades is not None:
                    # Remove duplicates
                    if 'tracking_id' in trades.columns:
                        trades = trades[~trades['tracking_id'].isin(seen_tracking_ids)]
                        seen_tracking_ids.update(trades['tracking_id'])
                    
                    if len(trades) > 0:
                        all_trades.append(trades)
                        
            except Exception as e:
                logger.error(f"Error processing chunk for {symbol} between {start} and {end}: {str(e)}")
    
    if all_trades:
        symbol_df = pd.concat(all_trades, ignore_index=True)
        symbol_df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(symbol_df)} unique trades to {csv_path}")
        return symbol_df
    
    return None

def fetch_all_trades(symbols=['SPY', 'QQQ'], use_today=False, force_refresh=False):
    """
    Fetch ALL trades for given symbols with data caching and parallel processing
    
    Parameters:
    - symbols: list of symbols to fetch
    - use_today: if True, fetch today's data instead of yesterday's
    - force_refresh: if True, fetch new data even if cache exists
    
    Returns:
    - DataFrame with all trades
    """
    target_date = get_target_date(use_today)
    logger.info(f"Fetching trades for date: {target_date.strftime('%Y-%m-%d')}")
    
    # Process symbols in parallel
    all_trades = []
    with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        futures = {
            executor.submit(process_symbol_trades, symbol, target_date, force_refresh): symbol
            for symbol in symbols
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing symbols"):
            symbol = futures[future]
            try:
                trades = future.result()
                if trades is not None:
                    all_trades.append(trades)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
    
    if all_trades:
        combined_df = pd.concat(all_trades, ignore_index=True)
        logger.info(f"\nTotal trades fetched across all symbols: {len(combined_df)}")
        return combined_df
    
    return pd.DataFrame()

def analyze_trades(trades: List[Dict]) -> Dict:
    """Analyze the trades and return key metrics."""
    if not trades:
        return {}
    
    df = pd.DataFrame(trades)
    df['executed_at'] = pd.to_datetime(df['executed_at'])
    df['price'] = pd.to_numeric(df['price'])
    df['size'] = pd.to_numeric(df['size'])
    df['premium'] = pd.to_numeric(df['premium'])
    
    # Calculate metrics
    total_volume = df['size'].sum()
    total_premium = df['premium'].sum()
    avg_price = total_premium / total_volume if total_volume > 0 else 0
    
    # Group by hour to analyze trading patterns
    df['hour'] = df['executed_at'].dt.hour
    hourly_stats = df.groupby('hour').agg({
        'size': 'sum',
        'premium': 'sum'
    }).reset_index()
    
    # Calculate price range
    min_price = df['price'].min()
    max_price = df['price'].max()
    price_range = max_price - min_price
    
    return {
        'total_volume': total_volume,
        'total_premium': total_premium,
        'average_price': avg_price,
        'price_range': price_range,
        'min_price': min_price,
        'max_price': max_price,
        'number_of_trades': len(trades),
        'hourly_stats': hourly_stats.to_dict('records')
    }

def save_results(symbol: str, date: str, trades: List[Dict], analysis: Dict):
    """Save the trades and analysis results to files."""
    # Create results directory if it doesn't exist
    os.makedirs('results', exist_ok=True)
    
    # Save raw trades
    trades_file = f'results/{symbol}_{date}_trades.json'
    with open(trades_file, 'w') as f:
        json.dump(trades, f, indent=2)
    
    # Save analysis
    analysis_file = f'results/{symbol}_{date}_analysis.json'
    with open(analysis_file, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    print(f"Results saved to {trades_file} and {analysis_file}")

def main():
    symbols = ['SPY', 'QQQ']
    date = '2025-04-11'  # Example date
    
    for symbol in symbols:
        print(f"\nProcessing {symbol} for {date}")
        
        # Get all trades for the date
        trades = get_all_trades_for_date(symbol, date)
        
        if trades:
            # Analyze the trades
            analysis = analyze_trades(trades)
            
            # Save results
            save_results(symbol, date, trades, analysis)
            
            # Print summary
            print(f"\nSummary for {symbol} on {date}:")
            print(f"Total trades: {analysis['number_of_trades']}")
            print(f"Total volume: {analysis['total_volume']:,}")
            print(f"Total premium: ${analysis['total_premium']:,.2f}")
            print(f"Average price: ${analysis['average_price']:.2f}")
            print(f"Price range: ${analysis['price_range']:.2f}")
        else:
            print(f"No trades found for {symbol} on {date}")

if __name__ == "__main__":
    main() 