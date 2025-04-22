```python
# Cell 1: Setup and Imports
import os
import sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup paths
notebook_dir = Path(os.getcwd())
project_root = notebook_dir.parent
sys.path.append(str(project_root))

# Constants
DARKPOOL_ENDPOINT = "https://api.example.com/darkpool"
DEFAULT_HEADERS = {"Accept": "application/json"}
REQUEST_TIMEOUT = 30
REQUEST_RATE_LIMIT = 5  # requests per second
MAX_RETRIES = 3
BATCH_SIZE = 200  # API limit
TIME_CHUNK_HOURS = 4  # Process data in 4-hour chunks

# Configure matplotlib
plt.style.use('seaborn')
plt.rcParams['figure.figsize'] = [12, 8]
plt.rcParams['figure.dpi'] = 100

# Set timezone
eastern = pytz.timezone('US/Eastern')

# Cell 2: Helper Functions
def get_target_date(use_today=False):
    """Get the target date for fetching trades."""
    if use_today:
        return datetime.now(eastern)
    return datetime.now(eastern) - timedelta(days=1)

def get_csv_path(target_date: datetime, symbol: str):
    """Get the path for saving/loading cached trades."""
    date_str = target_date.strftime('%Y-%m-%d')
    return Path(f'data/trades/{symbol}_{date_str}.csv')

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

# Cell 3: Analysis Functions
def analyze_trades(trades_df: pd.DataFrame) -> dict:
    """Perform comprehensive analysis on trades data."""
    if trades_df.empty:
        return {}
    
    # Basic statistics
    analysis = {
        'total_trades': len(trades_df),
        'total_volume': trades_df['size'].sum(),
        'total_premium': trades_df['premium'].sum(),
        'avg_trade_size': trades_df['size'].mean(),
        'avg_price': trades_df['price'].mean(),
        'price_range': {
            'min': trades_df['price'].min(),
            'max': trades_df['price'].max(),
            'range': trades_df['price'].max() - trades_df['price'].min()
        }
    }
    
    # Time-based analysis
    trades_df['hour'] = trades_df['executed_at'].dt.hour
    hourly_stats = trades_df.groupby('hour').agg({
        'size': ['count', 'sum', 'mean'],
        'premium': 'sum',
        'price': ['min', 'max', 'mean']
    }).round(2)
    
    analysis['hourly_stats'] = hourly_stats
    
    # Market center analysis
    if 'market_center' in trades_df.columns:
        market_center_stats = trades_df.groupby('market_center').agg({
            'size': ['count', 'sum', 'mean'],
            'premium': 'sum'
        }).round(2)
        analysis['market_center_stats'] = market_center_stats
    
    # Price impact analysis
    if 'price_impact' in trades_df.columns:
        analysis['price_impact'] = {
            'mean': trades_df['price_impact'].mean(),
            'median': trades_df['price_impact'].median(),
            'std': trades_df['price_impact'].std()
        }
    
    return analysis

def plot_trade_analysis(trades_df: pd.DataFrame, analysis: dict):
    """Create visualizations for the trades data."""
    if trades_df.empty:
        return
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Dark Pool Trade Analysis', fontsize=16)
    
    # 1. Trade Volume by Hour
    hourly_volume = trades_df.groupby('hour')['size'].sum()
    axes[0, 0].bar(hourly_volume.index, hourly_volume.values)
    axes[0, 0].set_title('Trade Volume by Hour')
    axes[0, 0].set_xlabel('Hour of Day')
    axes[0, 0].set_ylabel('Total Volume')
    
    # 2. Price Distribution
    sns.histplot(data=trades_df, x='price', bins=30, ax=axes[0, 1])
    axes[0, 1].set_title('Price Distribution')
    axes[0, 1].set_xlabel('Price')
    axes[0, 1].set_ylabel('Count')
    
    # 3. Trade Size Distribution
    sns.histplot(data=trades_df, x='size', bins=30, ax=axes[1, 0])
    axes[1, 0].set_title('Trade Size Distribution')
    axes[1, 0].set_xlabel('Trade Size')
    axes[1, 0].set_ylabel('Count')
    
    # 4. Market Center Distribution (if available)
    if 'market_center' in trades_df.columns:
        market_center_counts = trades_df['market_center'].value_counts()
        axes[1, 1].pie(market_center_counts, labels=market_center_counts.index, autopct='%1.1f%%')
        axes[1, 1].set_title('Market Center Distribution')
    
    plt.tight_layout()
    plt.show()

# Cell 4: Main Execution
def main():
    # Fetch trades
    symbols = ['SPY', 'QQQ']
    trades_df = fetch_all_trades(symbols=symbols, use_today=False, force_refresh=False)
    
    if not trades_df.empty:
        # Perform analysis
        analysis = analyze_trades(trades_df)
        
        # Print summary
        print("\nTrade Analysis Summary:")
        print(f"Total Trades: {analysis['total_trades']:,}")
        print(f"Total Volume: {analysis['total_volume']:,.0f}")
        print(f"Total Premium: ${analysis['total_premium']:,.2f}")
        print(f"Average Trade Size: {analysis['avg_trade_size']:,.2f}")
        print(f"Average Price: ${analysis['avg_price']:.2f}")
        print(f"Price Range: ${analysis['price_range']['min']:.2f} - ${analysis['price_range']['max']:.2f}")
        
        # Create visualizations
        plot_trade_analysis(trades_df, analysis)
        
        # Save analysis results
        results_dir = Path('results')
        results_dir.mkdir(exist_ok=True)
        
        # Save raw data
        trades_df.to_csv(results_dir / 'trades_data.csv', index=False)
        
        # Save analysis
        with open(results_dir / 'analysis_results.json', 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
    else:
        print("No trades data available for analysis.")

if __name__ == "__main__":
    main() 