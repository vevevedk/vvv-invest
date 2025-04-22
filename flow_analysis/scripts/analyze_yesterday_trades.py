#!/usr/bin/env python3
"""
Script to analyze yesterday's dark pool trades with comprehensive analysis
"""

import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import time
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API Configuration
UW_BASE_URL = "https://api.unusualwhales.com/api/v1"
DEFAULT_HEADERS = {
    "Authorization": f"Bearer {os.getenv('UW_API_KEY')}",
    "Content-Type": "application/json"
}
REQUEST_TIMEOUT = 30
REQUEST_RATE_LIMIT = 5  # requests per second

def get_target_date(use_today: bool = False) -> datetime:
    """Get target date for data fetching"""
    if use_today:
        return datetime.now()
    return datetime.now() - timedelta(days=1)

def get_csv_path(date: datetime, symbol: str) -> Path:
    """Get path for cached CSV file"""
    date_str = date.strftime("%Y-%m-%d")
    return Path(f"data/raw/darkpool/{date_str}/{symbol}_trades.csv")

def fetch_all_trades(symbols=['SPY', 'QQQ'], use_today=False, force_refresh=False):
    """
    Fetch ALL trades for given symbols with data caching
    
    Parameters:
    - symbols: list of symbols to fetch
    - use_today: if True, fetch today's data instead of yesterday's
    - force_refresh: if True, fetch new data even if cache exists
    
    Returns:
    - DataFrame with all trades
    """
    target_date = get_target_date(use_today)
    all_trades = []
    
    for symbol in symbols:
        csv_path = get_csv_path(target_date, symbol)
        
        # Check if we have cached data
        if not force_refresh and csv_path.exists():
            print(f"Loading cached data for {symbol} from {csv_path}")
            symbol_trades = pd.read_csv(csv_path)
            all_trades.append(symbol_trades)
            continue
            
        print(f"Fetching trades for {symbol}...")
        symbol_trades = []
        offset = 0
        total_trades = 0
        
        while True:
            # Make direct API request with offset-based pagination
            endpoint = f"{UW_BASE_URL}/darkpool/{symbol}"
            params = {
                "date": target_date.strftime("%Y-%m-%d"),
                "offset": offset,  # Use offset for pagination
                "limit": 500  # Maximum allowed by API
            }
            
            try:
                print(f"Requesting trades with params: {params}")
                response = requests.get(
                    endpoint,
                    headers=DEFAULT_HEADERS,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                data = response.json()
                
                if not data or "data" not in data or not data["data"]:
                    print(f"No more trades found for {symbol} after offset {offset}")
                    break
                
                trades = pd.DataFrame(data["data"])
                trades_count = len(trades)
                
                if trades_count == 0:
                    print(f"No trades returned for offset {offset}")
                    break
                
                # Clean numeric columns
                numeric_columns = ['price', 'size', 'premium', 'price_impact']
                for col in numeric_columns:
                    if col in trades.columns:
                        trades[col] = pd.to_numeric(trades[col], errors='coerce')
                
                symbol_trades.append(trades)
                total_trades += trades_count
                print(f"Fetched {trades_count} trades for {symbol} (Offset: {offset}, Total: {total_trades})")
                
                # Rate limiting
                time.sleep(1.0 / REQUEST_RATE_LIMIT)
                
                # If we got less than the limit, we've reached the end
                if trades_count < 500:
                    print(f"Received less than 500 trades, ending pagination")
                    break
                
                # Increment offset for next batch
                offset += trades_count
                
            except Exception as e:
                print(f"Error fetching trades at offset {offset} for {symbol}: {str(e)}")
                if isinstance(e, requests.exceptions.RequestException):
                    print(f"Response content: {e.response.content if hasattr(e, 'response') else 'No response'}")
                break
        
        if symbol_trades:
            # Combine all pages for this symbol
            symbol_df = pd.concat(symbol_trades, ignore_index=True)
            
            # Remove duplicates if any
            symbol_df = symbol_df.drop_duplicates(subset=['tracking_id']) if 'tracking_id' in symbol_df.columns else symbol_df
            
            # Save to CSV
            symbol_df.to_csv(csv_path, index=False)
            print(f"Saved {len(symbol_df)} unique trades to {csv_path}")
            
            all_trades.append(symbol_df)
    
    if all_trades:
        combined_df = pd.concat(all_trades, ignore_index=True)
        print(f"\nTotal trades fetched across all symbols: {len(combined_df)}")
        return combined_df
    
    return pd.DataFrame()

def get_yesterday_date():
    """Get yesterday's date in YYYY-MM-DD format"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def analyze_sentiment(trades: pd.DataFrame) -> dict:
    """Analyze overall sentiment from trades"""
    if trades.empty:
        return {}
        
    # Calculate volume-weighted price impact
    trades['vw_price_impact'] = trades['price_impact'] * trades['size']
    total_volume = trades['size'].sum()
    
    # Calculate sentiment metrics
    sentiment = {
        'total_volume': total_volume,
        'total_premium': trades['premium'].sum(),
        'avg_price_impact': trades['vw_price_impact'].sum() / total_volume,
        'block_trades': len(trades[trades['is_block_trade']]),
        'high_premium_trades': len(trades[trades['is_high_premium']]),
        'significant_impact_trades': len(trades[trades['is_price_impact']]),
        'total_trades': len(trades),
        'avg_trade_size': trades['size'].mean(),
        'median_trade_size': trades['size'].median(),
        'max_trade_size': trades['size'].max(),
        'volume_by_symbol': trades.groupby('symbol')['size'].sum().to_dict(),
        'premium_by_symbol': trades.groupby('symbol')['premium'].sum().to_dict()
    }
    
    return sentiment

def analyze_strike_prices(trades: pd.DataFrame) -> pd.DataFrame:
    """Analyze trades by strike price"""
    if trades.empty:
        return pd.DataFrame()
        
    # Group by strike price and calculate metrics
    strike_analysis = trades.groupby(['symbol', 'strike']).agg({
        'size': ['sum', 'count', 'mean', 'std'],
        'premium': ['sum', 'mean', 'std'],
        'price_impact': ['mean', 'std'],
        'is_block_trade': 'sum',
        'is_high_premium': 'sum',
        'is_price_impact': 'sum'
    }).reset_index()
    
    # Flatten column names
    strike_analysis.columns = ['_'.join(col).strip('_') for col in strike_analysis.columns.values]
    
    # Sort by volume
    strike_analysis = strike_analysis.sort_values('size_sum', ascending=False)
    
    return strike_analysis

def analyze_expirations(trades: pd.DataFrame) -> pd.DataFrame:
    """Analyze trades by expiration date"""
    if trades.empty:
        return pd.DataFrame()
        
    # Group by expiration and calculate metrics
    exp_analysis = trades.groupby(['symbol', 'expiration']).agg({
        'size': ['sum', 'count', 'mean', 'std'],
        'premium': ['sum', 'mean', 'std'],
        'price_impact': ['mean', 'std'],
        'is_block_trade': 'sum',
        'is_high_premium': 'sum',
        'is_price_impact': 'sum'
    }).reset_index()
    
    # Flatten column names
    exp_analysis.columns = ['_'.join(col).strip('_') for col in exp_analysis.columns.values]
    
    # Sort by volume
    exp_analysis = exp_analysis.sort_values('size_sum', ascending=False)
    
    return exp_analysis

def create_visualizations(trades: pd.DataFrame, output_dir: Path) -> None:
    """Create visualizations of the trade data"""
    if trades.empty:
        return
        
    # Set style
    plt.style.use('seaborn')
    
    # 1. Volume Distribution by Symbol
    plt.figure(figsize=(12, 6))
    sns.barplot(data=trades, x='symbol', y='size', estimator=sum)
    plt.title('Total Volume by Symbol')
    plt.xlabel('Symbol')
    plt.ylabel('Total Volume')
    plt.savefig(output_dir / 'volume_by_symbol.png')
    plt.close()
    
    # 2. Price Impact Distribution
    plt.figure(figsize=(12, 6))
    sns.histplot(data=trades, x='price_impact', bins=50)
    plt.title('Distribution of Price Impact')
    plt.xlabel('Price Impact')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'price_impact_distribution.png')
    plt.close()
    
    # 3. Premium Distribution
    plt.figure(figsize=(12, 6))
    sns.histplot(data=trades, x='premium', bins=50)
    plt.title('Distribution of Premium')
    plt.xlabel('Premium')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'premium_distribution.png')
    plt.close()
    
    # 4. Time Series of Volume
    trades['timestamp'] = pd.to_datetime(trades['timestamp'])
    trades['hour'] = trades['timestamp'].dt.hour
    volume_by_hour = trades.groupby(['symbol', 'hour'])['size'].sum().reset_index()
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=volume_by_hour, x='hour', y='size', hue='symbol')
    plt.title('Volume by Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Volume')
    plt.savefig(output_dir / 'volume_by_hour.png')
    plt.close()

def save_analysis_results(date: str, sentiment: dict, strike_analysis: pd.DataFrame, 
                         exp_analysis: pd.DataFrame, trades: pd.DataFrame) -> None:
    """Save analysis results to CSV files"""
    # Create output directory
    output_dir = Path("data/analysis") / date
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save sentiment as JSON
    import json
    with open(output_dir / "sentiment.json", "w") as f:
        json.dump(sentiment, f, indent=2)
    
    # Save strike and expiration analysis as CSV
    strike_analysis.to_csv(output_dir / "strike_analysis.csv", index=False)
    exp_analysis.to_csv(output_dir / "expiration_analysis.csv", index=False)
    
    # Create visualizations
    create_visualizations(trades, output_dir)
    
    logger.info(f"Analysis results saved to {output_dir}")

def main():
    # Get yesterday's date
    yesterday = get_yesterday_date()
    logger.info(f"Analyzing trades for {yesterday}")
    
    # Fetch all trades from yesterday
    logger.info("Fetching trades...")
    trades = fetch_all_trades(symbols=['SPY', 'QQQ'], use_today=False, force_refresh=False)
    
    if trades.empty:
        logger.warning(f"No trades found for {yesterday}")
        return
    
    logger.info(f"Found {len(trades)} trades to analyze")
    
    # Perform analysis
    logger.info("Analyzing sentiment...")
    sentiment = analyze_sentiment(trades)
    
    logger.info("Analyzing strike prices...")
    strike_analysis = analyze_strike_prices(trades)
    
    logger.info("Analyzing expirations...")
    exp_analysis = analyze_expirations(trades)
    
    # Save results
    logger.info("Saving analysis results...")
    save_analysis_results(yesterday, sentiment, strike_analysis, exp_analysis, trades)
    
    # Print summary
    logger.info("\nAnalysis Summary:")
    logger.info(f"Total Volume: {sentiment['total_volume']:,.0f}")
    logger.info(f"Total Premium: ${sentiment['total_premium']:,.2f}")
    logger.info(f"Average Price Impact: {sentiment['avg_price_impact']:.2%}")
    logger.info(f"Block Trades: {sentiment['block_trades']}")
    logger.info(f"High Premium Trades: {sentiment['high_premium_trades']}")
    logger.info(f"Significant Impact Trades: {sentiment['significant_impact_trades']}")
    
    logger.info("\nVolume by Symbol:")
    for symbol, volume in sentiment['volume_by_symbol'].items():
        logger.info(f"{symbol}: {volume:,.0f} shares")
    
    logger.info("\nTop Strike Prices by Volume:")
    for _, row in strike_analysis.head(5).iterrows():
        logger.info(f"{row['symbol']} Strike ${row['strike']}: {row['size_sum']:,.0f} shares, ${row['premium_sum']:,.2f} premium")
    
    logger.info("\nTop Expirations by Volume:")
    for _, row in exp_analysis.head(5).iterrows():
        logger.info(f"{row['symbol']} Expiration {row['expiration']}: {row['size_sum']:,.0f} shares, ${row['premium_sum']:,.2f} premium")

if __name__ == "__main__":
    main() 