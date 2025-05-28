#!/usr/bin/env python3
"""
Market Data Fetcher

This script fetches market data from the trading database and saves it to CSV files.
It supports fetching dark pool trades, news headlines, and options flow data for specified symbols.

Usage:
    python fetch_market_data.py [--symbols SYMBOL1 SYMBOL2 ...] [--hours HOURS]

Example:
    python fetch_market_data.py --symbols AAPL MSFT NVDA --hours 48
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import argparse
import psycopg2
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

# Database configuration
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'
}

def get_connection_string() -> str:
    """Create a PostgreSQL connection string from the configuration."""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?sslmode=require"

def fetch_market_data(symbols: Optional[List[str]] = None, hours: int = 24) -> Dict[str, Any]:
    """
    Fetch market data for specified symbols and save to CSV files.
    
    Args:
        symbols: List of stock symbols to fetch data for. Defaults to ["SPY", "QQQ", "GLD"].
        hours: Number of hours of historical data to fetch. Defaults to 24.
    
    Returns:
        Dictionary containing the fetched dataframes and output file paths.
    """
    if symbols is None:
        symbols = ["SPY", "QQQ", "GLD"]
    
    # Calculate timestamp for specified hours ago
    time_ago = datetime.now() - timedelta(hours=hours)

    # Query for dark pool trades
    darkpool_query = """
    SELECT 
        t.*,
        date_trunc('hour', t.executed_at) as trade_hour,
        t.price - t.nbbo_bid as price_impact,
        CASE 
            WHEN t.nbbo_bid != 0 THEN (t.price - t.nbbo_bid) / t.nbbo_bid
            ELSE NULL
        END as price_impact_pct,
        CASE 
            WHEN t.size >= 10000 THEN 'Block Trade'
            WHEN t.premium >= 0.02 THEN 'High Premium'
            ELSE 'Regular'
        END as trade_type,
        count(*) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as trades_per_hour,
        sum(t.size) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as volume_per_hour
    FROM trading.darkpool_trades t
    WHERE t.executed_at >= %(time_ago)s
      AND t.symbol = ANY(%(symbols)s)
    ORDER BY t.executed_at DESC
    """

    # Query for news headlines
    news_query = """
    SELECT 
        n.*,
        date_trunc('hour', n.published_at) as news_hour,
        CASE 
            WHEN n.impact_score >= 7 THEN 'High Impact'
            WHEN n.impact_score >= 4 THEN 'Medium Impact'
            ELSE 'Low Impact'
        END as impact_level,
        CASE 
            WHEN n.sentiment > 0.3 THEN 'Very Positive'
            WHEN n.sentiment > 0 THEN 'Positive'
            WHEN n.sentiment < -0.3 THEN 'Very Negative'
            WHEN n.sentiment < 0 THEN 'Negative'
            ELSE 'Neutral'
        END as sentiment_level,
        count(*) over (partition by date_trunc('hour', n.published_at)) as news_per_hour
    FROM trading.news_headlines n
    WHERE n.published_at >= %(time_ago)s
      AND n.symbols && %(symbols)s
    ORDER BY n.published_at DESC
    """

    # Query for options flow
    options_query = """
    SELECT 
        f.*,
        date_trunc('hour', f.collected_at) as flow_hour,
        CASE 
            WHEN f.premium >= 1000000 THEN 'Whale'
            WHEN f.premium >= 100000 THEN 'Large'
            ELSE 'Regular'
        END as flow_size,
        count(*) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as flows_per_hour,
        sum(f.premium) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as premium_per_hour,
        sum(f.contract_size) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as contracts_per_hour
    FROM trading.options_flow f
    WHERE f.collected_at >= %(time_ago)s
      AND f.symbol = ANY(%(symbols)s)
    ORDER BY f.collected_at DESC
    """

    # Create data directory
    os.makedirs('data', exist_ok=True)

    # Generate filenames with current timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    darkpool_filename = f'data/darkpool_trades_{hours}h_{timestamp}.csv'
    news_filename = f'data/news_headlines_{hours}h_{timestamp}.csv'
    options_filename = f'data/options_flow_{hours}h_{timestamp}.csv'

    # Use a single connection for all queries
    with psycopg2.connect(get_connection_string()) as conn:
        # Fetch all datasets
        print(f"Fetching dark pool trades from last {hours} hours...")
        trades_df = pd.read_sql_query(
            darkpool_query, conn, params={'time_ago': time_ago, 'symbols': symbols}
        )

        print(f"Fetching news headlines from last {hours} hours...")
        news_df = pd.read_sql_query(
            news_query, conn, params={'time_ago': time_ago, 'symbols': symbols}
        )

        print(f"Fetching options flow data from last {hours} hours...")
        options_df = pd.read_sql_query(
            options_query, conn, params={'time_ago': time_ago, 'symbols': symbols}
        )

    # Process timestamps
    for df, cols in [
        (trades_df, ['executed_at', 'collection_time', 'trade_hour']),
        (news_df, ['published_at', 'collected_at', 'news_hour']),
        (options_df, ['collected_at', 'created_at', 'expiry', 'flow_hour'])
    ]:
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

    # Save all datasets
    trades_df.to_csv(darkpool_filename, index=False)
    news_df.to_csv(news_filename, index=False)
    options_df.to_csv(options_filename, index=False)

    print(f"\nSaved {len(trades_df)} trades to {darkpool_filename}")
    print(f"Saved {len(news_df)} news headlines to {news_filename}")
    print(f"Saved {len(options_df)} option flows to {options_filename}")

    # Print summaries
    print("\nDarkpool Trade summary by symbol:")
    print(trades_df.groupby('symbol').agg({
        'size': ['count', 'sum', 'mean'],
        'premium': ['mean', 'max'],
        'price_impact_pct': 'mean'
    }).round(2))

    print("\nNews Headlines summary:")
    print(news_df.groupby('sentiment_level').agg({
        'headline': 'count',
        'impact_score': ['mean', 'max'],
        'sentiment': 'mean'
    }).round(2))

    print("\nOptions Flow summary by symbol:")
    print(options_df.groupby('symbol').agg({
        'premium': ['count', 'sum', 'mean', 'max'],
        'contract_size': ['sum', 'mean'],
        'iv_rank': 'mean'
    }).round(2))

    return {
        'trades': trades_df,
        'news': news_df,
        'options': options_df,
        'files': {
            'trades': darkpool_filename,
            'news': news_filename,
            'options': options_filename
        }
    }

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Fetch market data and save to CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--symbols', nargs='+', default=["SPY", "QQQ", "GLD"],
                      help='List of symbols to fetch data for')
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours of data to fetch')
    
    args = parser.parse_args()
    fetch_market_data(args.symbols, args.hours)

if __name__ == '__main__':
    main() 