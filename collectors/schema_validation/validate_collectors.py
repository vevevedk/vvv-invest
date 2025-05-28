#!/usr/bin/env python3

"""
Script to validate news and dark pool collectors and save their data to CSV files
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine, text
from pathlib import Path
from flow_analysis.config.db_config import get_db_config

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Create database URL
DATABASE_URL = f"postgresql://{get_db_config()['user']}:{get_db_config()['password']}@{get_db_config()['host']}:{get_db_config()['port']}/{get_db_config()['dbname']}"

# Create engine with SSL required
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'sslmode': 'require'
    }
)

def validate_and_export_data():
    """Validate collectors and export data to CSV files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    exports_dir = Path('exports')
    exports_dir.mkdir(exist_ok=True)

    try:
        # Validate and export news data
        print("\nValidating News Collector...")
        news_query = text("""
            SELECT 
                headline,
                published_at,
                source,
                url,
                symbols,
                sentiment,
                impact_score,
                collected_at
            FROM trading.news_headlines
            WHERE collected_at >= NOW() - INTERVAL '24 hours'
            ORDER BY collected_at DESC
        """)
        
        news_df = pd.read_sql(news_query, engine)
        
        if not news_df.empty:
            print(f"\nFound {len(news_df)} news items in the last 24 hours")
            print("\nDate ranges:")
            print(f"Earliest: {news_df['collected_at'].min()}")
            print(f"Latest: {news_df['collected_at'].max()}")
            
            # Save to CSV
            news_csv_path = exports_dir / f'news_data_{timestamp}.csv'
            news_df.to_csv(news_csv_path, index=False)
            print(f"\nSaved news data to: {news_csv_path}")
            
            # Print summary statistics
            print("\nNews Summary:")
            print(f"Total headlines: {len(news_df)}")
            print("\nBreakdown by source:")
            print(news_df['source'].value_counts())
            print("\nImpact levels:")
            print(news_df['impact_score'].apply(lambda x: 'High Impact' if x > 0.7 else 'Low Impact').value_counts())
            print("\nSentiment categories:")
            print(news_df['sentiment'].apply(lambda x: 'Bullish' if x > 0.1 else 'Bearish' if x < -0.1 else 'Neutral').value_counts())
        else:
            print("\nNo news items found in the last 24 hours")

        # Validate and export dark pool trades
        print("\nValidating Dark Pool Collector...")
        trades_query = text("""
            SELECT 
                tracking_id,
                symbol,
                size,
                price,
                volume,
                premium,
                executed_at,
                nbbo_ask,
                nbbo_bid,
                market_center,
                sale_cond_codes,
                collection_time
            FROM trading.darkpool_trades
            WHERE collection_time >= NOW() - INTERVAL '24 hours'
            ORDER BY collection_time DESC
        """)
        
        trades_df = pd.read_sql(trades_query, engine)
        
        if not trades_df.empty:
            print(f"\nFound {len(trades_df)} dark pool trades in the last 24 hours")
            print("\nDate ranges:")
            print(f"Earliest trade: {trades_df['executed_at'].min()}")
            print(f"Latest trade: {trades_df['executed_at'].max()}")
            print(f"Total trades: {len(trades_df):,}")
            print(f"Total volume: {trades_df['volume'].sum():,}")
            
            # Save to CSV
            trades_csv_path = exports_dir / f'darkpool_trades_{timestamp}.csv'
            trades_df.to_csv(trades_csv_path, index=False)
            print(f"\nSaved dark pool trades to: {trades_csv_path}")
            
            # Print summary statistics
            print("\nDark Pool Summary:")
            print("\nTrades by symbol:")
            print(trades_df['symbol'].value_counts())
            print("\nTotal volume by symbol:")
            print(trades_df.groupby('symbol')['volume'].sum().sort_values(ascending=False))
            print("\nAverage trade size by symbol:")
            print(trades_df.groupby('symbol')['size'].mean().sort_values(ascending=False))
        else:
            print("\nNo dark pool trades found in the last 24 hours")

    except Exception as e:
        print(f"Error validating collectors: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    validate_and_export_data() 