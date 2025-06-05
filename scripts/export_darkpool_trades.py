#!/usr/bin/env python3

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('export_darkpool_trades.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def export_darkpool_trades():
    """Export dark pool trades with enhanced metrics to CSV."""
    try:
        # Load environment variables
        env_file = os.getenv('ENV_FILE', '.env')
        load_dotenv(env_file)
        
        # Create exports directory if it doesn't exist
        exports_dir = Path('exports')
        exports_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = exports_dir / f'darkpool_trades_all_{timestamp}.csv'
        
        # Connect to database
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
        )
        
        # Query with enhanced metrics
        query = """
        SELECT 
            t.*,
            date_trunc('hour', t.executed_at) as trade_hour,
            t.price - t.nbbo_bid as price_impact,
            CASE 
                WHEN t.nbbo_bid IS NULL OR t.nbbo_bid = 0 THEN NULL
                ELSE (t.price - t.nbbo_bid) / t.nbbo_bid
            END as price_impact_pct,
            CASE 
                WHEN t.size >= 10000 THEN 'Block Trade'
                WHEN t.premium >= 1000000 THEN 'High Premium'
                ELSE 'Regular'
            END as trade_type,
            count(*) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as trades_per_hour,
            sum(t.size) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as volume_per_hour
        FROM trading.darkpool_trades t
        ORDER BY t.executed_at DESC
        """
        
        # Execute query and convert to DataFrame
        logger.info("Fetching dark pool trades from database...")
        df = pd.read_sql(query, engine)
        
        if df.empty:
            logger.warning("No trades found in database")
            return
        
        # Convert timestamp columns
        df['executed_at'] = pd.to_datetime(df['executed_at'])
        df['collection_time'] = pd.to_datetime(df['collection_time'])
        df['trade_hour'] = pd.to_datetime(df['trade_hour'])
        
        # Save to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} trades to {filename}")
        
        # Generate and print summary
        print("\nExport Summary:")
        print("=" * 50)
        print(f"Total Trades: {len(df):,}")
        print(f"Unique Symbols: {df['symbol'].nunique()}")
        print(f"Date Range: {df['executed_at'].min()} to {df['executed_at'].max()}")
        print(f"Total Volume: {df['size'].sum():,.0f}")
        print(f"Total Premium: ${df['premium'].sum():,.2f}")
        
        print("\nTrades by Symbol:")
        print("-" * 50)
        symbol_stats = df.groupby('symbol').agg({
            'size': ['count', 'sum', 'mean'],
            'premium': ['sum', 'mean'],
            'price_impact_pct': 'mean'
        }).round(2)
        print(symbol_stats)
        
        print("\nTrade Types:")
        print("-" * 50)
        print(df['trade_type'].value_counts())
        
        return filename
        
    except Exception as e:
        logger.error(f"Error exporting dark pool trades: {str(e)}")
        raise

if __name__ == "__main__":
    export_darkpool_trades() 