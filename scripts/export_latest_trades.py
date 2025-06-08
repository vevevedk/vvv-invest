#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging
from dotenv import load_dotenv
import psycopg2

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        sslmode=os.getenv('DB_SSLMODE', 'require')
    )

def export_selected_trades():
    """Export trades for SPY, QQQ, and TSLA."""
    # Create exports directory if it doesn't exist
    exports_dir = Path('exports')
    exports_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = exports_dir / f'darkpool_trades_selected_{timestamp}.csv'
    
    try:
        with get_db_connection() as conn:
            # Get trades for selected symbols
            query = """
                SELECT *
                FROM trading.darkpool_trades
                WHERE symbol IN ('SPY', 'QQQ', 'TSLA')
                ORDER BY executed_at DESC
            """
            
            df = pd.read_sql_query(
                query,
                conn,
                parse_dates=['executed_at', 'collection_time']
            )
            
            if df.empty:
                logger.info("No trades found for selected symbols")
                return
            
            # Calculate days to settlement
            df['days_to_settlement'] = None
            
            # For trades with T+2 settlement (most common)
            t2_mask = df['trade_settlement'].str.lower().isin(['t+2', 't+2 settlement', 'cash'])
            if t2_mask.any():
                df.loc[t2_mask, 'days_to_settlement'] = 2
            
            # For trades with T+1 settlement
            t1_mask = df['trade_settlement'].str.lower().isin(['t+1', 't+1 settlement'])
            if t1_mask.any():
                df.loc[t1_mask, 'days_to_settlement'] = 1
            
            # For trades with T+3 settlement
            t3_mask = df['trade_settlement'].str.lower().isin(['t+3', 't+3 settlement'])
            if t3_mask.any():
                df.loc[t3_mask, 'days_to_settlement'] = 3
            
            # Save to CSV
            df.to_csv(filename, index=False)
            logger.info(f"Exported {len(df)} trades to {filename}")
            
            # Print summary
            print("\nExport Summary:")
            print("=" * 50)
            print(f"Total Trades: {len(df)}")
            print(f"Time Range: {df['executed_at'].min()} to {df['executed_at'].max()}")
            print(f"Total Volume: {df['size'].sum():,} shares")
            print(f"Total Value: ${(df['price'] * df['size']).sum():,.2f}")
            
            # Print summary by symbol
            for symbol in ['SPY', 'QQQ', 'TSLA']:
                symbol_df = df[df['symbol'] == symbol]
                if not symbol_df.empty:
                    print(f"\n{symbol}:")
                    print(f"Total Trades: {len(symbol_df)}")
                    print(f"Time Range: {symbol_df['executed_at'].min()} to {symbol_df['executed_at'].max()}")
                    print(f"Total Volume: {symbol_df['size'].sum():,} shares")
                    print(f"Total Value: ${(symbol_df['price'] * symbol_df['size']).sum():,.2f}")
                    
                    # Print largest trades
                    largest_trades = symbol_df.nlargest(3, 'size')
                    print("\nLargest Trades:")
                    for _, trade in largest_trades.iterrows():
                        print(f"Time: {trade['executed_at']}, Size: {trade['size']:,}, Price: ${trade['price']:.2f}, Value: ${trade['size'] * trade['price']:,.2f}, DTS: {trade['days_to_settlement']}")
                else:
                    print(f"\n{symbol}: No trades found")
            
    except Exception as e:
        logger.error(f"Error exporting trades: {str(e)}")
        raise

def main():
    """Main function to run the export."""
    # Load environment variables
    env_file = os.getenv('ENV_FILE', '.env')
    load_dotenv(env_file)
    
    # Export selected trades
    export_selected_trades()

if __name__ == "__main__":
    main() 