#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from config.db_config import get_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using configuration."""
    db_config = get_db_config()
    return create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}",
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
    )

def export_recent_trades(hours=24):
    """Export recent dark pool trades."""
    try:
        engine = get_db_connection()
        end_time = datetime.now()
        # Add 5 minutes to ensure we get the latest data
        end_time = end_time + timedelta(minutes=5)
        start_time = end_time - timedelta(hours=hours)
        
        logger.info(f"Fetching trades from {start_time} to {end_time}")
        
        query = text("""
            SELECT 
                t.*,
                EXTRACT(HOUR FROM t.executed_at) as trade_hour,
                CASE 
                    WHEN t.price > t.nbbo_ask THEN 'Above Ask'
                    WHEN t.price < t.nbbo_bid THEN 'Below Bid'
                    ELSE 'Between'
                END as price_impact,
                CASE 
                    WHEN t.size > 10000 THEN 'Large'
                    WHEN t.size > 5000 THEN 'Medium'
                    ELSE 'Small'
                END as trade_size
            FROM trading.darkpool_trades t
            WHERE t.executed_at >= :start_time
            AND t.executed_at <= :end_time
            ORDER BY t.executed_at DESC
        """)
        
        df = pd.read_sql(
            query, 
            engine, 
            params={'start_time': start_time, 'end_time': end_time}
        )
        
        if df.empty:
            logger.warning("No trades found in the specified time range")
            return None
        
        # Create exports directory if it doesn't exist
        exports_dir = Path('exports')
        exports_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = exports_dir / f'darkpool_trades_{timestamp}.csv'
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        # Print summary
        logger.info(f"\nDark Pool Trades Export Summary:")
        logger.info("=" * 50)
        logger.info(f"Total Trades: {len(df)}")
        
        # Safely get unique symbols
        if 'ticker' in df.columns:
            unique_symbols = df['ticker'].unique()
            logger.info(f"Unique Symbols: {len(unique_symbols)}")
            
            # Print trades by symbol
            logger.info("\nTrades by Symbol:")
            for symbol in unique_symbols:
                symbol_trades = df[df['ticker'] == symbol]
                volume = symbol_trades['volume'].sum() if 'volume' in df.columns else 0
                logger.info(f"{symbol}: {len(symbol_trades)} trades, Volume: {volume:,}")
        
        # Safely get date range
        if 'executed_at' in df.columns:
            logger.info(f"Date Range: {df['executed_at'].min()} to {df['executed_at'].max()}")
            
            # Show most recent trades
            logger.info("\nMost Recent Trades:")
            recent_trades = df.head(5)  # Show last 5 trades
            for _, trade in recent_trades.iterrows():
                ticker = trade.get('ticker', 'N/A')
                size = trade.get('size', 0)
                price = trade.get('price', 0)
                executed_at = trade.get('executed_at', 'N/A')
                logger.info(f"{executed_at} - {ticker}: {size:,} shares @ ${price:,.2f}")
        
        # Safely get total volume
        if 'volume' in df.columns:
            logger.info(f"Total Volume: {df['volume'].sum():,}")
        
        logger.info(f"\nExported to: {filename}")
        
        return filename
        
    except Exception as e:
        logger.error(f"Error exporting trades: {str(e)}")
        raise

def export_recent_news(hours=24):
    """Export recent news headlines."""
    try:
        engine = get_db_connection()
        end_time = datetime.now()
        # Add 5 minutes to ensure we get the latest data
        end_time = end_time + timedelta(minutes=5)
        start_time = end_time - timedelta(hours=hours)
        
        logger.info(f"Fetching news from {start_time} to {end_time}")
        
        query = text("""
            SELECT *
            FROM trading.news_headlines
            WHERE created_at >= :start_time
            AND created_at <= :end_time
            ORDER BY created_at DESC
        """)
        
        df = pd.read_sql(
            query, 
            engine, 
            params={'start_time': start_time, 'end_time': end_time}
        )
        
        if df.empty:
            logger.warning("No news headlines found in the specified time range")
            return None
        
        # Create exports directory if it doesn't exist
        exports_dir = Path('exports')
        exports_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = exports_dir / f'news_headlines_{timestamp}.csv'
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        # Print summary
        logger.info(f"\nNews Headlines Export Summary:")
        logger.info("=" * 50)
        logger.info(f"Total Headlines: {len(df)}")
        
        # Safely get date range
        if 'created_at' in df.columns:
            logger.info(f"Date Range: {df['created_at'].min()} to {df['created_at'].max()}")
        
        # Safely get sources
        if 'source' in df.columns:
            sources = df['source'].unique()
            logger.info(f"Sources: {', '.join(sources)}")
            
            # Print headlines by source
            logger.info("\nHeadlines by Source:")
            for source in sources:
                source_news = df[df['source'] == source]
                logger.info(f"{source}: {len(source_news)} headlines")
        
        logger.info(f"\nExported to: {filename}")
        
        return filename
        
    except Exception as e:
        logger.error(f"Error exporting news: {str(e)}")
        raise

def main():
    """Main function to export recent data."""
    try:
        logger.info("Starting data export...")
        
        # Export trades
        trades_file = export_recent_trades()
        
        # Export news
        news_file = export_recent_news()
        
        logger.info("\nExport completed successfully!")
        if trades_file:
            logger.info(f"Trades file: {trades_file}")
        if news_file:
            logger.info(f"News file: {news_file}")
        
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 