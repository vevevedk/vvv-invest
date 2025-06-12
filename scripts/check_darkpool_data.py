#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

import psycopg2
from datetime import datetime
import pytz
from config.db_config import get_db_config

def get_latest_trades(symbol: str, limit: int = 5):
    """Get the latest trades for a specific symbol."""
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            sslmode=db_config['sslmode']
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    executed_at,
                    price,
                    size,
                    volume,
                    premium,
                    nbbo_ask,
                    nbbo_bid,
                    market_center,
                    sale_cond_codes
                FROM trading.darkpool_trades
                WHERE symbol = %s
                ORDER BY executed_at DESC
                LIMIT %s;
            """, (symbol, limit))
            
            trades = cur.fetchall()
            
            print(f"\nLatest {limit} trades for {symbol} (times shown in CEST):")
            print("-" * 120)
            print(f"{'Time (CEST)':25} | {'Price':10} | {'Size':8} | {'Volume':12} | {'Premium':10} | {'NBBO Ask':10} | {'NBBO Bid':10} | {'Market Center':15} | {'Sale Cond'}")
            print("-" * 120)
            cest = pytz.timezone('Europe/Copenhagen')
            for trade in trades:
                executed_at, price, size, volume, premium, nbbo_ask, nbbo_bid, market_center, sale_cond = trade
                # Convert to CEST
                if executed_at.tzinfo is None:
                    executed_at = pytz.UTC.localize(executed_at)
                executed_at_cest = executed_at.astimezone(cest)
                print(f"{executed_at_cest:%Y-%m-%d %H:%M:%S} | {price:10.2f} | {size:8,d} | {volume:12,.2f} | {premium:10.2f} | {nbbo_ask:10.2f} | {nbbo_bid:10.2f} | {market_center:15} | {sale_cond}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def check_darkpool_data():
    """Check the time range of collected darkpool data."""
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            sslmode=db_config['sslmode']
        )
        
        with conn.cursor() as cur:
            # Get time range and count
            cur.execute("""
                SELECT 
                    MIN(executed_at) as earliest,
                    MAX(executed_at) as latest,
                    COUNT(*) as total_trades,
                    COUNT(DISTINCT symbol) as unique_symbols
                FROM trading.darkpool_trades;
            """)
            
            result = cur.fetchone()
            earliest, latest, total_trades, unique_symbols = result
            
            # Get count by symbol
            cur.execute("""
                SELECT 
                    symbol,
                    COUNT(*) as trade_count,
                    MIN(executed_at) as earliest,
                    MAX(executed_at) as latest
                FROM trading.darkpool_trades
                GROUP BY symbol
                ORDER BY trade_count DESC;
            """)
            
            symbol_stats = cur.fetchall()
            
        print("\nDarkpool Data Summary:")
        print("-" * 50)
        print(f"Total Trades: {total_trades:,}")
        print(f"Unique Symbols: {unique_symbols}")
        print(f"Time Range: {earliest} to {latest}")
        print(f"Duration: {latest - earliest}")
        
        print("\nTrades by Symbol:")
        print("-" * 50)
        for symbol, count, sym_earliest, sym_latest in symbol_stats:
            print(f"{symbol:6} | {count:8,} trades | {sym_earliest} to {sym_latest}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # First show the summary
    check_darkpool_data()
    
    # Then show latest SPY trades
    get_latest_trades("SPY", limit=10)
    # Then show latest QQQ trades
    get_latest_trades("QQQ", limit=10) 