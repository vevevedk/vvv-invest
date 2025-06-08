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
from collections import defaultdict

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

def get_market_hours(start_date, end_date):
    """Get list of market hours between start and end date."""
    market_hours = []
    current = start_date
    
    while current <= end_date:
        # Skip weekends
        if current.weekday() < 5:  # 0-4 are weekdays
            # Convert date to datetime for market hours
            current_dt = datetime.combine(current, datetime.min.time())
            # Market hours are 9:30 AM to 4:00 PM ET
            market_open = current_dt.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = current_dt.replace(hour=16, minute=0, second=0, microsecond=0)
            market_hours.append((market_open, market_close))
        current += timedelta(days=1)
    
    return market_hours

def analyze_data_completeness():
    """Analyze data completeness and generate backfill commands."""
    try:
        with get_db_connection() as conn:
            # Get trades for selected symbols
            query = """
                SELECT 
                    symbol,
                    date_trunc('hour', executed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') as hour,
                    COUNT(*) as trade_count
                FROM trading.darkpool_trades
                WHERE symbol IN ('SPY', 'QQQ', 'TSLA')
                GROUP BY symbol, date_trunc('hour', executed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
                ORDER BY symbol, hour
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                logger.info("No trades found for selected symbols")
                return
            
            # Convert hour to datetime (assume naive, localize to NY)
            eastern = pytz.timezone('America/New_York')
            if df['hour'].dt.tz is None:
                df['hour'] = df['hour'].apply(lambda x: eastern.localize(x))
            else:
                df['hour'] = df['hour'].dt.tz_convert('America/New_York')
            
            # Get overall time range
            start_date = df['hour'].min().date()
            end_date = df['hour'].max().date()
            
            print("\nData Completeness Analysis:")
            print("=" * 50)
            print(f"Analysis Period: {start_date} to {end_date}")
            
            # Analyze each symbol
            for symbol in ['SPY', 'QQQ', 'TSLA']:
                symbol_df = df[df['symbol'] == symbol]
                
                if symbol_df.empty:
                    print(f"\n{symbol}: No trades found")
                    continue
                
                # Get market hours for the period
                market_hours = get_market_hours(start_date, end_date)
                
                # Create a set of all market hours
                all_hours = set()
                for market_open, market_close in market_hours:
                    current = market_open
                    while current <= market_close:
                        # Properly localize to NY timezone
                        current_tz = eastern.localize(current)
                        all_hours.add(current_tz)
                        current += timedelta(hours=1)
                
                # Get hours with data
                hours_with_data = set(symbol_df['hour'].dt.floor('h'))
                
                # Debug prints
                print(f"\n{symbol} - Sample of generated market hours:")
                for h in sorted(all_hours)[:5]:
                    print(f"  {h}")
                print(f"\n{symbol} - Sample of hours with data:")
                for h in sorted(hours_with_data)[:5]:
                    print(f"  {h}")
                
                # Find missing hours
                missing_hours = all_hours - hours_with_data
                
                # Group missing hours into continuous periods
                missing_periods = []
                if missing_hours:
                    sorted_hours = sorted(missing_hours)
                    period_start = sorted_hours[0]
                    period_end = sorted_hours[0]
                    
                    for hour in sorted_hours[1:]:
                        if hour == period_end + timedelta(hours=1):
                            period_end = hour
                        else:
                            missing_periods.append((period_start, period_end))
                            period_start = hour
                            period_end = hour
                    
                    missing_periods.append((period_start, period_end))
                
                # Print analysis
                print(f"\n{symbol}:")
                print(f"Total Trades: {symbol_df['trade_count'].sum():,}")
                print(f"Hours with Data: {len(hours_with_data):,}")
                print(f"Total Market Hours: {len(all_hours):,}")
                print(f"Missing Hours: {len(missing_hours):,}")
                
                if missing_periods:
                    print("\nMissing Periods:")
                    for start, end in missing_periods:
                        duration = end - start
                        hours = duration.total_seconds() / 3600
                        print(f"  {start} to {end} ({hours:.1f} hours)")
                        
                        # Generate backfill command
                        if hours <= 24:
                            print(f"  Command: python collectors/darkpool/darkpool_collector.py --backfill --hours {int(hours)} --symbol {symbol}")
                        else:
                            # Split into 24-hour chunks
                            current = start
                            while current < end:
                                chunk_end = min(current + timedelta(hours=24), end)
                                chunk_hours = (chunk_end - current).total_seconds() / 3600
                                print(f"  Command: python collectors/darkpool/darkpool_collector.py --backfill --hours {int(chunk_hours)} --symbol {symbol} --start-time {current.strftime('%Y-%m-%d %H:%M:%S')}")
                                current = chunk_end
                else:
                    print("No missing periods found")
            
    except Exception as e:
        logger.error(f"Error analyzing data completeness: {str(e)}")
        raise

def main():
    """Main function to run the analysis."""
    # Load environment variables
    env_file = os.getenv('ENV_FILE', '.env')
    load_dotenv(env_file)
    
    # Analyze data completeness
    analyze_data_completeness()

if __name__ == "__main__":
    main() 