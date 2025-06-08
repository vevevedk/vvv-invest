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
from psycopg2.extras import execute_values
from pytz import timezone

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/darkpool_gaps.log')
    ]
)
logger = logging.getLogger(__name__)

# US market hours
MARKET_OPEN = 9
MARKET_CLOSE = 16
EASTERN = timezone('US/Eastern')

# List of US market holidays for 2025 (add more as needed)
US_MARKET_HOLIDAYS_2025 = [
    datetime(2025, 1, 1),   # New Year's Day
    datetime(2025, 1, 20),  # Martin Luther King Jr. Day
    datetime(2025, 2, 17),  # Presidents' Day
    datetime(2025, 4, 18),  # Good Friday
    datetime(2025, 5, 26),  # Memorial Day
    datetime(2025, 7, 4),   # Independence Day
    datetime(2025, 9, 1),   # Labor Day
    datetime(2025, 11, 27), # Thanksgiving
    datetime(2025, 12, 25), # Christmas
]
US_MARKET_HOLIDAYS_2025 = [EASTERN.localize(dt) for dt in US_MARKET_HOLIDAYS_2025]

def is_market_day(dt):
    dt_eastern = dt.astimezone(EASTERN)
    if dt_eastern.weekday() >= 5:
        return False
    for holiday in US_MARKET_HOLIDAYS_2025:
        if dt_eastern.date() == holiday.date():
            return False
    return True

def get_market_hours_range(start_time, end_time):
    """
    Return a DatetimeIndex of all market hours (hourly) between start_time and end_time.
    """
    hours = []
    current = start_time.astimezone(EASTERN)
    end = end_time.astimezone(EASTERN)
    
    print(f"\nAnalyzing market hours from {current} to {end} (Eastern Time)")
    
    while current < end:
        if is_market_day(current):
            # Market hours: 9:30 to 16:00
            if (current.hour >= MARKET_OPEN and current.hour < MARKET_CLOSE) or \
               (current.hour == MARKET_OPEN and current.minute >= 30):
                # Include the hour if we're past 9:30
                if current.hour > MARKET_OPEN or (current.hour == MARKET_OPEN and current.minute >= 30):
                    market_hour = current.astimezone(pytz.UTC).replace(minute=0, second=0, microsecond=0)
                    hours.append(market_hour)
                    print(f"  Including market hour: {market_hour} (ET: {current})")
        current += timedelta(hours=1)
    
    print(f"Found {len(hours)} market hours")
    return pd.DatetimeIndex(hours)

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

def analyze_gaps(symbols=['SPY', 'QQQ'], hours=24):
    """
    Analyze gaps in darkpool data for the last N hours, considering only market hours.
    """
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(hours=hours)
    
    print(f"\nAnalyzing gaps from {start_time} to {end_time} (UTC)")
    print(f"Current time in ET: {end_time.astimezone(EASTERN)}")
    
    gaps = {}
    
    try:
        with get_db_connection() as conn:
            for symbol in symbols:
                print(f"\nAnalyzing {symbol}...")
                
                # Get all trades for the symbol in the time range
                query = """
                    SELECT 
                        date_trunc('hour', executed_at) as hour,
                        COUNT(*) as trade_count
                    FROM trading.darkpool_trades
                    WHERE symbol = %s
                    AND executed_at >= %s
                    AND executed_at < %s
                    GROUP BY date_trunc('hour', executed_at)
                    ORDER BY hour
                """
                
                df = pd.read_sql_query(
                    query, 
                    conn, 
                    params=(symbol, start_time, end_time),
                    parse_dates=['hour']
                )
                
                if not df.empty:
                    print(f"Found {len(df)} hours with data")
                    print("Sample hours with data:")
                    print(df.head())
                
                # Only consider market hours
                all_market_hours = get_market_hours_range(start_time, end_time)
                
                if df.empty:
                    gaps[symbol] = {
                        'total_hours': len(all_market_hours),
                        'missing_hours': len(all_market_hours),
                        'gaps': [(all_market_hours[0], all_market_hours[-1] + timedelta(hours=1))] if len(all_market_hours) > 0 else [],
                        'coverage': 0.0
                    }
                    continue
                
                # Find missing market hours
                existing_hours = pd.DatetimeIndex(df['hour'])
                missing_hours = all_market_hours.difference(existing_hours)
                
                print(f"\nMissing hours for {symbol}:")
                for hour in missing_hours:
                    print(f"  {hour} (ET: {hour.astimezone(EASTERN)})")
                
                # Calculate gaps
                gap_ranges = []
                if len(missing_hours) > 0:
                    gap_start = missing_hours[0]
                    for i in range(1, len(missing_hours)):
                        if (missing_hours[i] - missing_hours[i-1]).total_seconds() > 3600:
                            gap_ranges.append((gap_start, missing_hours[i-1] + timedelta(hours=1)))
                            gap_start = missing_hours[i]
                    gap_ranges.append((gap_start, missing_hours[-1] + timedelta(hours=1)))
                
                gaps[symbol] = {
                    'total_hours': len(all_market_hours),
                    'missing_hours': len(missing_hours),
                    'gaps': gap_ranges,
                    'coverage': (len(all_market_hours) - len(missing_hours)) / len(all_market_hours) * 100 if len(all_market_hours) > 0 else 0.0
                }
                
    except Exception as e:
        logger.error(f"Error analyzing gaps: {str(e)}")
        raise
    
    return gaps

def print_gap_analysis(gaps):
    """Print a formatted analysis of the gaps."""
    print("\nDark Pool Data Gap Analysis")
    print("=" * 50)
    
    for symbol, data in gaps.items():
        print(f"\n{symbol}:")
        print(f"Total Hours: {data['total_hours']}")
        print(f"Missing Hours: {data['missing_hours']}")
        print(f"Coverage: {data['coverage']:.1f}%")
        
        if data['gaps']:
            print("\nGaps to fill:")
            for start, end in data['gaps']:
                duration = (end - start).total_seconds() / 3600
                print(f"  {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')} ({duration:.1f} hours)")
        else:
            print("\nNo gaps found!")
    
    print("\n" + "=" * 50)

def main():
    """Main function to run the gap analysis."""
    # Load environment variables
    env_file = os.getenv('ENV_FILE', '.env')
    load_dotenv(env_file)
    
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    # Analyze gaps for the last 24 hours
    gaps = analyze_gaps()
    
    # Print the analysis
    print_gap_analysis(gaps)

if __name__ == "__main__":
    main() 