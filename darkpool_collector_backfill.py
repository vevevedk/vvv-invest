from datetime import datetime, timedelta
import pytz
import time
import os
import argparse
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env.prod file
load_dotenv('.env.prod')

# Verify required environment variables
required_vars = ['UW_API_TOKEN', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

from flow_analysis.scripts.darkpool_collector import DarkPoolCollector

def backfill_symbol(collector, symbol, start_time, end_time):
    """Backfill trades for a specific symbol."""
    print(f"\nStarting backfill for {symbol} from {start_time} to {end_time}")
    
    current_time = start_time
    total_trades = 0
    retry_count = 0
    max_retries = 3
    
    while current_time < end_time:
        try:
            # Make API request
            endpoint = f"{collector.UW_BASE_URL}{collector.DARKPOOL_RECENT_ENDPOINT}?symbol={symbol}"
            response = collector._make_request(endpoint)
            
            if response is None or not response.get('data'):
                print(f"No trades data received for {symbol} at {current_time}")
                time.sleep(1)  # Rate limiting
                continue
            
            # Process trades
            trades = collector._process_trades(response['data'])
            
            # Filter trades by time
            trades = trades[trades['executed_at'] >= current_time]
            trades = trades[trades['executed_at'] < current_time + timedelta(minutes=5)]
            
            if not trades.empty:
                # Save to database
                collector.save_trades_to_db(trades)
                total_trades += len(trades)
                print(f"Saved {len(trades)} {symbol} trades for {current_time}")
                retry_count = 0  # Reset retry count on success
            else:
                print(f"No trades found for {symbol} in time window {current_time}")
            
            # Move to next time window
            current_time += timedelta(minutes=5)
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"Error processing trades for {symbol}: {str(e)}")
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Max retries reached for {symbol} at {current_time}, moving to next time window")
                current_time += timedelta(minutes=5)
                retry_count = 0
            time.sleep(1)
            continue
    
    print(f"Backfill complete for {symbol}. Total trades saved: {total_trades}")
    return total_trades

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Backfill dark pool trades for specified symbols')
    parser.add_argument('--symbols', nargs='+', default=['SPY', 'QQQ', 'GLD'],
                      help='List of symbols to backfill (default: SPY QQQ GLD)')
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours to look back (default: 24)')
    args = parser.parse_args()
    
    collector = DarkPoolCollector()
    collector.connect_db()
    
    # Set time range
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(hours=args.hours)
    
    print(f"Starting backfill from {start_time} to {end_time}")
    print(f"Symbols to process: {', '.join(args.symbols)}")
    
    # Backfill each symbol
    results = {}
    
    for symbol in args.symbols:
        try:
            total_trades = backfill_symbol(collector, symbol, start_time, end_time)
            results[symbol] = total_trades
        except Exception as e:
            print(f"Fatal error processing {symbol}: {str(e)}")
            results[symbol] = -1
    
    # Print summary
    print("\nBackfill Summary:")
    print("-" * 50)
    for symbol, count in results.items():
        status = f"{count} trades" if count >= 0 else "Failed"
        print(f"{symbol}: {status}")
    print("-" * 50)

if __name__ == '__main__':
    main() 