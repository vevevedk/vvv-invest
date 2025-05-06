from datetime import datetime, timedelta
import pytz
from flow_analysis.scripts.darkpool_collector import DarkPoolCollector
import time

def main():
    collector = DarkPoolCollector()
    collector.connect_db()
    
    # Set time range for last 24 hours
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(days=1)
    
    print(f"Starting QQQ backfill from {start_time} to {end_time}")
    
    current_time = start_time
    total_trades = 0
    
    while current_time < end_time:
        try:
            # Make API request
            endpoint = f"{collector.UW_BASE_URL}{collector.DARKPOOL_RECENT_ENDPOINT}?symbol=QQQ"
            response = collector._make_request(endpoint)
            
            if response is None or not response.get('data'):
                print("No trades data received")
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
                print(f"Saved {len(trades)} QQQ trades for {current_time}")
            
            # Move to next time window
            current_time += timedelta(minutes=5)
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"Error processing trades: {str(e)}")
            time.sleep(1)
            continue
    
    print(f"Backfill complete. Total QQQ trades saved: {total_trades}")

if __name__ == '__main__':
    main() 