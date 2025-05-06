from collectors.darkpool_collector import DarkPoolCollector
from datetime import datetime, timedelta
import pytz
import time

def run_darkpool_collector():
    collector = DarkPoolCollector()
    collector.run()

def backfill_qqq_trades(start_time: datetime = None, end_time: datetime = None):
    """Backfill QQQ trades for a specific time period."""
    collector = DarkPoolCollector()
    collector.connect_db()
    
    # If no times provided, default to last 24 hours
    if not start_time:
        start_time = datetime.now(pytz.UTC) - timedelta(days=1)
    if not end_time:
        end_time = datetime.now(pytz.UTC)
    
    current_time = start_time
    total_trades = 0
    
    while current_time < end_time:
        try:
            # Make API request
            endpoint = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}?symbol=QQQ"
            response = collector._make_request(endpoint)
            
            if response is None or not response.get('data'):
                collector.logger.warning("No trades data received")
                time.sleep(REQUEST_RATE_LIMIT)
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
                collector.logger.info(f"Saved {len(trades)} QQQ trades for {current_time}")
            
            # Move to next time window
            current_time += timedelta(minutes=5)
            
            # Rate limiting
            time.sleep(REQUEST_RATE_LIMIT)
            
        except Exception as e:
            collector.logger.error(f"Error processing trades: {str(e)}")
            time.sleep(REQUEST_RATE_LIMIT)
            continue
    
    collector.logger.info(f"Backfill complete. Total QQQ trades saved: {total_trades}")
    return total_trades

# This will be decorated by celery_app.py 