import logging
from collectors.darkpool_collector import DarkPoolCollector
from datetime import datetime, timedelta
import pytz
import time
from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.watchlist import SYMBOLS
from collectors.utils.market_utils import is_market_open, get_next_market_open
import psycopg2
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_to_db(collector_name, level, message):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            sslmode=os.getenv('DB_SSLMODE', 'prefer')
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO trading.collector_logs (timestamp, collector_name, level, message) VALUES (%s, %s, %s, %s)",
            (datetime.utcnow(), collector_name, level, message)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to log to DB: {e}")

def run_darkpool_collector(hours: int = 24):
    """Run the dark pool collector to fetch and process dark pool trades for the last N hours."""
    try:
        logger.info(f"Starting dark pool collector for last {hours} hours...")
        
        # Check if market is open
        if not is_market_open():
            next_open = get_next_market_open()
            msg = f"Market is closed. Next market open: {next_open}"
            logger.info(msg)
            log_to_db('darkpool', 'INFO', msg)
            return {"status": "market_closed", "next_open": next_open.isoformat()}
        
        collector = DarkPoolCollector()
        results = collector.collect_trades()
        logger.info(f"Dark pool collector completed successfully. Results: {results}")
        return {"status": "success", "results": results}
    except Exception as e:
        logger.error(f"Error in dark pool collector: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}

def backfill_qqq_trades(start_time: datetime = None, end_time: datetime = None):
    """Backfill QQQ trades for a specific time period."""
    try:
        logger.info("Starting QQQ trades backfill...")
        collector = DarkPoolCollector()
        collector.connect_db()
        
        # If no times provided, default to last 24 hours
        if not start_time:
            start_time = datetime.now(pytz.UTC) - timedelta(days=1)
        if not end_time:
            end_time = datetime.now(pytz.UTC)
        
        logger.info(f"Starting QQQ backfill from {start_time} to {end_time}")
        
        current_time = start_time
        total_trades = 0
        
        while current_time < end_time:
            try:
                # Make API request
                endpoint = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}?symbol=QQQ"
                response = collector._make_request(endpoint)
                
                if response is None or not response.get('data'):
                    logger.warning("No trades data received")
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
                    logger.info(f"Saved {len(trades)} QQQ trades for {current_time}")
                
                # Move to next time window
                current_time += timedelta(minutes=5)
                
                # Rate limiting
                time.sleep(REQUEST_RATE_LIMIT)
                
            except Exception as e:
                logger.error(f"Error processing trades: {str(e)}")
                time.sleep(REQUEST_RATE_LIMIT)
                continue
        
        logger.info(f"Backfill complete. Total QQQ trades saved: {total_trades}")
        return total_trades
    except Exception as e:
        logger.error(f"Fatal error in backfill task: {str(e)}")
        raise 