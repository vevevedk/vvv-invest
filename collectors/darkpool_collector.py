from collectors.base_collector import BaseCollector
import logging
import requests
import psycopg2
from datetime import datetime, timedelta, time as dt_time
import time
import pytz
from flow_analysis.config.api_config import UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
from flow_analysis.config.db_config import get_db_config
from flow_analysis.config.watchlist import SYMBOLS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from celery import Task
from celery.exceptions import MaxRetriesExceededError

class DarkPoolCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.ny_tz = pytz.timezone('America/New_York')
        self.symbols = SYMBOLS
        token = DEFAULT_HEADERS.get('Authorization', '').split()[-1]
        self.logger.info(f"API Token configured: {token[:6]}... (length: {len(token)})")
        self.logger.info(f"Loaded UW_API_TOKEN (first 6): {os.getenv('UW_API_TOKEN', '')[:6]}...")
        self.batch_size = 500
        self.max_parallel_requests = 2  # Reduced for better stability
        self.request_timeout = 30
        self.retry_delay = 2.0
        self.max_retries = 3
        self.cache = {}  # Cache for storing recent trades
        self.cache_ttl = 300  # Cache TTL in seconds (5 minutes)
        self.last_cache_update = {}
        self.task_timeout = 300  # 5 minutes max per task

    def _get_cached_trades(self, symbol, date_str):
        """Get trades from cache if available and not expired."""
        cache_key = f"{symbol}_{date_str}"
        if cache_key in self.cache:
            last_update = self.last_cache_update.get(cache_key, 0)
            if time.time() - last_update < self.cache_ttl:
                return self.cache[cache_key]
        return None

    def _update_cache(self, symbol, date_str, trades):
        """Update cache with new trades."""
        cache_key = f"{symbol}_{date_str}"
        self.cache[cache_key] = trades
        self.last_cache_update[cache_key] = time.time()

    def is_market_open(self):
        """Check if US stock market is currently open."""
        now = datetime.now(self.ny_tz)
        
        # Log current time in NY
        self.logger.info(f"Current NY time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            self.logger.info("Market is closed: Weekend")
            return False
            
        # Check if it's between 9:30 AM and 4:00 PM ET
        market_open = dt_time(9, 30)  # 9:30 AM
        market_close = dt_time(16, 0)  # 4:00 PM
        current_time = now.time()
        
        is_open = market_open <= current_time <= market_close
        if not is_open:
            self.logger.info(f"Market is closed: Current time {current_time} outside market hours {market_open}-{market_close}")
        return is_open

    def validate_trade(self, trade):
        """Validate trade data before saving."""
        required_fields = ['tracking_id', 'size', 'price', 'executed_at']
        missing_fields = [field for field in required_fields if not trade.get(field)]
        if missing_fields:
            self.logger.debug(f"Invalid trade: Missing fields {missing_fields}")
            return False
        return True

    def collect(self):
        """Default incremental collection with Celery task handling."""
        start_time = time.time()
        
        try:
            if not self.is_market_open():
                self.logger.info("Market is closed. Skipping collection.")
                return
                
            self.collect_darkpool_trades(incremental=True)
            
            execution_time = time.time() - start_time
            if execution_time > self.task_timeout:
                self.logger.warning(f"Task execution time ({execution_time:.2f}s) exceeded timeout ({self.task_timeout}s)")
                
        except Exception as e:
            self.logger.error(f"Error in dark pool collection: {str(e)}")
            raise

    def collect_darkpool_trades(self, start_date=None, end_date=None, incremental=True):
        """Collect dark pool trades with improved error handling and rate limiting."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        total_inserted = 0
        today = datetime.now(self.ny_tz).date()
        
        if incremental:
            date_list = [today]
        else:
            if not start_date or not end_date:
                self.logger.error("Backfill requires start_date and end_date.")
                return
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]

        for symbol in self.symbols:
            for date_obj in date_list:
                date_str = date_obj.strftime("%Y-%m-%d")
                
                # Check cache first
                cached_trades = self._get_cached_trades(symbol, date_str)
                if cached_trades:
                    self.logger.info(f"Using cached trades for {symbol} on {date_str}")
                    inserted = self._save_trades_to_db(symbol, cached_trades)
                    total_inserted += inserted
                    continue

                self.logger.info(f"Collecting dark pool trades for {symbol} on {date_str}")
                
                try:
                    # Get latest executed_at for this symbol and date
                    newer_than = self._get_latest_executed_at(symbol, date_str)
                    
                    # Collect trades with pagination
                    all_trades = self._collect_trades_with_pagination(session, symbol, date_str, newer_than)
                    
                    if all_trades:
                        # Update cache
                        self._update_cache(symbol, date_str, all_trades)
                        # Save to database
                        inserted = self._save_trades_to_db(symbol, all_trades)
                        total_inserted += inserted
                        self.logger.info(f"Successfully saved {inserted} trades to database")
                        
                except Exception as e:
                    self.logger.error(f"Error collecting trades for {symbol} on {date_str}: {str(e)}")
                    continue

        self.logger.info(f"Total trades inserted: {total_inserted}")

    def _get_latest_executed_at(self, symbol, date_str):
        """Get the latest executed_at timestamp for a symbol and date."""
        try:
            with psycopg2.connect(**get_db_config()) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT MAX(executed_at) FROM trading.darkpool_trades
                        WHERE symbol = %s AND executed_at::date = %s
                    """, (symbol, date_str))
                    result = cur.fetchone()
                    if result and result[0]:
                        return result[0].isoformat()
        except Exception as e:
            self.logger.error(f"DB error when fetching latest executed_at: {str(e)}")
        return None

    def _collect_trades_with_pagination(self, session, symbol, date_str, newer_than):
        """Collect trades with pagination and rate limiting."""
        all_trades = []
        page = 0
        
        while True:
            params = {
                "date": date_str,
                "limit": self.batch_size,
                "page": page
            }
            if newer_than:
                params["newer_than"] = newer_than

            url = f"{UW_BASE_URL}/darkpool/{symbol}"
            self.logger.info(f"Making request to: {url}")
            
            try:
                response = session.get(url, headers=DEFAULT_HEADERS, params=params, timeout=self.request_timeout)
                response.raise_for_status()
                data = response.json().get("data", [])
                
                if not data:
                    break
                    
                valid_trades = [t for t in data if self.validate_trade(t)]
                all_trades.extend(valid_trades)
                
                # If we got fewer trades than the batch size, we've reached the end
                if len(data) < self.batch_size:
                    break
                    
                # For next page, set newer_than to latest executed_at in this batch
                latest = max(t["executed_at"] for t in data if t.get("executed_at"))
                newer_than = latest
                page += 1
                time.sleep(self.retry_delay)  # Rate limiting between pages
                
            except Exception as e:
                self.logger.error(f"API error for {symbol} on {date_str}: {str(e)}")
                break

        return all_trades

    def _save_trades_to_db(self, symbol, trades):
        """Save trades to database with deduplication."""
        if not trades:
            self.logger.info(f"No trades to insert for {symbol}")
            return 0
        
        self.logger.info(f"Attempting to insert {len(trades)} trades for {symbol}")
        inserted = 0
        
        try:
            with psycopg2.connect(**get_db_config()) as conn:
                with conn.cursor() as cur:
                    # Create schema and table if they don't exist
                    cur.execute("""
                        CREATE SCHEMA IF NOT EXISTS trading;
                        
                        CREATE TABLE IF NOT EXISTS trading.darkpool_trades (
                            id SERIAL PRIMARY KEY,
                            tracking_id BIGINT NOT NULL UNIQUE,
                            symbol VARCHAR(10) NOT NULL,
                            price NUMERIC NOT NULL,
                            size INTEGER NOT NULL,
                            volume NUMERIC,
                            premium NUMERIC,
                            executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            nbbo_ask NUMERIC,
                            nbbo_bid NUMERIC,
                            nbbo_ask_quantity INTEGER,
                            nbbo_bid_quantity INTEGER,
                            market_center VARCHAR(50),
                            sale_cond_codes VARCHAR(50),
                            ext_hour_sold_codes VARCHAR(50),
                            trade_code VARCHAR(50),
                            trade_settlement VARCHAR(50),
                            canceled BOOLEAN,
                            collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    conn.commit()
                    
                    # Prepare batch insert
                    values = []
                    for trade in trades:
                        try:
                            # Convert executed_at to UTC if it's not already
                            executed_at = trade.get('executed_at')
                            if isinstance(executed_at, str):
                                executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
                            
                            values.append({
                                'tracking_id': trade.get('tracking_id'),
                                'symbol': symbol,
                                'size': trade.get('size'),
                                'price': trade.get('price'),
                                'volume': trade.get('volume'),
                                'premium': trade.get('premium'),
                                'executed_at': executed_at,
                                'nbbo_ask': trade.get('nbbo_ask'),
                                'nbbo_bid': trade.get('nbbo_bid'),
                                'market_center': trade.get('market_center'),
                                'sale_cond_codes': trade.get('sale_cond_codes'),
                                'ext_hour_sold_codes': trade.get('ext_hour_sold_codes'),
                                'trade_code': trade.get('trade_code'),
                                'trade_settlement': trade.get('trade_settlement'),
                                'canceled': trade.get('canceled', False),
                                'collection_time': datetime.utcnow(),
                            })
                        except Exception as e:
                            self.logger.error(f"Error preparing trade for insert: {str(e)}")
                            continue
                    
                    # Execute batch insert with deduplication
                    if values:
                        cur.executemany("""
                            INSERT INTO trading.darkpool_trades (
                                tracking_id, symbol, size, price, volume, premium, executed_at, 
                                nbbo_ask, nbbo_bid, market_center, sale_cond_codes, ext_hour_sold_codes, 
                                trade_code, trade_settlement, canceled, collection_time
                            ) VALUES (
                                %(tracking_id)s, %(symbol)s, %(size)s, %(price)s, %(volume)s, %(premium)s, 
                                %(executed_at)s, %(nbbo_ask)s, %(nbbo_bid)s, %(market_center)s, 
                                %(sale_cond_codes)s, %(ext_hour_sold_codes)s, %(trade_code)s, 
                                %(trade_settlement)s, %(canceled)s, %(collection_time)s
                            ) ON CONFLICT (tracking_id) DO NOTHING
                        """, values)
                        conn.commit()
                        inserted = len(values)
                        
        except Exception as e:
            self.logger.error(f"Error saving trades to database: {str(e)}")
            if conn:
                conn.rollback()
            raise
            
        return inserted

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('darkpool_collector.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create and run collector
    collector = DarkPoolCollector()
    collector.run() 