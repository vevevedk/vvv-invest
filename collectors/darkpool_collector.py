from collectors.base_collector import BaseCollector
import logging
import requests
import psycopg2
from datetime import datetime, timedelta, time as dt_time
import time
import pytz
from flow_analysis.config.api_config import UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME, TABLE_NAME
from flow_analysis.config.watchlist import SYMBOLS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

class DarkPoolCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.ny_tz = pytz.timezone('America/New_York')
        # Use the SYMBOLS list from the watchlist config for all target tickers
        self.symbols = SYMBOLS  # Update the watchlist to add/remove symbols to fetch
        # Log API token status (first 6 chars only for security)
        token = DEFAULT_HEADERS.get('Authorization', '').split()[-1]
        self.logger.info(f"API Token configured: {token[:6]}... (length: {len(token)})")
        self.logger.info(f"Loaded UW_API_TOKEN (first 6): {os.getenv('UW_API_TOKEN', '')[:6]}...")

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

    def collect_darkpool_trades(self, start_date=None, end_date=None, incremental=True):
        """
        Collect dark pool trades for all symbols.
        If incremental=True, only fetch new trades since last executed_at for today.
        If backfill, loop over dates from start_date to end_date (inclusive).
        """
        # Configure session with retries and keep-alive
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
            # Only today
            date_list = [today]
        else:
            # Backfill: all dates from start_date to end_date
            if not start_date or not end_date:
                self.logger.error("Backfill requires start_date and end_date.")
                return
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        for symbol in self.symbols:
            for date_obj in date_list:
                date_str = date_obj.strftime("%Y-%m-%d")
                self.logger.info(f"Collecting dark pool trades for {symbol} on {date_str}")
                newer_than = None
                if incremental:
                    # Get latest executed_at for this symbol and date
                    try:
                        with psycopg2.connect(**DB_CONFIG) as conn:
                            with conn.cursor() as cur:
                                cur.execute(f"""
                                    SELECT MAX(executed_at) FROM {SCHEMA_NAME}.{TABLE_NAME}
                                    WHERE symbol = %s AND executed_at::date = %s
                                """, (symbol, date_str))
                                result = cur.fetchone()
                                if result and result[0]:
                                    newer_than = result[0].isoformat()
                                    self.logger.info(f"Found existing trades up to {newer_than}")
                    except Exception as e:
                        self.logger.error(f"DB error when fetching latest executed_at: {str(e)}")
                page = 0
                max_pages = 10  # Limit number of pages to prevent excessive API calls
                while page < max_pages:
                    params = {
                        "date": date_str,
                        "limit": 500,
                    }
                    if newer_than:
                        params["newer_than"] = newer_than
                    url = f"{UW_BASE_URL}/darkpool/{symbol}"
                    self.logger.debug(f"Requesting: {url} params={params}")
                    # Log the headers before making the request
                    self.logger.info(f"Request headers: {DEFAULT_HEADERS}")
                    try:
                        start_time = time.time()
                        response = session.get(url, headers=DEFAULT_HEADERS, params=params, timeout=REQUEST_TIMEOUT)
                        response.raise_for_status()
                        data = response.json().get("data", [])
                        end_time = time.time()
                        self.logger.info(f"API call took {end_time - start_time:.2f} seconds")
                        if not data:
                            self.logger.info(f"No more data for {symbol} on {date_str}")
                            break
                        valid_trades = [t for t in data if self.validate_trade(t)]
                        inserted = self._save_trades_to_db(symbol, valid_trades)
                        total_inserted += inserted
                        self.logger.info(f"Inserted {inserted} trades for {symbol} on {date_str} (page {page})")
                        if len(data) < 500:
                            self.logger.info(f"Received less than 500 trades, assuming end of data")
                            break
                        # For next page, set newer_than to latest executed_at in this batch
                        latest = max(t["executed_at"] for t in data if t.get("executed_at"))
                        newer_than = latest
                        page += 1
                        time.sleep(0.5)
                    except Exception as e:
                        self.logger.error(f"API error for {symbol} on {date_str}: {str(e)}")
                        break
        self.logger.info(f"Total trades inserted: {total_inserted}")

    def collect(self):
        """Default incremental collection (every 5 min)"""
        if not self.is_market_open():
            self.logger.info("Market is closed. Skipping collection.")
            return
        self.collect_darkpool_trades(incremental=True)

    def _save_trades_to_db(self, symbol, trades):
        inserted = 0
        if not trades:
            self.logger.info(f"No trades to insert for {symbol}")
            return 0
        
        self.logger.info(f"Attempting to insert {len(trades)} trades for {symbol}")

        # Track missing optional fields
        optional_fields = [
            'trade_settlement', 'trade_code', 'ext_hour_sold_codes',
            'sale_cond_codes', 'nbbo_ask_quantity', 'nbbo_bid_quantity'
        ]
        missing_counts = {field: 0 for field in optional_fields}
        
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                self.logger.info(f"Successfully connected to database for {symbol}")
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
                    
                    for i, trade in enumerate(trades, 1):
                        try:
                            # Convert executed_at to UTC if it's not already
                            executed_at = trade.get('executed_at')
                            if isinstance(executed_at, str):
                                executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
                            
                            # Log missing optional fields for this trade
                            for field in optional_fields:
                                if trade.get(field) is None:
                                    missing_counts[field] += 1

                            # Log trade details before insert
                            self.logger.debug(f"Processing trade {i}/{len(trades)} for {symbol}: tracking_id={trade.get('tracking_id')}, executed_at={executed_at}")
                            
                            cur.execute(f'''
                                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                                    tracking_id, symbol, size, price, volume, premium, executed_at, nbbo_ask, nbbo_bid, market_center, sale_cond_codes, ext_hour_sold_codes, trade_code, trade_settlement, canceled, collection_time
                                ) VALUES (
                                    %(tracking_id)s, %(symbol)s, %(size)s, %(price)s, %(volume)s, %(premium)s, %(executed_at)s, %(nbbo_ask)s, %(nbbo_bid)s, %(market_center)s, %(sale_cond_codes)s, %(ext_hour_sold_codes)s, %(trade_code)s, %(trade_settlement)s, %(canceled)s, %(collection_time)s
                                ) ON CONFLICT (tracking_id) DO NOTHING
                            ''', {
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
                            inserted += 1
                            conn.commit()
                            self.logger.debug(f"Successfully inserted trade {i} for {symbol}")
                        except psycopg2.Error as e:
                            self.logger.error(f"PostgreSQL error for {symbol} trade {i}: {str(e)}")
                            self.logger.error(f"Error code: {e.pgcode}, Error message: {e.pgerror}")
                            self.logger.error(f"Trade data: {trade}")
                            conn.rollback()
                            continue
                        except Exception as e:
                            self.logger.error(f"Unexpected error for {symbol} trade {i}: {str(e)}")
                            self.logger.error(f"Error type: {type(e).__name__}")
                            self.logger.error(f"Trade data: {trade}")
                            conn.rollback()
                            continue
                    # Log summary of missing optional fields for this batch
                    for field, count in missing_counts.items():
                        if count > 0:
                            self.logger.info(f"{count} out of {len(trades)} trades missing optional field '{field}' for {symbol}")
                self.logger.info(f"Completed processing {len(trades)} trades for {symbol}. Successfully inserted: {inserted}")
        except psycopg2.Error as e:
            self.logger.error(f"PostgreSQL connection error: {str(e)}")
            self.logger.error(f"Error code: {e.pgcode}, Error message: {e.pgerror}")
        except Exception as e:
            self.logger.error(f"Unexpected database connection error: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            
        return inserted 