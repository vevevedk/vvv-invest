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

class DarkPoolCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.ny_tz = pytz.timezone('America/New_York')
        # Only collect for SPY and QQQ
        self.symbols = ['SPY', 'QQQ']
        # Log API token status (first 4 chars only for security)
        token = DEFAULT_HEADERS.get('Authorization', '').split()[-1]
        self.logger.info(f"API Token configured: {token[:4]}...")

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
            return 0
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    for trade in trades:
                        try:
                            # Convert executed_at to UTC if it's not already
                            executed_at = trade.get('executed_at')
                            if isinstance(executed_at, str):
                                executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
                            
                            cur.execute(f'''
                                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                                    tracking_id, symbol, size, price, volume, premium, executed_at, nbbo_ask, nbbo_bid, market_center, sale_cond_codes, collection_time
                                ) VALUES (
                                    %(tracking_id)s, %(symbol)s, %(size)s, %(price)s, %(volume)s, %(premium)s, %(executed_at)s, %(nbbo_ask)s, %(nbbo_bid)s, %(market_center)s, %(sale_cond_codes)s, %(collection_time)s
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
                                'collection_time': datetime.utcnow(),
                            })
                            inserted += 1
                        except Exception as e:
                            self.logger.error(f"DB insert error for {symbol}: {str(e)}")
                conn.commit()
        except Exception as e:
            self.logger.error(f"DB connection error: {str(e)}")
        return inserted 