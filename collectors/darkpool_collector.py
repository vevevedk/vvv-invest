from collectors.base_collector import BaseCollector
import logging
import requests
import psycopg2
from datetime import datetime, time
import pytz
from flow_analysis.config.api_config import UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME, TABLE_NAME
from flow_analysis.config.watchlist import SYMBOLS

class DarkPoolCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.ny_tz = pytz.timezone('America/New_York')

    def is_market_open(self):
        """Check if US stock market is currently open."""
        now = datetime.now(self.ny_tz)
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False
            
        # Check if it's between 9:30 AM and 4:00 PM ET
        market_open = time(9, 30)  # 9:30 AM
        market_close = time(16, 0)  # 4:00 PM
        current_time = now.time()
        
        return market_open <= current_time <= market_close

    def validate_trade(self, trade):
        """Validate trade data before saving."""
        required_fields = ['tracking_id', 'size', 'price', 'executed_at']
        return all(trade.get(field) for field in required_fields)

    def collect(self):
        if not self.is_market_open():
            self.logger.info("Market is closed. Skipping collection.")
            return

        self.logger.info("Collecting dark pool trades...")
        total_trades = 0
        for symbol in SYMBOLS:
            try:
                url = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}?symbol={symbol}"
                response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                
                if not data or 'data' not in data or not data['data']:
                    self.logger.info(f"No trades found for {symbol}")
                    continue
                    
                trades = data['data']
                # Filter out invalid trades
                valid_trades = [t for t in trades if self.validate_trade(t)]
                
                if not valid_trades:
                    self.logger.info(f"No valid trades found for {symbol}")
                    continue
                    
                inserted = self._save_trades_to_db(symbol, valid_trades)
                total_trades += inserted
                self.logger.info(f"{inserted} trades saved for {symbol}")
                
            except Exception as e:
                self.logger.error(f"Error collecting trades for {symbol}: {e}")
                
        self.logger.info(f"Dark pool collection complete. Total trades saved: {total_trades}")

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
                            self.logger.error(f"DB insert error for {symbol}: {e}")
                conn.commit()
        except Exception as e:
            self.logger.error(f"DB connection error: {e}")
        return inserted 