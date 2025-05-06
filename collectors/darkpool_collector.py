from collectors.base_collector import BaseCollector
import logging
import requests
import psycopg2
from datetime import datetime
from flow_analysis.config.api_config import UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT, DEFAULT_HEADERS, REQUEST_TIMEOUT
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME, TABLE_NAME
from flow_analysis.config.watchlist import SYMBOLS

class DarkPoolCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        # Initialize any other resources here

    def collect(self):
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
                inserted = self._save_trades_to_db(symbol, trades)
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
                                'executed_at': trade.get('executed_at'),
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