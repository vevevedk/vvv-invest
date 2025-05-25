import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load production environment variables
load_dotenv('.env.prod')

EXPORT_DIR = 'exports'
os.makedirs(EXPORT_DIR, exist_ok=True)

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    sslmode=os.getenv('DB_SSLMODE', 'require')
)

seven_days_ago = datetime.utcnow() - timedelta(days=7)

# Export news headlines
news_query = """
SELECT id, headline, published_at, symbols, sentiment, impact_score, collected_at
FROM trading.news_headlines
WHERE collected_at >= %s
ORDER BY collected_at DESC;
"""
news_df = pd.read_sql(news_query, conn, params=(seven_days_ago,))
news_path = os.path.join(EXPORT_DIR, f'news_headlines_last7d_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
news_df.to_csv(news_path, index=False)
print(f'Exported news headlines to {news_path}')

# Export dark pool trades
trades_query = """
SELECT id, symbol, price, size, executed_at, collection_time
FROM trading.darkpool_trades
WHERE collection_time >= %s
ORDER BY collection_time DESC;
"""
trades_df = pd.read_sql(trades_query, conn, params=(seven_days_ago,))
trades_path = os.path.join(EXPORT_DIR, f'darkpool_trades_last7d_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
trades_df.to_csv(trades_path, index=False)
print(f'Exported dark pool trades to {trades_path}')

conn.close() 