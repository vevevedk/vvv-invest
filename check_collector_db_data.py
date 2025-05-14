import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

print('--- trading.darkpool_trades ---')
cur.execute('SELECT MIN(collection_time), MAX(collection_time), COUNT(*) FROM trading.darkpool_trades;')
min_time, max_time, count = cur.fetchone()
print(f"Rows: {count}")
print(f"Earliest collection_time: {min_time}")
print(f"Latest collection_time: {max_time}")

print('\nSample rows:')
df = pd.read_sql_query('SELECT * FROM trading.darkpool_trades ORDER BY collection_time DESC LIMIT 5', conn)
print(df)

print('\n--- trading.news_headlines ---')
cur.execute('SELECT MIN(collected_at), MAX(collected_at), COUNT(*) FROM trading.news_headlines;')
min_time, max_time, count = cur.fetchone()
print(f"Rows: {count}")
print(f"Earliest collected_at: {min_time}")
print(f"Latest collected_at: {max_time}")

print('\nSample rows:')
df = pd.read_sql_query('SELECT * FROM trading.news_headlines ORDER BY collected_at DESC LIMIT 5', conn)
print(df)

cur.close()
conn.close() 