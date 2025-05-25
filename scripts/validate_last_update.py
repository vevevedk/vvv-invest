import os
import psycopg2
from dotenv import load_dotenv

# Load production environment variables
load_dotenv('.env.prod')

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    sslmode=os.getenv('DB_SSLMODE', 'require')
)
cur = conn.cursor()

print('--- Latest News Headline Collected At ---')
cur.execute("""
SELECT MAX(collected_at) FROM trading.news_headlines;
""")
print(cur.fetchone()[0])

print('--- Latest Dark Pool Trade Collection Time ---')
cur.execute("""
SELECT MAX(collection_time) FROM trading.darkpool_trades;
""")
print(cur.fetchone()[0])

cur.close()
conn.close() 