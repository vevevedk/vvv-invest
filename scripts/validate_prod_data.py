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

print('--- 10 Most Recent News Headlines ---')
cur.execute("""
SELECT id, headline, published_at, symbols, sentiment, impact_score, collected_at
FROM trading.news_headlines
ORDER BY collected_at DESC
LIMIT 10;
""")
for row in cur.fetchall():
    print(row)

print('\n--- 10 Most Recent Dark Pool Trades ---')
cur.execute("""
SELECT id, symbol, price, size, executed_at, collection_time
FROM trading.darkpool_trades
ORDER BY collection_time DESC
LIMIT 10;
""")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close() 