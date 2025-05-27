import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv('.env.prod', override=True)

# Get DB credentials from environment
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_SSLMODE = os.getenv('DB_SSLMODE', 'require')

print("DB_NAME:", DB_NAME)
print("DB_USER:", DB_USER)
print("DB_PASSWORD:", DB_PASSWORD)
print("DB_HOST:", DB_HOST)
print("DB_PORT:", DB_PORT)
print("DB_SSLMODE:", DB_SSLMODE)

# Connect to the database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    sslmode=DB_SSLMODE
)
cur = conn.cursor()

# Query the latest 5 records
cur.execute("""
    SELECT id, headline, collected_at
    FROM trading.news_headlines
    ORDER BY collected_at DESC
    LIMIT 5;
""")
rows = cur.fetchall()

print("Latest 5 news_headlines records:")
for row in rows:
    print(row)

cur.close()
conn.close() 