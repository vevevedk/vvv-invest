import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env
load_dotenv('.env')

# Read database connection details from environment variables
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Connect to the database and verify recent inserts
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
    cursor.execute("""
        SELECT headline, published_at 
        FROM trading.news_headlines 
        WHERE collected_at > %s
        ORDER BY published_at DESC
    """, (twenty_four_hours_ago,))
    records = cursor.fetchall()
    print(f"Number of new records inserted in the last 24 hours: {len(records)}")
    print("Recent headlines:")
    for headline, published_at in records:
        print(f"{published_at}: {headline}")
except Exception as e:
    print(f"Error verifying inserts: {e}")
finally:
    if conn:
        conn.close() 