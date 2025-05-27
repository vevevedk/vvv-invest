import os
import psycopg2
from dotenv import load_dotenv

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

# Connect to the database and verify the schema
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'trading' 
        AND table_name = 'news_headlines'
    """)
    columns = cursor.fetchall()
    print("Schema of trading.news_headlines table:")
    for column in columns:
        print(f"{column[0]}: {column[1]}")
except Exception as e:
    print(f"Error verifying schema: {e}")
finally:
    if conn:
        conn.close() 