import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env or .env.prod
env_file = '.env.prod' if os.getenv('ENV') == 'prod' else '.env'
load_dotenv(env_file)

# Read database connection details from environment variables
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Connect to the database and check if the url column exists
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'trading' 
        AND table_name = 'news_headlines' 
        AND column_name = 'url'
    """)
    result = cursor.fetchone()
    if result:
        print("The url column still exists in the trading.news_headlines table.")
    else:
        print("The url column has been successfully removed from the trading.news_headlines table.")
except Exception as e:
    print(f"Error verifying migration: {e}")
finally:
    if conn:
        conn.close() 