import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv(dotenv_path='.env.prod')

# Database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Query to check if data is being inserted into trading.news_headlines
cur.execute("""
    SELECT COUNT(*) FROM trading.news_headlines;
""")

# Fetch and print the result
count = cur.fetchone()[0]
print(f"Number of records in trading.news_headlines: {count}")

# Close the cursor and connection
cur.close()
conn.close() 