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

# Get column information for trading.news_headlines
cur.execute("""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = 'trading'
    AND table_name = 'news_headlines'
    ORDER BY ordinal_position;
""")

columns = cur.fetchall()
print("\nSchema of trading.news_headlines:")
for col in columns:
    col_name, data_type, max_length = col
    if max_length:
        print(f"- {col_name}: {data_type}({max_length})")
    else:
        print(f"- {col_name}: {data_type}")

# Close the cursor and connection
cur.close()
conn.close() 