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

# Check current schema
cur.execute("SELECT current_schema();")
current_schema = cur.fetchone()[0]
print(f"Current schema: {current_schema}")

# Check search path
cur.execute("SHOW search_path;")
search_path = cur.fetchone()[0]
print(f"Search path: {search_path}")

# List all tables in the trading schema
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'trading';
""")
tables = cur.fetchall()
print("\nTables in trading schema:")
for table in tables:
    print(f"- {table[0]}")

# Check if news_headlines exists in any schema
cur.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_name = 'news_headlines';
""")
news_tables = cur.fetchall()
print("\nnews_headlines tables found:")
for schema, table in news_tables:
    print(f"- {schema}.{table}")

# Close the cursor and connection
cur.close()
conn.close() 