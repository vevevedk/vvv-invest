import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

# Database connection parameters from environment variables
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

try:
    cur.execute('''
        ALTER TABLE trading.darkpool_trades
        ADD COLUMN IF NOT EXISTS collected_at TIMESTAMP WITH TIME ZONE;
    ''')
    conn.commit()
    print("Added collected_at column to trading.darkpool_trades (if it did not exist).")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close() 