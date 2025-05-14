import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv('.env.prod')

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require'),
}

ALTER_SQL = '''
ALTER TABLE trading.darkpool_trades
    ALTER COLUMN symbol TYPE VARCHAR(32),
    ALTER COLUMN market_center TYPE VARCHAR(32);
'''

if __name__ == '__main__':
    try:
        print('Connecting to database...')
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        print('Altering columns symbol and market_center to VARCHAR(32)...')
        cur.execute(ALTER_SQL)
        print('Columns altered successfully!')
        cur.close()
        conn.close()
    except Exception as e:
        print(f'Error: {e}') 