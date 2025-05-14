import os
import psycopg2
from psycopg2 import sql
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

SCHEMA_NAME = 'trading'
TABLE_NAME = 'darkpool_trades'

CREATE_TABLE_SQL = f'''
CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (
    tracking_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    size INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    volume INTEGER NOT NULL,
    premium DECIMAL(15,2) NOT NULL,
    executed_at TIMESTAMP NOT NULL,
    nbbo_ask DECIMAL(10,2),
    nbbo_bid DECIMAL(10,2),
    market_center VARCHAR(10),
    sale_cond_codes VARCHAR(10),
    collection_time TIMESTAMP NOT NULL
);
'''

if __name__ == '__main__':
    try:
        print('Connecting to database...')
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        print(f'Dropping table {SCHEMA_NAME}.{TABLE_NAME} if it exists...')
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.{} CASCADE;').format(
            sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME)))

        print(f'Creating table {SCHEMA_NAME}.{TABLE_NAME}...')
        cur.execute(CREATE_TABLE_SQL)

        print('Table created successfully!')
        cur.close()
        conn.close()
    except Exception as e:
        print(f'Error: {e}') 