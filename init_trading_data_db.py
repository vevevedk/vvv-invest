import os
import psycopg2
from dotenv import load_dotenv

# Set ENV_FILE for downstream imports
os.environ['ENV_FILE'] = os.getenv('ENV_FILE', '.env')

# Load environment variables from ENV_FILE or default to .env
env_file = os.getenv('ENV_FILE', '.env')
load_dotenv(env_file, override=True)

# Database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE IF NOT EXISTS trading.news_headlines (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    content TEXT,
    url TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    source TEXT,
    symbols TEXT[],
    sentiment DOUBLE PRECISION,
    impact_score DOUBLE PRECISION,
    collected_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.darkpool_trades (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size BIGINT NOT NULL,
    exchange TEXT,
    condition_codes TEXT[],
    collected_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Add other tables as needed for your collectors
"""

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

try:
    cur.execute(SCHEMA_SQL)
    conn.commit()
    print("Schema and tables created successfully in trading_data database.")
except Exception as e:
    print(f"Error initializing database: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close() 