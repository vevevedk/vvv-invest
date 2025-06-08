#!/usr/bin/env python3

import os
import psycopg2
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS trading.flow_alerts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    size INTEGER NOT NULL,
    premium DECIMAL(12,2) NOT NULL,
    expiration DATE NOT NULL,
    strike DECIMAL(10,2) NOT NULL,
    option_type VARCHAR(4) NOT NULL,
    delta DECIMAL(5,2) NOT NULL,
    volume INTEGER NOT NULL,
    open_interest INTEGER NOT NULL,
    bid DECIMAL(10,2) NOT NULL,
    ask DECIMAL(10,2) NOT NULL,
    bid_ask_spread_pct DECIMAL(5,2) NOT NULL,
    collection_time TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_flow_alerts_symbol ON trading.flow_alerts(symbol);
CREATE INDEX IF NOT EXISTS idx_flow_alerts_timestamp ON trading.flow_alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_flow_alerts_premium ON trading.flow_alerts(premium);
CREATE INDEX IF NOT EXISTS idx_flow_alerts_collection_time ON trading.flow_alerts(collection_time);

-- Add comments
COMMENT ON TABLE trading.flow_alerts IS 'Stores flow alerts data collected from Unusual Whales API';
COMMENT ON COLUMN trading.flow_alerts.premium IS 'Total premium value of the alert';
COMMENT ON COLUMN trading.flow_alerts.delta IS 'Option delta value';
COMMENT ON COLUMN trading.flow_alerts.bid_ask_spread_pct IS 'Percentage spread between bid and ask prices';

-- Grant permissions
GRANT SELECT, INSERT ON trading.flow_alerts TO collector;
GRANT USAGE ON SEQUENCE trading.flow_alerts_id_seq TO collector;
'''

def run_migration():
    """Create the flow alerts table."""
    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        logger.info("Creating flow alerts table...")
        cur.execute(CREATE_TABLE_SQL)
        
        logger.info("Flow alerts table created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating flow alerts table: {str(e)}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_migration() 