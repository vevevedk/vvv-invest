import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv(dotenv_path='.env.prod')

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
    # Drop existing table if it exists
    cur.execute('DROP TABLE IF EXISTS trading.collector_logs;')
    
    # Create enhanced collector_logs table
    cur.execute('''
        CREATE TABLE trading.collector_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            collector_name VARCHAR(50) NOT NULL,
            level VARCHAR(16) NOT NULL,
            message TEXT NOT NULL,
            task_type VARCHAR(50),
            details JSONB,
            is_heartbeat BOOLEAN DEFAULT FALSE,
            status VARCHAR(50),
            items_processed INTEGER,
            api_credits_used INTEGER,
            duration_seconds FLOAT,
            error_details JSONB
        );
        
        -- Create indexes for efficient querying
        CREATE INDEX idx_collector_logs_timestamp ON trading.collector_logs(timestamp);
        CREATE INDEX idx_collector_logs_collector ON trading.collector_logs(collector_name);
        CREATE INDEX idx_collector_logs_level ON trading.collector_logs(level);
        CREATE INDEX idx_collector_logs_status ON trading.collector_logs(status);
        CREATE INDEX idx_collector_logs_heartbeat ON trading.collector_logs(is_heartbeat);
        
        -- Add comments
        COMMENT ON TABLE trading.collector_logs IS 'Enhanced logging table for collector operations and heartbeats';
        COMMENT ON COLUMN trading.collector_logs.collector_name IS 'Name of the collector (e.g., darkpool, news)';
        COMMENT ON COLUMN trading.collector_logs.task_type IS 'Type of task being performed (e.g., backfill, realtime)';
        COMMENT ON COLUMN trading.collector_logs.details IS 'Additional structured data about the operation';
        COMMENT ON COLUMN trading.collector_logs.is_heartbeat IS 'Whether this is a heartbeat message';
        COMMENT ON COLUMN trading.collector_logs.status IS 'Current status of the collector';
        COMMENT ON COLUMN trading.collector_logs.items_processed IS 'Number of items processed in this operation';
        COMMENT ON COLUMN trading.collector_logs.api_credits_used IS 'Number of API credits used';
        COMMENT ON COLUMN trading.collector_logs.duration_seconds IS 'Duration of the operation in seconds';
        COMMENT ON COLUMN trading.collector_logs.error_details IS 'Detailed error information if an error occurred';
    ''')
    conn.commit()
    print("Created enhanced trading.collector_logs table.")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close() 