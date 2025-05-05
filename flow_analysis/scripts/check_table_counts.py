#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from tabulate import tabulate

# Database connection setup
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'
}

# Create database URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Create engine with SSL required
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'sslmode': 'require'
    }
)

# Query to get row counts for all relevant tables
query = """
SELECT 
    'trading.darkpool_trades' as table_name,
    COUNT(*) as row_count,
    MAX(collection_time) as last_update
FROM trading.darkpool_trades

UNION ALL

SELECT 
    'trading.options_flow' as table_name,
    COUNT(*) as row_count,
    MAX(collected_at) as last_update
FROM trading.options_flow

UNION ALL

SELECT 
    'trading.news_headlines' as table_name,
    COUNT(*) as row_count,
    MAX(collected_at) as last_update
FROM trading.news_headlines

UNION ALL

SELECT 
    'trading.collector_logs' as table_name,
    COUNT(*) as row_count,
    MAX(timestamp) as last_update
FROM trading.collector_logs

ORDER BY table_name;
"""

try:
    counts_df = pd.read_sql_query(query, engine)
    print("\nTable Row Counts:")
    print(tabulate(counts_df, headers='keys', tablefmt='psql', showindex=False))
    
    # Also show recent logs
    logs_query = """
    SELECT 
        timestamp,
        level,
        message,
        date_trunc('minute', timestamp) as log_minute,
        count(*) over (partition by date_trunc('minute', timestamp)) as logs_per_minute
    FROM trading.collector_logs
    ORDER BY timestamp DESC
    LIMIT 10
    """
    
    logs_df = pd.read_sql_query(logs_query, engine)
    print(f"\nMost recent {len(logs_df)} log entries:")
    print(tabulate(logs_df, headers='keys', tablefmt='psql', showindex=False))

except Exception as e:
    print(f"Error executing query: {str(e)}") 