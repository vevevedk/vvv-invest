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

def monitor_collectors():
    """Monitor the status of all collectors"""
    # Query for logs and recent data collection status
    query = """
    WITH recent_data AS (
        -- Check darkpool trades
        SELECT 
            'Collection Status' as section,
            'Darkpool Trades' as collector,
            MAX(collection_time) as last_collection,
            COUNT(*) as records_last_hour,
            1 as section_order
        FROM trading.darkpool_trades
        WHERE collection_time >= NOW() - INTERVAL '1 hour'
        
        UNION ALL
        
        -- Check options flow
        SELECT 
            'Collection Status' as section,
            'Options Flow' as collector,
            MAX(collected_at) as last_collection,
            COUNT(*) as records_last_hour,
            1 as section_order
        FROM trading.options_flow
        WHERE collected_at >= NOW() - INTERVAL '1 hour'
        
        UNION ALL
        
        -- Check news headlines
        SELECT 
            'Collection Status' as section,
            'News Headlines' as collector,
            MAX(collected_at) as last_collection,
            COUNT(*) as records_last_hour,
            1 as section_order
        FROM trading.news_headlines
        WHERE collected_at >= NOW() - INTERVAL '1 hour'
    ),
    recent_logs AS (
        SELECT 
            'Recent Logs' as section,
            level as collector,
            timestamp as last_collection,
            count(*) over (partition by date_trunc('minute', timestamp)) as records_last_hour,
            2 as section_order
        FROM trading.collector_logs
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        ORDER BY timestamp DESC
        LIMIT 10
    ),
    recent_news AS (
        SELECT 
            'Recent News' as section,
            headline as collector,
            published_at as last_collection,
            impact_score as records_last_hour,
            3 as section_order
        FROM trading.news_headlines
        WHERE collected_at >= NOW() - INTERVAL '1 hour'
        ORDER BY published_at DESC
        LIMIT 5
    )

    -- Combine results
    SELECT * FROM recent_data
    UNION ALL
    SELECT * FROM recent_logs
    UNION ALL
    SELECT * FROM recent_news
    ORDER BY section_order, last_collection DESC;
    """

    try:
        results_df = pd.read_sql_query(query, engine)

        # Display collection status
        collection_status = results_df[results_df['section'] == 'Collection Status']
        print("\nCollection Status:")
        print(tabulate(collection_status[['collector', 'last_collection', 'records_last_hour']], 
                      headers=['Collector', 'Last Collection', 'Records (Last Hour)'], 
                      tablefmt='psql', 
                      showindex=False))

        # Display recent logs
        logs = results_df[results_df['section'] == 'Recent Logs']
        print("\nMost recent log entries:")
        print(tabulate(logs[['collector', 'last_collection', 'records_last_hour']], 
                      headers=['Level', 'Timestamp', 'Messages/Minute'], 
                      tablefmt='psql', 
                      showindex=False))

        # Display recent news
        news = results_df[results_df['section'] == 'Recent News']
        if not news.empty:
            print("\nMost recent news headlines:")
            for _, row in news.iterrows():
                print(f"\nHeadline: {row['collector']}")
                print(f"Published: {row['last_collection']}")
                print(f"Impact Score: {row['records_last_hour']}")
                print("-" * 80)
        else:
            print("\nNo recent news headlines found.")

    except Exception as e:
        print(f"Error executing query: {str(e)}")

if __name__ == "__main__":
    monitor_collectors() 