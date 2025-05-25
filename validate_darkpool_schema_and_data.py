import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Load production environment variables
load_dotenv('.env.prod')

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

def get_table_schema():
    """Get the current schema of the darkpool_trades table."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'trading'
                AND table_name = 'darkpool_trades'
                ORDER BY ordinal_position;
            """)
            return cur.fetchall()

def get_column_null_counts():
    """Get count of NULL values for each column in the last 24 hours, only for columns that exist."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get actual columns in the table
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'trading' AND table_name = 'darkpool_trades';
            """)
            columns = [row[0] for row in cur.fetchall()]
            # Build dynamic SQL for NULL count
            null_count_sql = []
            for col in columns:
                null_count_sql.append(f"SELECT '{col}' as column_name, COUNT(*) FILTER (WHERE {col} IS NULL) as null_count, COUNT(*) as total_count FROM trading.darkpool_trades WHERE collection_time >= NOW() - INTERVAL '24 hours'")
            union_sql = " UNION ALL ".join(null_count_sql)
            cur.execute(union_sql)
            return cur.fetchall()

def get_recent_trade_sample():
    """Get a sample of recent trades to verify data quality."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM trading.darkpool_trades
                WHERE collection_time >= NOW() - INTERVAL '24 hours'
                ORDER BY collection_time DESC
                LIMIT 5;
            """)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)

def get_trade_counts_per_symbol():
    """Get count of trades per symbol for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, COUNT(*) as trade_count
                FROM trading.darkpool_trades
                WHERE collection_time >= NOW() - INTERVAL '7 days'
                GROUP BY symbol
                ORDER BY trade_count DESC;
            """)
            return cur.fetchall()

def main():
    print("\n=== Darkpool Trades Schema and Data Validation ===\n")
    
    # Check schema
    print("1. Current Table Schema:")
    print("-" * 80)
    schema = get_table_schema()
    for col in schema:
        print(f"Column: {col[0]:<20} Type: {col[1]:<15} Nullable: {col[2]:<5} Default: {col[3]}")
    
    # Check NULL values
    print("\n2. NULL Value Analysis (Last 24 Hours):")
    print("-" * 80)
    null_counts = get_column_null_counts()
    for col, null_count, total_count in null_counts:
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        print(f"Column: {col:<20} NULL Count: {null_count:<5} Total: {total_count:<5} NULL %: {null_percentage:.1f}%")
    
    # Show sample data
    print("\n3. Recent Trade Sample (Last 24 Hours):")
    print("-" * 80)
    sample = get_recent_trade_sample()
    print(sample.to_string())

    # Print trade counts per symbol for last 7 days
    print("\n4. Trade Counts Per Symbol (Last 7 Days):")
    print("-" * 80)
    counts = get_trade_counts_per_symbol()
    for symbol, count in counts:
        print(f"Symbol: {symbol:<8} Trades: {count}")

if __name__ == "__main__":
    main() 