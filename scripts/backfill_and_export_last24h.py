import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import psycopg2

# Import collectors
from collectors.darkpool_collector import DarkPoolCollector
from collectors.news_collector import NewsCollector

# Load environment variables
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

EXPORT_DIR = 'exports'
os.makedirs(EXPORT_DIR, exist_ok=True)

def main():
    now = datetime.utcnow()
    start_dt = now - timedelta(days=1)
    end_dt = now
    start_str = start_dt.strftime('%Y-%m-%d')
    end_str = end_dt.strftime('%Y-%m-%d')
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    print(f"Backfilling news and dark pool from {start_str} to {end_str} (UTC)...")
    try:
        NewsCollector().collect(start_date=start_str, end_date=end_str)
        print("News backfill complete.")
    except Exception as e:
        print(f"Error in news backfill: {e}")
    try:
        DarkPoolCollector().collect_darkpool_trades(start_date=start_str, end_date=end_str, incremental=False)
        print("Dark pool backfill complete.")
    except Exception as e:
        print(f"Error in dark pool backfill: {e}")
    # Export both tables
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'sslmode': os.getenv('DB_SSLMODE', 'prefer')
    }
    try:
        conn = psycopg2.connect(**db_params)
        queries = {
            'news_headlines': f"""
                SELECT * FROM trading.news_headlines
                WHERE collected_at >= %s AND collected_at < %s
                ORDER BY collected_at DESC
            """,
            'darkpool_trades': f"""
                SELECT * FROM trading.darkpool_trades
                WHERE collection_time >= %s AND collection_time < %s
                ORDER BY collection_time DESC
            """
        }
        for name, query in queries.items():
            df = pd.read_sql_query(query, conn, params=(start_dt.isoformat(), end_dt.isoformat()))
            out_path = os.path.join(EXPORT_DIR, f"{name}_last24h_{timestamp}.csv")
            df.to_csv(out_path, index=False)
            print(f"Exported {len(df)} rows to {out_path}")
        conn.close()
    except Exception as e:
        print(f"Error exporting data: {e}")
    print("Backfill and export complete.")

if __name__ == "__main__":
    main() 