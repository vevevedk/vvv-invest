import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Export collector data for a custom date range.")
parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)', default=None)
parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD, inclusive)', default=None)
args = parser.parse_args()

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

EXPORT_DIR = 'exports'
os.makedirs(EXPORT_DIR, exist_ok=True)

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

# Determine date range
if args.start:
    start_dt = datetime.strptime(args.start, '%Y-%m-%d')
else:
    start_dt = datetime.utcnow() - timedelta(days=1)

if args.end:
    end_dt = datetime.strptime(args.end, '%Y-%m-%d') + timedelta(days=1)  # inclusive
else:
    end_dt = datetime.utcnow() + timedelta(days=1)  # up to now

start_iso = start_dt.isoformat()
end_iso = end_dt.isoformat()

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
    df = pd.read_sql_query(query, conn, params=(start_iso, end_iso))
    out_path = os.path.join(EXPORT_DIR, f"{name}_{args.start or 'last24h'}_{args.end or ''}.csv")
    df.to_csv(out_path, index=False)
    print(f"Exported {len(df)} rows to {out_path}")

conn.close() 