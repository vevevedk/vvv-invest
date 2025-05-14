import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

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

since = (datetime.utcnow() - timedelta(days=1)).isoformat()

queries = {
    'news_headlines': f"""
        SELECT * FROM trading.news_headlines
        WHERE collected_at >= %s
        ORDER BY collected_at DESC
    """,
    'darkpool_trades': f"""
        SELECT * FROM trading.darkpool_trades
        WHERE collection_time >= %s
        ORDER BY collection_time DESC
    """
}

for name, query in queries.items():
    df = pd.read_sql_query(query, conn, params=(since,))
    out_path = os.path.join(EXPORT_DIR, f"{name}_last_24h.csv")
    df.to_csv(out_path, index=False)
    print(f"Exported {len(df)} rows to {out_path}")

conn.close() 