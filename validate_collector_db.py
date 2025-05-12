import os
from dotenv import load_dotenv
load_dotenv('/opt/darkpool_collector/.env.prod')
import psycopg2
from datetime import datetime, timedelta
from tabulate import tabulate

# Load DB config from environment variables
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'
}

def check_table(conn, table, time_col, extra_cols=None):
    cur = conn.cursor()
    since = datetime.utcnow() - timedelta(minutes=30)
    # Get latest record
    cur.execute(f"SELECT {time_col} FROM {table} ORDER BY {time_col} DESC LIMIT 1;")
    latest = cur.fetchone()
    # Get count in last 30 minutes
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {time_col} >= %s;", (since,))
    count = cur.fetchone()[0]
    # Get a few recent rows
    cols = '*' if not extra_cols else ', '.join(extra_cols)
    cur.execute(f"SELECT {cols} FROM {table} ORDER BY {time_col} DESC LIMIT 5;")
    rows = cur.fetchall()
    return latest, count, rows

def main():
    print("Connecting to production DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("\n--- News Headlines ---")
    latest, count, rows = check_table(
        conn, 'trading.news_headlines', 'collected_at', ['headline', 'published_at', 'collected_at']
    )
    print(f"Latest collected_at: {latest[0] if latest else 'None'}")
    print(f"Records in last 30 min: {count}")
    print(tabulate(rows, headers=["headline", "published_at", "collected_at"]))

    print("\n--- Dark Pool Trades ---")
    latest, count, rows = check_table(
        conn, 'trading.darkpool_trades', 'collection_time', ['symbol', 'executed_at', 'collection_time']
    )
    print(f"Latest collection_time: {latest[0] if latest else 'None'}")
    print(f"Records in last 30 min: {count}")
    print(tabulate(rows, headers=["symbol", "executed_at", "collection_time"]))
    conn.close()

if __name__ == "__main__":
    main() 