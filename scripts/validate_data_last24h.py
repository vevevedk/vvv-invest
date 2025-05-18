import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

def validate_last24h():
    now = datetime.utcnow()
    start_dt = now - timedelta(days=1)
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'sslmode': os.getenv('DB_SSLMODE', 'prefer')
    }
    queries = {
        'news_headlines': {
            'count': """
                SELECT COUNT(*) FROM trading.news_headlines
                WHERE collected_at >= %s AND collected_at < %s
            """,
            'latest': """
                SELECT collected_at FROM trading.news_headlines
                ORDER BY collected_at DESC LIMIT 1
            """
        },
        'darkpool_trades': {
            'count': """
                SELECT COUNT(*) FROM trading.darkpool_trades
                WHERE collection_time >= %s AND collection_time < %s
            """,
            'latest': """
                SELECT collection_time FROM trading.darkpool_trades
                ORDER BY collection_time DESC LIMIT 1
            """
        }
    }
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            for name, q in queries.items():
                cur.execute(q['count'], (start_dt, now))
                count = cur.fetchone()[0]
                cur.execute(q['latest'])
                latest = cur.fetchone()
                print(f"--- {name} ---")
                print(f"Rows inserted in last 24h: {count}")
                print(f"Most recent timestamp: {latest[0] if latest else 'None'}\n")
        conn.close()
    except Exception as e:
        print(f"Error validating data: {e}")

if __name__ == "__main__":
    validate_last24h() 