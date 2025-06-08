from config.db_config import get_db_config
import psycopg2
from psycopg2.extras import DictCursor

def check_news_collector_logs():
    db_config = get_db_config()
    query = """
        SELECT timestamp, level, message, error_details
        FROM trading.collector_logs
        WHERE collector_name = 'news'
        ORDER BY timestamp DESC
        LIMIT 10;
    """
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                print(f"{'Timestamp':<25} {'Level':<8} {'Message':<50} {'Error Details'}")
                print('-' * 120)
                for row in rows:
                    print(f"{row['timestamp']:<25} {row['level']:<8} {row['message']:<50} {row['error_details']}")
    except Exception as e:
        print(f"Error querying logs: {e}")

if __name__ == "__main__":
    check_news_collector_logs() 