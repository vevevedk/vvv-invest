import os
from datetime import datetime, timedelta
import pytz
import psycopg2
from dotenv import load_dotenv

# Load environment variables
env_file = os.getenv('ENV_FILE', '.env')
load_dotenv(env_file)

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    sslmode=os.getenv('DB_SSLMODE', 'require')
)

symbols = ['SPY', 'QQQ']
now = datetime.now(pytz.UTC)
start = now - timedelta(hours=24)

print(f"Checking dark pool trade coverage for the last 24 hours: {start} to {now}\n")

for symbol in symbols:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(executed_at), MAX(executed_at)
            FROM trading.darkpool_trades
            WHERE symbol = %s AND executed_at >= %s AND executed_at < %s
            """,
            (symbol, start, now)
        )
        count, min_time, max_time = cur.fetchone()
        print(f"{symbol}: {count} trades in last 24h")
        print(f"  Earliest: {min_time}")
        print(f"  Latest:   {max_time}\n")

conn.close() 