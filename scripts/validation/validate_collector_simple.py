import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from config.db_config import get_db_config

# Database connection setup
db_config = get_db_config()
DATABASE_URL = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
engine = create_engine(DATABASE_URL)

def print_table_summary(table, time_col):
    now = datetime.utcnow()
    since = now - timedelta(minutes=30)
    query = f"""
        SELECT COUNT(*) FROM {table} WHERE {time_col} >= :since;
    """
    latest_query = f"""
        SELECT {time_col} FROM {table} ORDER BY {time_col} DESC LIMIT 1;
    """
    sample_query = f"""
        SELECT * FROM {table} ORDER BY {time_col} DESC LIMIT 5;
    """
    with engine.connect() as conn:
        count = conn.execute(text(query), {'since': since}).scalar()
        latest = conn.execute(text(latest_query)).scalar()
        sample = pd.read_sql(sample_query, conn)
    print(f"{table}:")
    print(f"  Latest {time_col}: {latest}")
    print(f"  Records in last 30 min: {count}")
    print(f"  Latest 5 rows:")
    print(sample)
    print()

if __name__ == "__main__":
    print_table_summary('trading.darkpool_trades', 'collection_time')
    print_table_summary('trading.options_flow', 'collected_at')
    print_table_summary('trading.news_headlines', 'collected_at') 