import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

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
cur = conn.cursor()

try:
    # List all news_headlines tables in all schemas
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = 'news_headlines';
    """)
    tables = cur.fetchall()
    print("Found news_headlines tables:")
    for schema, table in tables:
        print(f"- {schema}.{table}")

    # Drop all news_headlines tables in all schemas
    for schema, table in tables:
        cur.execute(f'DROP TABLE IF EXISTS {schema}.news_headlines CASCADE;')
        print(f"Dropped {schema}.news_headlines")
    conn.commit()

    # Recreate only in trading schema
    cur.execute("""
        CREATE TABLE trading.news_headlines (
            id SERIAL PRIMARY KEY,
            headline TEXT NOT NULL,
            content TEXT,
            url TEXT,
            published_at TIMESTAMP WITH TIME ZONE,
            source TEXT,
            symbols TEXT[],
            sentiment DOUBLE PRECISION,
            impact_score DOUBLE PRECISION,
            collected_at TIMESTAMP WITH TIME ZONE NOT NULL
        );
    """)
    conn.commit()
    print("Recreated trading.news_headlines with correct schema.")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close() 