"""
ALTER TABLE migration for news_headlines: change 'sentiment' from double precision to text.
MUST be run as part of deployment for the news collector pagination fix (both local and prod DBs).
"""
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from ENV_FILE or default to .env
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env'))

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

ALTER_SQL = """
ALTER TABLE trading.news_headlines
ALTER COLUMN sentiment TYPE text USING sentiment::text;
"""

def main():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                print("Altering 'sentiment' column to type text...")
                cur.execute(ALTER_SQL)
            conn.commit()
        print("Success: 'sentiment' column is now type text.")
    except Exception as e:
        print(f"Error altering column: {e}")

if __name__ == "__main__":
    main() 