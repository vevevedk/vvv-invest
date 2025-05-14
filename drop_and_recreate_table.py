import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv(dotenv_path='.env.prod')

# Database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Drop the trading.news_headlines table
cur.execute("DROP TABLE IF EXISTS trading.news_headlines;")

# Recreate the trading.news_headlines table with the correct schema
cur.execute("""
    CREATE TABLE trading.news_headlines (
        id SERIAL PRIMARY KEY,
        headline TEXT,
        content TEXT,
        url TEXT,
        published_at TIMESTAMP WITH TIME ZONE,
        source TEXT,
        symbols TEXT[],
        sentiment DOUBLE PRECISION,
        impact_score DOUBLE PRECISION,
        collected_at TIMESTAMP WITH TIME ZONE
    );
""")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Table trading.news_headlines dropped and recreated successfully.") 