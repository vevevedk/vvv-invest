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

try:
    # Drop the table if it exists
    cur.execute("DROP TABLE IF EXISTS trading.news_headlines;")
    print("Dropped existing table")
    
    # Create the table with the correct schema
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
    print("Created new table with correct schema")
    
    # Commit the changes
    conn.commit()
    print("Changes committed successfully")
    
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    # Close the cursor and connection
    cur.close()
    conn.close() 