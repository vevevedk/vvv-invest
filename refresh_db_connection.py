import os
import psycopg2
from dotenv import load_dotenv
import time

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
    # Terminate all connections to the database except our own
    cur.execute("""
        SELECT pg_terminate_backend(pid) 
        FROM pg_stat_activity 
        WHERE datname = %s 
        AND pid <> pg_backend_pid();
    """, (db_params['dbname'],))
    
    # Commit the changes
    conn.commit()
    print("Terminated all other connections to the database")
    
    # Close our connection
    cur.close()
    conn.close()
    print("Closed our connection")
    
    # Wait a moment
    time.sleep(2)
    
    # Reconnect
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    print("Reconnected to the database")
    
    # Verify we can see the content column
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'trading' 
        AND table_name = 'news_headlines' 
        AND column_name = 'content';
    """)
    
    result = cur.fetchone()
    if result:
        print("Verified content column exists in trading.news_headlines")
    else:
        print("WARNING: content column not found in trading.news_headlines")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close the cursor and connection
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close() 