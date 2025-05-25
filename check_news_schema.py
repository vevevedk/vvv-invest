import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv('.env.prod')

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

def check_news_schema():
    """Check the current schema of the news_headlines table."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Get column information
            cur.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns 
                WHERE table_schema = 'trading' 
                AND table_name = 'news_headlines'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            
            print("\nCurrent Schema of trading.news_headlines:")
            print("-" * 80)
            for col in columns:
                col_name, data_type, nullable, default, max_length = col
                type_info = f"{data_type}({max_length})" if max_length else data_type
                print(f"Column: {col_name:<20} Type: {type_info:<20} Nullable: {nullable:<5} Default: {default}")
            
            # Get sample data to check data types
            print("\nSample Data Types:")
            print("-" * 80)
            cur.execute("""
                SELECT * FROM trading.news_headlines 
                ORDER BY collected_at DESC 
                LIMIT 1;
            """)
            if cur.description:
                for i, col in enumerate(cur.description):
                    print(f"{col.name}: {col.type_code}")

if __name__ == "__main__":
    check_news_schema() 