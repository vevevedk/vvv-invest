#!/usr/bin/env python3

import psycopg2

# Database configuration
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'
}

def check_schema():
    """Check the schema of the news_headlines table"""
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Get table schema
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'trading'
            AND table_name = 'news_headlines'
            ORDER BY ordinal_position;
        """)
        
        print("\nTable Schema:")
        print("-------------")
        for row in cur.fetchall():
            col_name, data_type, max_length = row
            if max_length:
                print(f"{col_name}: {data_type}({max_length})")
            else:
                print(f"{col_name}: {data_type}")
                
    except Exception as e:
        print(f"Error checking schema: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_schema() 