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

def create_news_tables():
    """Create the news headlines table and its indexes"""
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create the table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trading.news_headlines (
                id SERIAL PRIMARY KEY,
                headline TEXT NOT NULL,
                source VARCHAR(100) NOT NULL,
                published_at TIMESTAMP NOT NULL,
                symbols TEXT[] NOT NULL,
                sentiment DECIMAL(10,4),
                impact_score INTEGER,
                collected_at TIMESTAMP NOT NULL,
                UNIQUE (headline, published_at)
            );
        """)
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_news_symbols 
            ON trading.news_headlines USING GIN(symbols);
            
            CREATE INDEX IF NOT EXISTS idx_news_published 
            ON trading.news_headlines (published_at);
            
            CREATE INDEX IF NOT EXISTS idx_news_collected 
            ON trading.news_headlines (collected_at);
        """)
        
        # Add comments
        cur.execute("""
            COMMENT ON TABLE trading.news_headlines IS 
            'Stores news headlines with sentiment and impact analysis';
            
            COMMENT ON COLUMN trading.news_headlines.sentiment IS 
            'Sentiment score from -1.0 (negative) to 1.0 (positive)';
            
            COMMENT ON COLUMN trading.news_headlines.impact_score IS 
            'Impact score from 1 (low) to 10 (high)';
        """)
        
        # Grant permissions
        cur.execute("""
            GRANT SELECT, INSERT ON trading.news_headlines TO collector;
            GRANT USAGE ON SEQUENCE trading.news_headlines_id_seq TO collector;
        """)
        
        # Commit the changes
        conn.commit()
        print("Successfully created news headlines table and indexes")
        
    except Exception as e:
        print(f"Error creating news headlines table: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_news_tables() 