#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import execute_values
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060',
    'sslmode': 'require'
}

def migrate_news_table():
    """Migrate the news_headlines table to the correct schema"""
    try:
        # Connect to the database
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Drop existing table
        logger.info("Dropping existing table if it exists...")
        cur.execute("DROP TABLE IF EXISTS trading.news_headlines CASCADE;")
        
        # Create the table with the full expected schema
        logger.info("Creating new table with correct schema...")
        cur.execute("""
            CREATE TABLE trading.news_headlines (
                id SERIAL PRIMARY KEY,
                headline TEXT NOT NULL,
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                source TEXT NOT NULL,
                url TEXT NOT NULL,
                symbols TEXT[] NOT NULL,
                sentiment FLOAT NOT NULL,
                impact_score FLOAT NOT NULL,
                collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (headline, published_at)
            );
        """)
        
        # Create indexes
        logger.info("Creating indexes...")
        cur.execute("""
            CREATE INDEX idx_news_symbols ON trading.news_headlines USING GIN(symbols);
            CREATE INDEX idx_news_published ON trading.news_headlines (published_at);
            CREATE INDEX idx_news_collected ON trading.news_headlines (collected_at);
        """)
        
        # Add comments
        logger.info("Adding table comments...")
        cur.execute("""
            COMMENT ON TABLE trading.news_headlines IS 
            'Stores news headlines with sentiment and impact analysis';
            
            COMMENT ON COLUMN trading.news_headlines.sentiment IS 
            'Sentiment score from -1.0 (negative) to 1.0 (positive)';
            
            COMMENT ON COLUMN trading.news_headlines.impact_score IS 
            'Impact score from 1 (low) to 10 (high)';
        """)
        
        # Grant permissions
        logger.info("Granting permissions...")
        cur.execute("""
            GRANT SELECT, INSERT ON trading.news_headlines TO collector;
            GRANT USAGE ON SEQUENCE trading.news_headlines_id_seq TO collector;
        """)
        
        # Commit the changes
        conn.commit()
        logger.info("Migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during migration: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    migrate_news_table() 