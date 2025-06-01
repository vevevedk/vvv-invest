#!/usr/bin/env python3

import logging
from sqlalchemy import create_engine, text
from config.db_config import get_db_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using configuration."""
    db_config = get_db_config()
    return create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    )

def recreate_news_table():
    """Drop and recreate the news_headlines table."""
    engine = get_db_connection()
    with engine.connect() as conn:
        # Drop the table if it exists
        conn.execute(text("DROP TABLE IF EXISTS trading.news_headlines;"))
        logger.info("Dropped existing news_headlines table")
        
        # Create schema if it doesn't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
        
        # Create the table with the correct schema
        conn.execute(text("""
            CREATE TABLE trading.news_headlines (
                id SERIAL PRIMARY KEY,
                headline TEXT NOT NULL,
                source VARCHAR(255),
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                tags TEXT[],
                tickers TEXT[],
                is_major BOOLEAN,
                sentiment TEXT,
                meta JSONB,
                collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()
        logger.info("Created news_headlines table with correct schema")

if __name__ == "__main__":
    try:
        recreate_news_table()
        logger.info("Successfully recreated news_headlines table")
    except Exception as e:
        logger.error(f"Error recreating news_headlines table: {str(e)}")
        raise 