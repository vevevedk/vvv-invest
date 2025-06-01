#!/usr/bin/env python3

import logging
from sqlalchemy import create_engine, text
from config.db_config import get_db_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using configuration."""
    db_config = get_db_config()
    return create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    )

def update_news_table():
    """Update the news_headlines table to use created_at and collected_at."""
    engine = get_db_connection()
    with engine.connect() as conn:
        # Add created_at column if it doesn't exist
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_schema = 'trading' 
                               AND table_name = 'news_headlines' 
                               AND column_name = 'created_at') THEN
                    ALTER TABLE trading.news_headlines ADD COLUMN created_at TIMESTAMP WITH TIME ZONE;
                END IF;
            END $$;
        """))
        # Drop published_at column if it exists
        conn.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'trading' 
                            AND table_name = 'news_headlines' 
                            AND column_name = 'published_at') THEN
                    ALTER TABLE trading.news_headlines DROP COLUMN published_at;
                END IF;
            END $$;
        """))
        # Rename collection_time to collected_at if it exists
        conn.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'trading' 
                            AND table_name = 'news_headlines' 
                            AND column_name = 'collection_time') THEN
                    ALTER TABLE trading.news_headlines RENAME COLUMN collection_time TO collected_at;
                END IF;
            END $$;
        """))
        conn.commit()
        logger.info("News headlines table updated successfully.")

if __name__ == "__main__":
    update_news_table() 