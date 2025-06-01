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

def check_table_structure():
    """Check the structure of the news_headlines table."""
    engine = get_db_connection()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'trading' 
            AND table_name = 'news_headlines'
            ORDER BY ordinal_position;
        """))
        columns = result.fetchall()
        logger.info("Table structure:")
        for col in columns:
            logger.info(f"Column: {col[0]}, Type: {col[1]}")

if __name__ == "__main__":
    check_table_structure() 