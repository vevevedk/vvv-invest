#!/usr/bin/env python3

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def run_migration(env_file='.env'):
    # Load environment variables from specified .env file
    load_dotenv(env_file)
    
    # Import get_db_config from config.db_config
    from config.db_config import get_db_config
    
    # Get database configuration
    db_config = get_db_config()
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    
    # Create engine
    engine = create_engine(connection_string)
    
    # SQL to drop redundant columns
    sql = """
    ALTER TABLE trading.news_headlines
    DROP COLUMN IF EXISTS url,
    DROP COLUMN IF EXISTS content,
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS symbols,
    DROP COLUMN IF EXISTS sentiment,
    DROP COLUMN IF EXISTS impact_score;
    """
    
    # Execute the migration
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    print("Migration completed: Cleaned up redundant columns.")

if __name__ == "__main__":
    # Get environment file from command line argument, default to .env
    env_file = sys.argv[1] if len(sys.argv) > 1 else '.env'
    run_migration(env_file) 