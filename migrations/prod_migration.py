#!/usr/bin/env python3

import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

# Import get_db_config from config.db_config
from config.db_config import get_db_config

def backup_table(engine):
    """Create a backup of the news_headlines table."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_table_name = f'news_headlines_backup_{timestamp}'
    
    backup_sql = f"""
    CREATE TABLE trading.{backup_table_name} AS 
    SELECT * FROM trading.news_headlines;
    """
    
    with engine.connect() as conn:
        conn.execute(text(backup_sql))
        conn.commit()
    
    print(f"Backup created: trading.{backup_table_name}")
    return backup_table_name

def verify_production():
    """Verify we're connecting to production database."""
    db_config = get_db_config()
    if 'vvv-trading-db-do-user' not in db_config['host']:
        raise ValueError("This appears to be a non-production database. Aborting for safety.")
    print("Verified production database connection.")

def run_migration():
    try:
        # Get database configuration
        db_config = get_db_config()
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
        
        # Create engine
        engine = create_engine(connection_string)
        
        # Verify production
        verify_production()
        
        # Create backup
        backup_table_name = backup_table(engine)
        
        # SQL to add meta column and drop redundant columns
        sql = """
        ALTER TABLE trading.news_headlines
        ADD COLUMN meta JSONB,
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
        
        print("Migration completed successfully:")
        print(f"- Added 'meta' column")
        print(f"- Removed redundant columns")
        print(f"- Backup created as: trading.{backup_table_name}")
        
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        raise

if __name__ == "__main__":
    # Confirm before proceeding
    print("WARNING: This script will modify the production database.")
    print("Please ensure you are running this during off-market hours.")
    response = input("Do you want to proceed? (yes/no): ")
    
    if response.lower() == 'yes':
        run_migration()
    else:
        print("Migration cancelled.") 