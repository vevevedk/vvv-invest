#!/usr/bin/env python3

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

# Load environment variables from .env file
load_dotenv()

# Import get_db_config from config.db_config
from config.db_config import get_db_config

def check_schema():
    # Get database configuration
    db_config = get_db_config()
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    
    # Create engine
    engine = create_engine(connection_string)
    
    # Create inspector
    inspector = inspect(engine)
    
    # Get columns for the trading.news_headlines table
    columns = inspector.get_columns('news_headlines', schema='trading')
    
    print("Schema for trading.news_headlines:")
    for column in columns:
        print(f"Column: {column['name']}, Type: {column['type']}")

if __name__ == "__main__":
    check_schema() 