#!/usr/bin/env python3

import os
from pathlib import Path
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Load env
load_dotenv()

from flow_analysis.config.db_config import get_db_config

def check_schema():
    db_config = get_db_config()
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    )
    
    with engine.connect() as conn:
        # Get table schema
        result = conn.execute(text("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'trading' 
            AND table_name = 'darkpool_trades'
            ORDER BY ordinal_position;
        """))
        
        print("\nTable Schema:")
        print("-------------")
        for row in result:
            print(f"{row[0]}: {row[1]}")

if __name__ == "__main__":
    check_schema() 