#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from tabulate import tabulate

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.db_config import DB_CONFIG

def read_sql_file(filename):
    """Read SQL queries from file"""
    with open(filename, 'r') as f:
        return f.read()

def format_table(df):
    """Format DataFrame as a nice table"""
    return tabulate(df, headers='keys', tablefmt='psql', showindex=False)

def main():
    # Connect to production database
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Read SQL queries
        sql_file = Path(__file__).parent / 'analyze_flow_data.sql'
        sql = read_sql_file(sql_file)
        
        # Split into individual queries
        queries = [q.strip() for q in sql.split(';') if q.strip()]
        
        # Execute each query
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for query in queries:
                print("\n" + "="*80)
                cur.execute(query)
                results = cur.fetchall()
                if results:
                    df = pd.DataFrame(results)
                    print(format_table(df))
                    
    finally:
        conn.close()

if __name__ == "__main__":
    main() 