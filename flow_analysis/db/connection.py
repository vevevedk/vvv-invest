"""Database connection utilities."""

import os
import psycopg2
from dotenv import load_dotenv

def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    load_dotenv()
    
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME', 'trading_data'),
        user=os.getenv('DB_USER', 'collector'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432')
    ) 