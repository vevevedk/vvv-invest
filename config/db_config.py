"""
Database configuration settings
"""

import os
from dotenv import load_dotenv

env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'trading_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Schema and table names
SCHEMA_NAME = 'trading'
TABLE_NAME = 'darkpool_trades' 