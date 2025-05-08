"""
Database Configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'defaultdb'),
    'user': os.getenv('DB_USER', 'doadmin'),
    'password': os.getenv('DB_PASSWORD', 'AVNS_SrG4Bo3B7uCNEPONkE4'),
    'host': os.getenv('DB_HOST', 'vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com'),
    'port': os.getenv('DB_PORT', '25060'),
    'sslmode': 'require'  # Force SSL mode for production
}

# Schema and table configuration
SCHEMA_NAME = 'trading'
TABLE_NAME = 'darkpool_trades' 