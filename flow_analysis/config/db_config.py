"""
Database Configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSL_MODE', 'disable')  # Default to disable for local development
}

# Schema and table configuration
SCHEMA_NAME = 'trading'
TABLE_NAME = 'darkpool_trades' 