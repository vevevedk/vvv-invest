"""
Centralized environment configuration module.
Handles loading and validation of all environment variables.
"""

import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# API Configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
if not UW_API_TOKEN:
    raise ValueError("UW_API_TOKEN environment variable is not set")

# Database Configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

# Validate required database configuration
required_db_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST']
missing_db_vars = [var for var in required_db_vars if not os.getenv(var)]
if missing_db_vars:
    raise ValueError(f"Missing required database environment variables: {', '.join(missing_db_vars)}")

# Celery Configuration
CELERY_CONFIG = {
    'broker_url': os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    'result_backend': os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'),
    'task_serializer': 'json',
    'accept_content': ['json'],
    'result_serializer': 'json',
    'timezone': 'UTC',
    'enable_utc': True,
}

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_DIR = Path(os.getenv('LOG_DIR', 'logs'))
LOG_DIR.mkdir(exist_ok=True)

# Collector Configuration
COLLECTION_INTERVAL = int(os.getenv('COLLECTION_INTERVAL', '300'))  # 5 minutes in seconds
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30')) 