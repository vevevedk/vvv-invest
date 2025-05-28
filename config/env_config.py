import os
from pathlib import Path

# Celery configuration
CELERY_CONFIG = {
    'broker_url': os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    'result_backend': os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'),
    'task_serializer': 'json',
    'accept_content': ['json'],
    'result_serializer': 'json',
    'timezone': 'UTC',
    'enable_utc': True,
}

# UW API token
UW_API_TOKEN = os.getenv('UW_API_TOKEN', '')

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_DIR = Path(os.getenv('LOG_DIR', 'logs')) 