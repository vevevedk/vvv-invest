"""
Common Celery configuration settings.
"""

# Broker settings
BROKER_URL = 'redis://localhost:6379/0'
RESULT_BACKEND = 'redis://localhost:6379/0'

# Task settings
TASK_SERIALIZER = 'json'
ACCEPT_CONTENT = ['json']
RESULT_SERIALIZER = 'json'
TIMEZONE = 'UTC'
ENABLE_UTC = True

# Connection settings
BROKER_CONNECTION_RETRY_ON_STARTUP = True

# Queue settings
TASK_DEFAULT_QUEUE = 'default'
TASK_QUEUES = {
    'news_queue': {
        'exchange': 'news_queue',
        'routing_key': 'news_queue',
    },
    'dark_pool_queue': {
        'exchange': 'dark_pool_queue',
        'routing_key': 'dark_pool_queue',
    },
} 