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
    'darkpool_queue': {
        'exchange': 'darkpool_queue',
        'routing_key': 'darkpool_queue',
    },
} 