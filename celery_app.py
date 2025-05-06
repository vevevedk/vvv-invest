import os
import sys
from celery import Celery

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

app = Celery(
    'collectors',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

app.conf.timezone = 'UTC'

# Import and decorate the task
from collectors.tasks import run_darkpool_collector
run_darkpool_collector = app.task(run_darkpool_collector)

app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'collectors.tasks.run_darkpool_collector',
        'schedule': 300.0,  # every 5 minutes
    },
}

# No need for autodiscover since we're explicitly importing the task 