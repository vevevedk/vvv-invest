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
app.conf.beat_schedule = {
    'run-darkpool-collector-every-5-mins': {
        'task': 'collectors.tasks.run_darkpool_collector',
        'schedule': 300.0,  # every 5 minutes
    },
}

# Import tasks after app is created
from collectors.tasks import run_darkpool_collector

app.autodiscover_tasks(['collectors']) 