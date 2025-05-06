from celery import Celery

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

app.autodiscover_tasks(['collectors']) 