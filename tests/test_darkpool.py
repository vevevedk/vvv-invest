from celery_app import app

# Send the dark pool collector task with explicit queue routing
result = app.send_task(
    'celery_app.run_darkpool_collector_task',
    queue='darkpool_collector'
)
print(f"Task sent with ID: {result.id}") 