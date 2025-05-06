from celery_app import app
from collectors.darkpool_collector import DarkPoolCollector

@app.task
def run_darkpool_collector():
    collector = DarkPoolCollector()
    collector.run() 