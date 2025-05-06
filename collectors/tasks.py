from collectors.darkpool_collector import DarkPoolCollector
from celery_app import app

@app.task
def run_darkpool_collector():
    collector = DarkPoolCollector()
    collector.run() 