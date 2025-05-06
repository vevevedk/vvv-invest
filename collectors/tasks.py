from collectors.darkpool_collector import DarkPoolCollector

def run_darkpool_collector():
    collector = DarkPoolCollector()
    collector.run()

# This will be decorated by celery_app.py 