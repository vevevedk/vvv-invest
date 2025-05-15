import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collectors.darkpool_collector import DarkPoolCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    print("Starting test...")
    collector = DarkPoolCollector()
    print("Collector initialized...")
    collector.collect()
    print("Collection completed.") 