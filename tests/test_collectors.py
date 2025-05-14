#!/usr/bin/env python3

"""
Script to test news and dark pool collectors locally
"""

import logging
from collectors.news_collector import NewsCollector
from collectors.darkpool_collector import DarkPoolCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_collectors():
    """Test both collectors locally"""
    try:
        # Test news collector
        print("\nTesting News Collector...")
        news_collector = NewsCollector()
        news_collector.run()
        
        # Test dark pool collector
        print("\nTesting Dark Pool Collector...")
        darkpool_collector = DarkPoolCollector()
        darkpool_collector.run()
        
        print("\nAll collectors tested successfully!")
        
    except Exception as e:
        print(f"Error testing collectors: {str(e)}")
        raise

if __name__ == "__main__":
    test_collectors() 