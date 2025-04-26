#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta
import pytz
from options_flow_collector import OptionsFlowCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_collector():
    """Test the options flow collector with a limited set of symbols"""
    # Test with just a few symbols and recent expiries
    test_symbols = ['SPY', 'QQQ']  # Limited set for testing
    
    # Create collector instance
    collector = OptionsFlowCollector()
    
    # Override the symbols list for testing
    collector.SYMBOLS = test_symbols
    
    try:
        # Test historical collection for yesterday
        yesterday = datetime.now(pytz.timezone('US/Eastern')) - timedelta(days=1)
        logger.info(f"Starting test collection for {yesterday.strftime('%Y-%m-%d')}")
        
        # Run the collector
        result = collector.collect_flow(historical_date=yesterday)
        
        if result is not None and not result.empty:
            logger.info(f"Test collection completed successfully. Collected {len(result)} records.")
        else:
            logger.warning("Test collection completed but no data was collected.")
            
    except Exception as e:
        logger.error(f"Test collection failed: {str(e)}")
        raise

if __name__ == '__main__':
    test_collector() 