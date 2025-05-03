#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta
import pytz
from options_flow_collector import OptionsFlowCollector
from config.db_config import DB_CONFIG
from config.api_config import UW_API_TOKEN

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Get yesterday's date in Eastern time
    eastern = pytz.timezone('US/Eastern')
    yesterday = datetime.now(eastern) - timedelta(days=1)
    
    logger.info(f"Testing historical data collection for {yesterday.strftime('%Y-%m-%d')}")
    
    collector = OptionsFlowCollector(DB_CONFIG, UW_API_TOKEN)
    
    # Test with SPY first
    try:
        collector.collect_flow('SPY', historical_date=yesterday)
        logger.info("Successfully collected historical data for SPY")
    except Exception as e:
        logger.error(f"Error collecting historical data for SPY: {str(e)}")
        raise

if __name__ == "__main__":
    main() 