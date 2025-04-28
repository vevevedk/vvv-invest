import logging
import argparse
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flow_analysis.scripts.options_flow_collector import OptionsFlowCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Collect options flow data')
    parser.add_argument('--symbol', type=str, default='SPY', help='Symbol to collect data for')
    parser.add_argument('--historical', action='store_true', help='Collect historical data')
    args = parser.parse_args()

    logger.info(f"Starting options flow collection for {args.symbol}")
    
    collector = OptionsFlowCollector()
    collector.SYMBOLS = [args.symbol]  # Override with single symbol for testing
    
    try:
        if args.historical:
            # Get yesterday's date in Eastern time
            yesterday = datetime.now() - timedelta(days=1)
            result = collector.collect_flow(historical_date=yesterday)
        else:
            result = collector.collect_flow()
            
        if result is not None and not result.empty:
            logger.info(f"Successfully collected {len(result)} records")
        else:
            logger.warning("No data was collected")
            
    except Exception as e:
        logger.error(f"Error collecting flow data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 