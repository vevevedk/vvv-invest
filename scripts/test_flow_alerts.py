#!/usr/bin/env python3

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('flow_alerts_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
logger.info(f"Using environment file: {os.getenv('ENV_FILE', '.env')}")

from flow_analysis.scripts.flow_alerts_collector import FlowAlertsCollector
from flow_analysis.config.api_config import UW_API_TOKEN, UW_BASE_URL
from flow_analysis.config.db_config import get_db_config

def test_single_symbol(symbol: str):
    """Test flow alerts collection for a single symbol."""
    logger.info(f"Testing flow alerts collection for {symbol}")
    
    # Initialize collector
    collector = FlowAlertsCollector(get_db_config(), UW_API_TOKEN)
    
    # Log API configuration
    logger.info(f"API Base URL: {UW_BASE_URL}")
    logger.info(f"API Token: {UW_API_TOKEN[:5]}...{UW_API_TOKEN[-5:] if len(UW_API_TOKEN) > 10 else ''}")
    
    try:
        # Test the API request directly
        alert_data = collector.get_flow_alerts(symbol)
        logger.info(f"Raw API response: {alert_data}")
        
        if alert_data:
            # Process the alerts
            alerts = collector._process_alert_data(alert_data)
            logger.info(f"Processed alerts: {len(alerts)}")
            if not alerts.empty:
                logger.info(f"Sample alert data:\n{alerts.head()}")
        else:
            logger.error("No alert data received")
            
    except Exception as e:
        logger.error(f"Error testing {symbol}: {str(e)}", exc_info=True)
    finally:
        if hasattr(collector, 'db_conn') and collector.db_conn:
            collector.db_conn.close()

def main():
    """Main test function."""
    # Test with a single symbol first
    test_single_symbol("SPY")
    
    # If successful, you can test more symbols
    # test_single_symbol("AAPL")
    # test_single_symbol("TSLA")

if __name__ == "__main__":
    main() 