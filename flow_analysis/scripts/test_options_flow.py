#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta
import pytz
from options_flow_collector import OptionsFlowCollector
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_collector():
    """Test the options flow collector with a limited set of symbols"""
    test_symbols = ["SPY", "QQQ"]
    collector = OptionsFlowCollector()
    
    try:
        logger.info("Starting test collection")
        for symbol in test_symbols:
            try:
                logger.info(f"Testing collection for {symbol}")
                
                # Step 1: Get option contracts
                contracts = collector.get_option_contracts(symbol)
                if contracts:
                    logger.info(f"Sample contract data for {symbol}:")
                    logger.info(json.dumps(contracts[0], indent=2))
                
                # Step 2: Get flow data for first contract
                if contracts:
                    first_contract = contracts[0]
                    contract_id = first_contract['option_symbol']
                    logger.info(f"Testing flow data collection for contract: {contract_id}")
                    flow_data = collector.get_flow_data(contract_id)
                    
                    if not flow_data.empty:
                        logger.info("Sample flow data columns:")
                        logger.info(f"Columns: {flow_data.columns.tolist()}")
                        logger.info("First row of data:")
                        logger.info(flow_data.iloc[0].to_dict())
                
                # Step 3: Full collection test
                collector.collect_flow(symbol)
                logger.info(f"Successfully completed test for {symbol}")
                
            except Exception as e:
                logger.error(f"Error testing {symbol}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                
    except Exception as e:
        logger.error(f"Test collection failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_collector()
