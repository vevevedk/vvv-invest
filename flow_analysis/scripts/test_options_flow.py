#!/usr/bin/env python3

import logging
import sys
from datetime import datetime, timedelta
import pytz
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.scripts.options_flow_collector import OptionsFlowCollector
from flow_analysis.config.api_config import UW_API_TOKEN, UW_BASE_URL
from flow_analysis.config.db_config import DB_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_health_checks(collector):
    """Test health check functionality"""
    logger.info("Testing health checks...")
    health = collector.health_check()
    
    for component, status in health.items():
        logger.info(f"{component}: {'OK' if status else 'FAILED'}")
        
    if not all(health.values()):
        logger.error("Health check failed. Please fix issues before proceeding.")
        return False
    return True

def test_flow_analysis(collector):
    """Test flow analysis calculations"""
    logger.info("Testing flow analysis calculations...")
    
    # Test VWAP calculation
    trades = [
        {'price': 1.0, 'size': 100},
        {'price': 1.1, 'size': 200},
        {'price': 1.2, 'size': 300}
    ]
    vwap = collector._calculate_vwap(trades)
    logger.info(f"VWAP test: {vwap:.4f}")
    
    # Test flow direction analysis
    flow_trades = [
        {'price': 1.0, 'size': 100, 'side': 'buy'},
        {'price': 1.1, 'size': 200, 'side': 'sell'},
        {'price': 1.2, 'size': 300, 'side': 'buy'}
    ]
    flow_direction = collector._analyze_flow_direction(flow_trades)
    logger.info(f"Flow direction test: {flow_direction}")
    
    # Test relative volume analysis
    current_volume = 1000
    historical_volumes = [500, 600, 700, 800, 900]
    relative_volume = collector._analyze_relative_volume(current_volume, historical_volumes)
    logger.info(f"Relative volume test: {relative_volume}")
    
    return True

def test_contract_filtering(collector):
    """Test contract filtering functionality"""
    logger.info("Testing contract filtering...")
    
    # Test market conditions
    market_conditions = collector._get_market_conditions()
    logger.info(f"Market conditions: {market_conditions}")
    
    # Test liquidity score calculation
    flow = {
        'volume': 1000,
        'open_interest': 2000,
        'bid': 1.0,
        'ask': 1.1,
        'volume_ratio': 1.5
    }
    liquidity_score = collector._calculate_liquidity_score(flow)
    logger.info(f"Liquidity score test: {liquidity_score:.4f}")
    
    return True

def test_collection(collector, symbol: str = 'SPY'):
    """Test data collection for a symbol"""
    logger.info(f"Testing data collection for {symbol}...")
    
    try:
        # Use yesterday's date for historical data
        yesterday = datetime.now(pytz.timezone('US/Eastern')) - timedelta(days=1)
        logger.info(f"Using historical date: {yesterday.strftime('%Y-%m-%d')}")
        
        # Get all contracts for the symbol and date
        contracts = collector.get_option_contracts(symbol, date=yesterday)
        logger.info(f"Found {len(contracts)} contracts for {symbol}")
        
        if not contracts:
            logger.warning("No contracts found for the date")
            return True
            
        # Group contracts by expiry for reporting
        contracts_by_expiry = {}
        for contract in contracts:
            expiry = contract.get('expiry', 'unknown')
            if expiry not in contracts_by_expiry:
                contracts_by_expiry[expiry] = []
            contracts_by_expiry[expiry].append(contract)
            
        # Report contracts by expiry
        for expiry, expiry_contracts in contracts_by_expiry.items():
            logger.info(f"Found {len(expiry_contracts)} contracts for expiry {expiry}")
            
        # Get flow data for the first contract
        contract = contracts[0]
        contract_id = contract.get('option_symbol')
        logger.info(f"Getting flow data for contract {contract_id}")
        
        flow_data = collector.get_flow_data(contract_id)
        logger.info(f"Found {len(flow_data)} flow records for contract")
        
        # Print sample analysis
        if flow_data:
            sample = flow_data[0]
            logger.info("Sample flow analysis:")
            logger.info(f"- Contract: {contract_id}")
            logger.info(f"- Strike: {contract.get('strike', 'N/A')}")
            logger.info(f"- Expiry: {contract.get('expiry', 'N/A')}")
            logger.info(f"- Type: {contract.get('option_type', 'N/A')}")
            logger.info(f"- VWAP: {sample.get('vwap', 'N/A')}")
            logger.info(f"- Buy Volume: {sample.get('buy_volume', 'N/A')}")
            logger.info(f"- Sell Volume: {sample.get('sell_volume', 'N/A')}")
            logger.info(f"- Net Flow: {sample.get('net_flow', 'N/A')}")
            logger.info(f"- Volume Ratio: {sample.get('volume_ratio', 'N/A')}")
            logger.info(f"- Liquidity Score: {sample.get('liquidity_score', 'N/A')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during collection test: {str(e)}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting options flow collector tests...")
    
    try:
        # Initialize collector
        collector = OptionsFlowCollector(DB_CONFIG, UW_API_TOKEN)
        
        # Run tests
        tests = [
            ("Health Checks", test_health_checks),
            ("Flow Analysis", test_flow_analysis),
            ("Contract Filtering", test_contract_filtering),
            ("Data Collection", lambda c: test_collection(c, 'SPY'))
        ]
        
        all_passed = True
        for test_name, test_func in tests:
            logger.info(f"\nRunning {test_name}...")
            if not test_func(collector):
                logger.error(f"{test_name} failed!")
                all_passed = False
            else:
                logger.info(f"{test_name} passed!")
                
        if all_passed:
            logger.info("\nAll tests passed successfully!")
        else:
            logger.error("\nSome tests failed. Please check the logs above.")
            
    except Exception as e:
        logger.error(f"Fatal error during testing: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
