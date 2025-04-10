import requests
import logging
import json
import pandas as pd
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Configuration
API_KEY = "imqq2tU79_8153YxqhLHcNy8jcCYtWQ1"
BASE_URL = "https://api.polygon.io/v3"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def test_endpoint(endpoint, params=None):
    """Test a single endpoint and handle pagination"""
    url = f"{BASE_URL}{endpoint}"
    all_results = []
    page_count = 0
    
    try:
        while True:
            page_count += 1
            logger.info(f"Testing endpoint: {url}")
            logger.info(f"With params: {params}")
            response = requests.get(url, headers=HEADERS, params=params)
            logger.info(f"Status code: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Error: {response.text}")
                return False
            
            data = response.json()
            results = data.get("results", [])
            logger.info(f"Page {page_count} results: {len(results)}")
            
            if results:
                all_results.extend(results)
                # Log a sample of the data structure
                if page_count == 1:
                    logger.info("Sample data structure:")
                    logger.info(json.dumps(results[0], indent=2))
            
            # Check for pagination
            next_url = data.get("next_url")
            if not next_url:
                break
                
            url = next_url
            params = None  # Don't need params for next_url as they're included
            
        logger.info(f"Total pages: {page_count}")
        logger.info(f"Total results: {len(all_results)}")
        
        # Save results to CSV
        if all_results:
            df = pd.DataFrame(all_results)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/raw/{endpoint.replace('/', '_')}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved {len(all_results)} results to {filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing endpoint: {e}")
        return False

def test_trades_endpoint():
    """Test the trades endpoint specifically for exchange ID and trf_id fields"""
    endpoint = "/trades/SPY"  # Updated endpoint structure
    test_date = "2023-04-05"
    
    params = {
        "timestamp.gte": f"{test_date}T00:00:00Z",
        "timestamp.lt": f"{test_date}T23:59:59Z",
        "limit": 1000
    }
    
    url = f"{BASE_URL}{endpoint}"
    logger.info(f"Testing trades endpoint: {url}")
    logger.info(f"With params: {json.dumps(params, indent=2)}")
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        logger.info(f"Status code: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error: {response.text}")
            return False
        
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            logger.warning("No results returned from trades endpoint")
            return False
            
        # Log the first result to inspect available fields
        first_trade = results[0]
        logger.info("Sample trade data structure:")
        logger.info(json.dumps(first_trade, indent=2))
        
        # Check specifically for exchange ID and trf_id
        has_exchange_id = "exchange_id" in first_trade
        has_trf_id = "trf_id" in first_trade
        
        logger.info(f"Has exchange_id field: {has_exchange_id}")
        logger.info(f"Has trf_id field: {has_trf_id}")
        
        if has_exchange_id:
            logger.info(f"Exchange ID value: {first_trade['exchange_id']}")
        if has_trf_id:
            logger.info(f"TRF ID value: {first_trade['trf_id']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing trades endpoint: {e}")
        return False

def main():
    # Test date
    test_date = "2023-04-05"
    
    # Endpoints to test
    endpoints = [
        "/reference/options/contracts",
        f"/trades/options/SPY",  # Updated endpoint for trades
        f"/aggs/ticker/options/SPY/range/1/minute/{test_date}/{test_date}"  # Updated endpoint for aggregates
    ]
    
    # Test different parameter combinations
    param_combinations = [
        # Basic query with just underlying ticker
        {
            "underlying_ticker": "SPY",
            "limit": 1000
        },
        
        # Add date range for trades and aggregates
        {
            "timestamp.gte": f"{test_date}T00:00:00Z",
            "timestamp.lt": f"{test_date}T23:59:59Z",
            "limit": 1000
        },
        
        # Add contract type and strike price range
        {
            "contract_type": "call",
            "strike_price.gte": 400,
            "strike_price.lte": 500,
            "limit": 1000
        }
    ]
    
    # Test each endpoint with each parameter combination
    for endpoint in endpoints:
        logger.info(f"\nTesting endpoint: {endpoint}")
        for i, params in enumerate(param_combinations, 1):
            logger.info(f"\nTesting parameter combination {i}:")
            logger.info(json.dumps(params, indent=2))
            success = test_endpoint(endpoint, params)
            logger.info(f"Endpoint {endpoint} {'succeeded' if success else 'failed'}")
            print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    test_trades_endpoint() 