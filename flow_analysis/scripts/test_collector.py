#!/usr/bin/env python3

import os
import logging
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more info
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_api():
    """Test API with different endpoints and auth methods"""
    api_key = os.getenv("UW_API_TOKEN")
    if not api_key:
        logger.error("UW_API_TOKEN environment variable not set")
        return False

    # Test different endpoint variations
    endpoints = [
        "https://api.unusualwhales.com/api/v2/options/flow",
        "https://api.unusualwhales.com/api/options/flow",
        "https://api.unusualwhales.com/options/flow",
        "https://unusualwhales.com/api/options/flow"
    ]

    # Test different auth methods
    auth_headers = [
        {"Authorization": f"Bearer {api_key}"},
        {"Authorization": api_key},
        {"X-API-Key": api_key},
        {"api-key": api_key},
        {"token": api_key}
    ]

    for url in endpoints:
        logger.debug(f"\nTesting URL: {url}")
        
        for headers in auth_headers:
            logger.debug(f"Testing with headers: {headers}")
            
            try:
                response = requests.get(url, headers=headers, timeout=10)
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                
                if response.ok:
                    data = response.json()
                    logger.info(f"Success with URL: {url}")
                    logger.info(f"Using headers: {headers}")
                    logger.debug(f"Response data type: {type(data)}")
                    logger.debug(f"Response data preview: {str(data)[:200]}")
                    return True
                else:
                    logger.debug(f"Failed with status {response.status_code}")
                    logger.debug(f"Response text: {response.text}")
                    
            except Exception as e:
                logger.debug(f"Error during API call: {str(e)}")
                continue
    
    logger.error("All API endpoint combinations failed")
    return False

if __name__ == "__main__":
    print("Starting API test...")
    test_api() 