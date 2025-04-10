import requests
import json
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnusualWhalesAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.unusualwhales.com"
        self.headers = {
            "Accept": "application/json, text/plain",
            "Authorization": f"Bearer {api_key}"
        }

    def get_recent_darkpool_trades(self, date=None, limit=100, max_premium=None, max_size=None):
        """
        Fetch recent darkpool trades from Unusual Whales API
        
        Args:
            date (str, optional): Trading date in YYYY-MM-DD format
            limit (int, optional): Number of trades to return (default 100, max 200)
            max_premium (int, optional): Maximum premium filter
            max_size (int, optional): Maximum size filter
        """
        endpoint = "/api/darkpool/recent"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "limit": min(limit, 200)  # API limit is 200
        }
        
        if date:
            params["date"] = date
        if max_premium:
            params["max_premium"] = max_premium
        if max_size:
            params["max_size"] = max_size

        logger.info(f"Requesting darkpool trades from: {url}")
        logger.info(f"With parameters: {json.dumps(params, indent=2)}")

        try:
            response = requests.get(url, headers=self.headers, params=params)
            logger.info(f"Status code: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Error: {response.text}")
                return None
            
            data = response.json()
            
            # Log sample of the response structure
            if data.get("data"):
                logger.info("Sample trade data structure:")
                logger.info(json.dumps(data["data"][0], indent=2))
                
                # Convert to DataFrame for easier analysis
                df = pd.DataFrame(data["data"])
                logger.info("\nDataFrame Summary:")
                logger.info(f"Number of trades: {len(df)}")
                logger.info("\nColumns available:")
                logger.info(df.columns.tolist())
                
                return df
            else:
                logger.warning("No data returned from API")
                return None

        except Exception as e:
            logger.error(f"Error fetching darkpool trades: {e}")
            return None

def main():
    # API Key
    API_KEY = "9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999"
    
    # Initialize API client
    client = UnusualWhalesAPI(API_KEY)
    
    # Test recent darkpool trades
    trades = client.get_recent_darkpool_trades(
        limit=10,  # Start with small limit for testing
        max_premium=150000  # Example filter
    )
    
    if trades is not None and not trades.empty:
        logger.info("\nSample of trades:")
        logger.info(trades.head())

if __name__ == "__main__":
    main() 