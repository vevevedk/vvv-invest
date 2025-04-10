"""
Simple test script for Polygon API connection
"""

import requests
from config.api_config import POLYGON_API_KEY

def test_api_connection():
    # Test endpoint
    url = "https://api.polygon.io/v3/reference/options/contracts"
    headers = {"Authorization": f"Bearer {POLYGON_API_KEY}"}
    params = {
        "underlying_ticker": "SPY",
        "limit": 1
    }
    
    try:
        print("Testing Polygon API connection...")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        if data.get("results"):
            print("API connection successful!")
            print("\nSample response:")
            print(data["results"][0])
        else:
            print("API connection successful, but no data returned")
            print("\nResponse:", data)
            
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        if hasattr(e.response, 'json'):
            print("\nError response:", e.response.json())

if __name__ == "__main__":
    test_api_connection() 