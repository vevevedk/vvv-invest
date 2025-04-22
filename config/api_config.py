"""
API configuration settings
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Unusual Whales API configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
UW_BASE_URL = "https://api.unusualwhales.com/api"
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"
DARKPOOL_TICKER_ENDPOINT = "/darkpool"

# Request configuration
DEFAULT_HEADERS = {
    "Authorization": f"Bearer {UW_API_TOKEN}",
    "Accept": "application/json"
}
REQUEST_TIMEOUT = 30
REQUEST_RATE_LIMIT = 5  # requests per second 