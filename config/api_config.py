"""
API configuration settings
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Unusual Whales API configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
UW_BASE_URL = "https://api.unusualwhales.com/api/v1"  # Updated to v1 API
OPTION_CONTRACTS_ENDPOINT = "/options/contracts/{ticker}"
OPTION_FLOW_ENDPOINT = "/options/flow/{ticker}"
QUOTE_ENDPOINT = "/quotes/{symbol}"
HEALTH_ENDPOINT = "/status"  # Updated health check endpoint

# Request configuration
DEFAULT_HEADERS = {
    "Authorization": f"Bearer {UW_API_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}
REQUEST_TIMEOUT = 30
REQUEST_RATE_LIMIT = 5  # requests per second 