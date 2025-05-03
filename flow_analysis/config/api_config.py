"""
Unusual Whales API Configuration
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN', '9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999')
UW_BASE_URL = "https://api.unusualwhales.com/api"  # Base URL for all endpoints

# Endpoints
OPTION_CONTRACTS_ENDPOINT = "/stock/{ticker}/option-contracts"  # Get all option contracts for a ticker
OPTION_FLOW_ENDPOINT = "/option-contract/{id}/flow"  # Get flow data for a specific contract
EXPIRY_BREAKDOWN_ENDPOINT = "/stock/{ticker}/expiry-breakdown"  # Get expiry dates for a ticker
NEWS_HEADLINES_ENDPOINT = "/news/headlines"  # Get news headlines

# Request Configuration
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {UW_API_TOKEN}"
}

# Rate Limiting
REQUEST_RATE_LIMIT = 2  # requests per second (reduced from 10 to avoid 429 errors)
REQUEST_TIMEOUT = 30  # seconds 