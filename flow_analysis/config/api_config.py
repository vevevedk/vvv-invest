"""
Unusual Whales API Configuration
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Get the environment from ENV_FILE or default to .env
env_file = os.getenv('ENV_FILE', '.env')

# Load environment variables
load_dotenv(env_file)

# API Configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
if not UW_API_TOKEN:
    raise ValueError(f"UW_API_TOKEN environment variable is not set in {env_file}")

# Base URL for API requests
UW_BASE_URL = "https://api.unusualwhales.com/api"  # Remove /v1 from the base URL
OPTION_CONTRACTS_ENDPOINT = '/option-contracts'
OPTION_FLOW_ENDPOINT = "/option-trades/flow-alerts"  # Update to match documentation
EXPIRY_BREAKDOWN_ENDPOINT = '/expiry-breakdown'

# News Endpoints
NEWS_ENDPOINT = "/news/headlines"  # Get news headlines

# Dark Pool Endpoints
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"  # Get recent dark pool trades
DARKPOOL_TICKER_ENDPOINT = "/darkpool/{ticker}"  # Get dark pool trades for a specific ticker

# Request Configuration
REQUEST_TIMEOUT = 30  # seconds
REQUEST_RATE_LIMIT = 60  # requests per minute

# Headers
DEFAULT_HEADERS = {
    'Authorization': f'Bearer {UW_API_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
} 