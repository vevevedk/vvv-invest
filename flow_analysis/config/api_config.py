"""
Unusual Whales API Configuration
"""

import os
from dotenv import load_dotenv

# Determine which env file to load
env_file = os.getenv("ENV_FILE")  # e.g., ".env" or ".env.prod"
if not env_file:
    app_env = os.getenv("APP_ENV")
    if app_env == "production":
        env_file = ".env.prod"
    elif app_env == "staging":
        env_file = ".env.staging"
    else:
        env_file = ".env"

load_dotenv(env_file)

# API Configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
if not UW_API_TOKEN:
    raise ValueError("UW_API_TOKEN environment variable is not set")

UW_BASE_URL = "https://api.unusualwhales.com/api"  # Base URL without v1

# Endpoints
# Options Flow Endpoints
EXPIRY_BREAKDOWN_ENDPOINT = "/stock/{ticker}/expiry-breakdown"  # Get all expirations for a ticker
OPTION_CONTRACTS_ENDPOINT = "/options/contracts/{ticker}"  # Get all option contracts for a ticker
OPTION_FLOW_ENDPOINT = "/options/flow/{id}"  # Get flow data for a specific contract

# News Endpoints
NEWS_ENDPOINT = "/news/headlines"  # Get news headlines

# Dark Pool Endpoints
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"  # Get recent dark pool trades
DARKPOOL_TICKER_ENDPOINT = "/darkpool/{ticker}"  # Get dark pool trades for a specific ticker

# Request Configuration
DEFAULT_HEADERS = {
    "Authorization": f"Bearer {UW_API_TOKEN}",
    "Content-Type": "application/json"
}

# Rate limiting and timeout settings
REQUEST_TIMEOUT = 30  # seconds
REQUEST_RATE_LIMIT = 1.0  # requests per second 