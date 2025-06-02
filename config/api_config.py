"""
API configuration settings
"""

import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load production environment file by default
env_file = os.getenv("ENV_FILE", ".env.prod")
logger.info(f"Loading environment from: {env_file}")
load_dotenv(env_file, override=True)

# Unusual Whales API configuration
UW_API_TOKEN = os.getenv('UW_API_TOKEN')
if not UW_API_TOKEN:
    logger.error("UW_API_TOKEN not found in environment variables!")
else:
    logger.info("UW_API_TOKEN loaded successfully")

UW_BASE_URL = "https://api.unusualwhales.com/api"  # Do NOT include /v1 for news endpoints

# Dark Pool endpoints
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"
DARKPOOL_TICKER_ENDPOINT = "/darkpool/{ticker}"

# Options endpoints
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