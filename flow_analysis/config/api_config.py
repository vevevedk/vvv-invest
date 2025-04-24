"""
Unusual Whales API Configuration
"""

# API Configuration
UW_API_TOKEN = "9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999"
UW_BASE_URL = "https://api.unusualwhales.com/api"

# Endpoints
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"
DARKPOOL_TICKER_ENDPOINT = "/darkpool"  # Will append /{symbol} in the code

# Options Flow Endpoints
EXPIRY_BREAKDOWN_ENDPOINT = "/stock/{ticker}/expiry-breakdown"  # Get all expirations for a ticker
OPTION_CONTRACTS_ENDPOINT = "/stock/{ticker}/option-contracts"  # Get all contracts for a ticker
OPTION_FLOW_ENDPOINT = "/option-contract/{id}/flow"  # Get flow data for a specific contract

# Request Configuration
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain",
    "Authorization": f"Bearer {UW_API_TOKEN}"
}

# Rate Limiting
REQUEST_RATE_LIMIT = 10  # requests per second
REQUEST_TIMEOUT = 30  # seconds 