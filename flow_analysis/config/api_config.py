"""
Unusual Whales API Configuration
"""

# API Configuration
UW_API_TOKEN = "9dd00196-7f7f-4e2c-ad7c-2c2cb6a33999"
UW_BASE_URL = "https://api.unusualwhales.com/api"

# Endpoints
DARKPOOL_RECENT_ENDPOINT = "/darkpool/recent"
DARKPOOL_TICKER_ENDPOINT = "/darkpool"  # Will append /{symbol} in the code

# Request Configuration
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain",
    "Authorization": f"Bearer {UW_API_TOKEN}"
}

# Rate Limiting
REQUEST_RATE_LIMIT = 10  # requests per second
REQUEST_TIMEOUT = 30  # seconds 