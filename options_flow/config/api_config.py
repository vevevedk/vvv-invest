"""
API configuration settings
"""

import os

# Polygon API Configuration
POLYGON_API_KEY = "imqq2tU79_8153YxqhLHcNy8jcCYtWQ1"  # Replace with actual API key
API_RATE_LIMIT = 20  # Increased from 5 to 20 calls per minute

# Data Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# API Endpoints
BASE_URL = "https://api.polygon.io/v3"
OPTIONS_ENDPOINT = "/reference/options/contracts"
TRADES_ENDPOINT = "/trades/options"
AGGREGATES_ENDPOINT = "/options/aggregates" 