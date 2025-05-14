import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# News API Configuration
NEWS_API_TOKEN = os.getenv('NEWS_API_TOKEN')
if not NEWS_API_TOKEN:
    raise ValueError("NEWS_API_TOKEN environment variable is not set")

NEWS_BASE_URL = "https://api.unusualwhales.com/news"
NEWS_ENDPOINT = f"{NEWS_BASE_URL}/latest"

DEFAULT_HEADERS = {
    "Authorization": f"Bearer {NEWS_API_TOKEN}",
    "Content-Type": "application/json"
}

REQUEST_TIMEOUT = 30  # seconds 