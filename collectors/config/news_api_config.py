import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# News API Configuration
UW_API_TOKEN = os.getenv("UW_API_TOKEN")
if not UW_API_TOKEN:
    raise ValueError("UW_API_TOKEN environment variable is not set")

NEWS_BASE_URL = "https://api.unusualwhales.com"
NEWS_ENDPOINT = f"{NEWS_BASE_URL}/api/news/headlines"

DEFAULT_HEADERS = {"Authorization": f"Bearer {UW_API_TOKEN}"}

REQUEST_TIMEOUT = 30  # seconds 