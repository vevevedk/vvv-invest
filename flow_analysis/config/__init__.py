"""Configuration module for flow analysis."""
import os
from dotenv import load_dotenv

env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

API_KEY = os.getenv('API_KEY')