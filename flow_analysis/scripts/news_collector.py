#!/usr/bin/env python3

"""
News Headlines Collector
Collects and processes news headlines from Unusual Whales API
"""

import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import pytz
from typing import Optional, Dict, List
import psycopg2
from psycopg2.extras import execute_values
import requests
import argparse
import json

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, 
    NEWS_HEADLINES_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from config.db_config import DB_CONFIG, SCHEMA_NAME
from config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS

# Constants
BATCH_SIZE = 100  # Number of records to insert at once
COLLECTION_INTERVAL = 300  # 5 minutes in seconds

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "news_collector.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NewsCollector:
    def __init__(self):
        self.base_url = UW_BASE_URL
        self.headers = DEFAULT_HEADERS
        self.last_request_time = 0
        self.eastern = pytz.timezone('US/Eastern')
        
        # Initialize database connection
        self.db_conn = None
        self.connect_db()
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def _rate_limit(self):
        """Implement rate limiting to prevent API throttling"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1.0 / REQUEST_RATE_LIMIT:
            time.sleep(1.0 / REQUEST_RATE_LIMIT - time_since_last_request)
        self.last_request_time = time.time()
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = requests.get(
                    endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Max retries reached for endpoint: {endpoint}")
                    return None

    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        now = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return False
            
        # Create datetime objects for market hours comparison
        market_open = datetime.strptime(MARKET_OPEN, "%H:%M").time()
        market_close = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
        current_time = now.time()
        
        return market_open <= current_time <= market_close

    def collect_news(self) -> pd.DataFrame:
        """Collect news headlines from the API"""
        endpoint = f"{self.base_url}{NEWS_HEADLINES_ENDPOINT}"
        
        params = {
            "limit": 100,  # Maximum allowed
            "major_only": True,  # Focus on significant news
            "sources": "Reuters,Bloomberg,CNBC"  # Major financial news sources
        }
        
        data = self._make_request(endpoint, params)
        if not data or "data" not in data:
            logger.warning("No valid news data received")
            return pd.DataFrame()
            
        # Convert to DataFrame
        news_df = pd.DataFrame(data["data"])
        
        if news_df.empty:
            logger.info("No new headlines collected")
            return news_df
            
        # Add collection timestamp
        news_df['collected_at'] = datetime.now(self.eastern)
        
        # Convert published_at to datetime
        news_df['published_at'] = pd.to_datetime(news_df['created_at'])
        
        # Filter for our watchlist symbols
        news_df = news_df[news_df['tickers'].apply(lambda x: any(symbol in x for symbol in SYMBOLS))]
        
        logger.info(f"Collected {len(news_df)} relevant news headlines")
        return news_df

    def save_news_to_db(self, news_df: pd.DataFrame) -> None:
        """Save news headlines to database"""
        if news_df.empty:
            return
            
        try:
            with self.db_conn.cursor() as cur:
                # Prepare data for insertion
                data = news_df[[
                    'headline', 'source', 'published_at', 'tickers',
                    'sentiment', 'is_major', 'tags', 'meta', 'collected_at'
                ]].values.tolist()
                
                # Insert data in batches
                for i in range(0, len(data), BATCH_SIZE):
                    batch = data[i:i + BATCH_SIZE]
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {SCHEMA_NAME}.news_headlines 
                        (headline, source, published_at, symbols, sentiment, 
                         is_major, tags, meta, collected_at)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                        """,
                        batch
                    )
                    
                self.db_conn.commit()
                logger.info(f"Successfully saved {len(news_df)} news headlines to database")
                
        except Exception as e:
            logger.error(f"Error saving news to database: {str(e)}")
            self.db_conn.rollback()
            raise

    def run(self):
        """Run one collection cycle"""
        # Log market status but collect news regardless
        if not self.is_market_open():
            logger.info("Market is closed, but collecting news anyway")
        else:
            logger.info("Market is open")
            
        logger.info("Starting news collection")
        start_time = time.time()
        
        try:
            news_df = self.collect_news()
            if not news_df.empty:
                self.save_news_to_db(news_df)
                
        except Exception as e:
            logger.error(f"Error during collection cycle: {str(e)}")
            
        duration = time.time() - start_time
        logger.info(f"Collection cycle completed in {duration:.2f} seconds")

    def __del__(self):
        """Clean up database connection"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

def main():
    parser = argparse.ArgumentParser(description='News Headlines Collector')
    parser.add_argument('--continuous', action='store_true', help='Run continuously during market hours')
    args = parser.parse_args()

    collector = NewsCollector()
    
    if args.continuous:
        logger.info("Starting continuous collection mode")
        while True:
            collector.run()
            time.sleep(COLLECTION_INTERVAL)
    else:
        # Single collection cycle
        collector.run()

if __name__ == "__main__":
    main() 