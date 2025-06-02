#!/usr/bin/env python3

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
from sqlalchemy import create_engine, text
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from datetime import timezone

from collectors.schema_validation import NewsSchemaValidator
from config.db_config import get_db_config
from config.api_config import UW_BASE_URL, DEFAULT_HEADERS, REQUEST_TIMEOUT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/collector/news_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
NEWS_API_ENDPOINT = f"{UW_BASE_URL}/news/headlines"
BATCH_SIZE = 100
MAX_RETRIES = 3
MAX_DAYS_BACKFILL = 7

def get_db_connection():
    """Get database connection using configuration."""
    db_config = get_db_config()
    return create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}",
        pool_size=5,  # Limit pool size
        max_overflow=10,  # Allow some overflow connections
        pool_timeout=30,  # Timeout for getting a connection from pool
        pool_recycle=1800,  # Recycle connections after 30 minutes
    )

def validate_schema():
    """Validate the news headlines schema."""
    engine = get_db_connection()
    try:
        return NewsSchemaValidator.validate_news_schema(engine)
    finally:
        engine.dispose()

def fetch_headlines(symbols: List[str], start_date: str, end_date: str) -> List[Dict]:
    """Fetch headlines from the API."""
    try:
        params = {
            'limit': BATCH_SIZE,
            'major_only': False,
            'page': 0,
            'search_term': '',
            'sources': ''
        }

        response = requests.get(
            NEWS_API_ENDPOINT,
            headers=DEFAULT_HEADERS,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        if not data or 'data' not in data:
            return []
            
        return data['data']

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching headlines: {str(e)}")
        return []

def main():
    """Command-line interface for the news collector."""
    import argparse
    
    parser = argparse.ArgumentParser(description='News Headlines Collector')
    parser.add_argument('--mode', choices=['production', 'backfill'], default='backfill',
                      help='Collection mode: production (incremental) or backfill (date range)')
    parser.add_argument('--start-date', help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for backfill (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        collector = NewsCollector()
        collector.collect(start_date=args.start_date, end_date=args.end_date)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

class NewsCollector:
    """Collector for news articles with pagination and date range support."""
    
    def __init__(self):
        self.batch_size = 100
        self.max_parallel_requests = 2  # Reduced from 5 to 2
        self.request_timeout = 30
        self.retry_delay = 2.0  # Increased from 1.0
        self.max_retries = 3
        self.total_articles = 0
        self.last_progress_update = 0
        self.max_pages = 10
        self.last_logged_page = -1
        self.daily_request_count = 0
        self.daily_limit = 15000
        self.rate_limit_delay = 1.0
        self.validator = NewsSchemaValidator  # Use the class directly
        self.logger = logging.getLogger(__name__)
        self.api_endpoint = NEWS_API_ENDPOINT
        self.headers = DEFAULT_HEADERS
        self.engine = get_db_connection()
        self._create_schema_if_not_exists()

    def _create_schema_if_not_exists(self):
        """Create the news headlines schema and table if they don't exist."""
        try:
            with self.engine.connect() as conn:
                # Create schema if it doesn't exist
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
                
                # Create table if it doesn't exist
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS trading.news_headlines (
                        id SERIAL PRIMARY KEY,
                        headline TEXT NOT NULL,
                        source VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        tags TEXT[],
                        tickers TEXT[],
                        is_major BOOLEAN,
                        sentiment TEXT,
                        meta JSONB,
                        collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                conn.commit()
                self.logger.info("News headlines schema and table created/verified")
        except Exception as e:
            self.logger.error(f"Error creating schema: {str(e)}")
            raise

    def _check_api_limit(self) -> bool:
        """Check if we're approaching the API limit."""
        if self.daily_request_count >= (self.daily_limit * 0.9):
            self.logger.warning(f"Approaching API limit: {self.daily_request_count}/{self.daily_limit} requests used")
            return False
        return True

    def _make_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Make a single API request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                if not self._check_api_limit():
                    return []
                    
                # Log request details
                self.logger.info(f"Making request to {self.api_endpoint}")
                self.logger.info(f"Request params: {params}")
                
                response = requests.get(
                    self.api_endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=self.request_timeout
                )
                
                # Log response details
                self.logger.info(f"API Response Status: {response.status_code}")
                
                if response.status_code != 200:
                    self.logger.error(f"API Error Response: {response.text}")
                    response.raise_for_status()
                
                self.daily_request_count += 1
                data = response.json()
                
                # Log response data
                articles = data.get('data', [])
                self.logger.info(f"Received {len(articles)} articles from API")
                
                return articles
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
        return []

    def fetch_data(self, start_date=None, end_date=None):
        """Fetch news headlines from the API"""
        if not start_date:
            start_date = datetime.now()
        if not end_date:
            end_date = datetime.now()

        # Convert to UTC for API
        start_date = start_date.astimezone(timezone.utc)
        end_date = end_date.astimezone(timezone.utc)

        # Format dates for API
        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S%z')
        end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S%z')

        # Check cache first
        cached_articles = self._get_cached_articles(start_date, end_date)
        if cached_articles:
            self.logger.info(f"Using {len(cached_articles)} cached articles")
            return cached_articles

        all_articles = []
        page = 0
        total_articles = 0

        while True:
            params = {
                'limit': 100,
                'page': page,
                'newer_than': start_str
            }

            try:
                response = requests.get(
                    self.api_endpoint,
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()

                if not data.get('data'):
                    break

                articles = data['data']
                if not articles:
                    break

                all_articles.extend(articles)
                total_articles += len(articles)
                self.logger.info(f"Progress: {total_articles} articles (page {page + 1}/10)")

                if len(articles) < 100:
                    break

                page += 1
                if page >= 10:  # Limit to 1000 articles
                    break

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching data: {str(e)}")
                break

        self.logger.info(f"Successfully collected {total_articles} headlines")
        return all_articles

    def _get_cached_articles(self, start_date, end_date):
        # Implementation of _get_cached_articles method
        # This method should return cached articles based on the start_date and end_date
        return []

    def collect(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """Collect news headlines.
        
        Args:
            start_date (str, optional): Start date in YYYY-MM-DD format for backfill mode
            end_date (str, optional): End date in YYYY-MM-DD format for backfill mode
        """
        try:
            data = self.fetch_data(start_date, end_date)
            
            if not data:
                logger.warning("No headlines found")
                return
            
            # Validate and save headlines
            valid_headlines = []
            for headline in data:
                if self.validator.validate(headline):
                    # Convert meta dict to JSON string if it's a dict
                    if isinstance(headline.get('meta'), dict):
                        headline['meta'] = json.dumps(headline['meta'])
                    valid_headlines.append(headline)
            
            if not valid_headlines:
                logger.warning("No valid headlines found after validation")
                return
            
            # Save to database
            self.save_headlines(valid_headlines)
            logger.info(f"Successfully collected {len(valid_headlines)} headlines")
            
        except Exception as e:
            logger.error(f"Error collecting news headlines: {str(e)}")
            raise
    
    def save_headlines(self, headlines):
        """Save headlines to the database"""
        if not headlines:
            self.logger.info("No headlines to save")
            return

        try:
            with self.engine.connect() as conn:
                # Convert headlines to DataFrame
                df = pd.DataFrame(headlines)
                
                # Ensure all required columns exist
                required_columns = ['headline', 'source', 'created_at', 'is_major', 'sentiment', 'tickers', 'tags', 'meta']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                # Convert lists to JSON strings
                df['tickers'] = df['tickers'].apply(lambda x: json.dumps(x) if isinstance(x, list) else '[]')
                df['tags'] = df['tags'].apply(lambda x: json.dumps(x) if isinstance(x, list) else '[]')
                df['meta'] = df['meta'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else '{}')

                # Convert created_at to datetime
                df['created_at'] = pd.to_datetime(df['created_at'])

                # Save to database
                df.to_sql('news_headlines', conn, if_exists='append', index=False)
                self.logger.info(f"Saved {len(headlines)} headlines to database")

        except Exception as e:
            self.logger.error(f"Error saving headlines: {str(e)}")
            raise

if __name__ == "__main__":
    main() 