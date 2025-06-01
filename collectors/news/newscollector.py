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
from celery import Celery
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

from collectors.schema_validation import NewsSchemaValidator
from config.db_config import get_db_config
from config.celery.news_celery_app import app as celery_app
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
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
    )

def validate_schema():
    """Validate the news headlines schema."""
    engine = get_db_connection()
    return NewsSchemaValidator.validate_news_schema(engine)

@celery_app.task
def collect_news_headlines():
    """Celery task to collect news headlines in production mode."""
    try:
        collector = NewsCollector(is_production=True)
        collector.collect()
        return True
    except Exception as e:
        logger.error(f"Error in Celery task: {str(e)}")
        return False

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
        collector = NewsCollector()  # Remove is_production parameter
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
        self._create_schema_if_not_exists()

    def _create_schema_if_not_exists(self):
        """Create the news headlines schema and table if they don't exist."""
        engine = get_db_connection()
        with engine.connect() as conn:
            # Create schema if it doesn't exist
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
            
            # Create table if it doesn't exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trading.news_headlines (
                    id SERIAL PRIMARY KEY,
                    headline TEXT NOT NULL,
                    source VARCHAR(255),
                    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    tags TEXT[],
                    tickers TEXT[],
                    is_major BOOLEAN,
                    sentiment TEXT,
                    meta JSONB,
                    collection_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.commit()
            logger.info("News headlines schema and table created/verified")

    def _check_api_limit(self) -> bool:
        """Check if we're approaching the API limit."""
        if self.daily_request_count >= (self.daily_limit * 0.9):
            logger.warning(f"Approaching API limit: {self.daily_request_count}/{self.daily_limit} requests used")
            return False
        return True

    def _make_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Make a single API request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                if not self._check_api_limit():
                    return []
                    
                # Log request details
                logger.info(f"Making API request with params: {params}")
                
                response = requests.get(
                    NEWS_API_ENDPOINT,
                    headers=DEFAULT_HEADERS,
                    params=params,
                    timeout=self.request_timeout
                )
                
                # Log response status only
                logger.info(f"API Response Status: {response.status_code}")
                
                response.raise_for_status()
                self.daily_request_count += 1
                data = response.json()
                
                # Log number of articles received instead of full data
                articles = data.get('data', [])
                logger.info(f"Received {len(articles)} articles from API")
                
                return articles
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
        return []

    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch news data with parallel requests and optimized pagination."""
        all_articles = []
        page = 0
        retry_count = 0
        self.total_articles = 0
        self.last_progress_update = time.time()
        self.last_logged_page = -1
        
        # Convert dates to timezone-aware datetime objects
        if start_date:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            start_dt = start_dt.replace(tzinfo=pytz.UTC)
        else:
            start_dt = None
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            end_dt = end_dt.replace(tzinfo=pytz.UTC)
        else:
            end_dt = None
        
        # Only use latest article date if we're not in backfill mode
        if not start_date:
            latest_article_date = self._get_latest_article_date()
            if latest_article_date:
                start_date = latest_article_date.isoformat()
                start_dt = latest_article_date
        
        while page < self.max_pages:
            if not self._check_api_limit():
                logger.warning("Stopping collection due to API limit")
                return all_articles

            futures = []
            with ThreadPoolExecutor(max_workers=self.max_parallel_requests) as executor:
                for i in range(self.max_parallel_requests):
                    current_page = page + i
                    params = {
                        'limit': self.batch_size,
                        'page': current_page
                    }
                    
                    # For backfill mode, use older_than to get historical data
                    if start_date and end_date:
                        params['older_than'] = end_date
                        params['newer_than'] = start_date
                    elif start_date:
                        params['newer_than'] = start_date
                    elif end_date:
                        params['older_than'] = end_date
                    
                    futures.append(executor.submit(self._make_request, params))
                
                for future in as_completed(futures):
                    try:
                        data = future.result()
                        if not data:
                            continue
                            
                        filtered = self._filter_by_date(data, start_dt, end_dt)
                        all_articles.extend(filtered)
                        self.total_articles += len(filtered)
                        
                        current_time = time.time()
                        if current_time - self.last_progress_update >= 2 and page > self.last_logged_page:
                            logger.info(f"Progress: {self.total_articles} articles (page {page}/{self.max_pages})")
                            self.last_progress_update = current_time
                            self.last_logged_page = page
                        
                        if len(data) < self.batch_size:
                            return all_articles
                            
                    except Exception as e:
                        if retry_count < self.max_retries:
                            retry_count += 1
                            time.sleep(self.retry_delay * (2 ** retry_count))
                            continue
                        else:
                            return all_articles
            
            page += self.max_parallel_requests
            time.sleep(self.rate_limit_delay)
            
        return all_articles

    def _get_latest_article_date(self):
        """Get the latest article date from the database."""
        engine = get_db_connection()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(created_at) 
                FROM trading.news_headlines
            """))
            return result.scalar()

    def _filter_by_date(self, data: List[Dict[str, Any]], start_dt: Optional[datetime], end_dt: Optional[datetime]) -> List[Dict[str, Any]]:
        """Filter articles based on date range."""
        filtered = []
        for article in data:
            # Use created_at from API response but store it as published_at
            article_date = datetime.fromisoformat(article['created_at'].replace('Z', '+00:00'))
            article_date = article_date.replace(tzinfo=pytz.UTC)
            if (start_dt is None or article_date >= start_dt) and (end_dt is None or article_date <= end_dt):
                filtered.append(article)
        return filtered

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
    
    def save_headlines(self, headlines: List[Dict[str, Any]]) -> None:
        """Save headlines to the database."""
        if not headlines:
            return
            
        with get_db_connection().connect() as conn:
            for headline in headlines:
                # Add collection time
                headline_data = headline.copy()
                headline_data['collected_at'] = datetime.now(pytz.UTC)
                
                conn.execute(
                    text("""
                    INSERT INTO trading.news_headlines 
                    (headline, source, created_at, tags, tickers, is_major, sentiment, meta, collected_at)
                    VALUES (:headline, :source, :created_at, :tags, :tickers, :is_major, :sentiment, :meta, :collected_at)
                    """),
                    headline_data
                )
            conn.commit()

if __name__ == "__main__":
    main() 