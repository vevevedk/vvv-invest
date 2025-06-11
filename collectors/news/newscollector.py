#!/usr/bin/env python3

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from sqlalchemy import create_engine, text
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from datetime import timezone
from celery import shared_task
import hashlib
import pickle
import logging

from collectors.schema_validation import NewsSchemaValidator
from config.db_config import get_db_config
from config.api_config import UW_BASE_URL, DEFAULT_HEADERS, REQUEST_TIMEOUT
from collectors.utils.logging_config import (
    setup_logging, log_heartbeat, log_collector_summary, log_error, log_warning, log_info
)
from collectors.utils.market_utils import is_market_open

# Set up logging
logger = setup_logging('news_collector', 'news_collector.log')

# Constants
NEWS_API_ENDPOINT = f"{UW_BASE_URL}/news/headlines"
BATCH_SIZE = 100
MAX_RETRIES = 3
MAX_DAYS_BACKFILL = 7
CACHE_DIR = Path('cache/news')
CACHE_EXPIRY_DAYS = 7
CREDITS_PER_REQUEST = 1  # Each API request costs 1 credit
MARKET_OPEN_COLLECTION_INTERVAL = 5  # minutes
MARKET_CLOSED_COLLECTION_INTERVAL = 15  # minutes

def get_db_connection():
    """Get database connection using configuration."""
    db_config = get_db_config()
    logger = logging.getLogger('news_collector')
    logger.info('Opening new SQLAlchemy engine connection')
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}",
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
    )
    return engine

class NewsCollector:
    """Collector for news articles with pagination and date range support."""
    
    def __init__(self):
        self.batch_size = 100
        self.max_parallel_requests = 2
        self.request_timeout = 30
        self.retry_delay = 2.0
        self.max_retries = 3
        self.total_articles = 0
        self.last_progress_update = 0
        self.max_pages = 10
        self.last_logged_page = -1
        self.daily_request_count = 0
        self.daily_limit = 15000
        self.rate_limit_delay = 1.0
        self.validator = NewsSchemaValidator
        self.api_endpoint = NEWS_API_ENDPOINT
        self.headers = DEFAULT_HEADERS
        self.engine = get_db_connection()
        self._create_schema_if_not_exists()
        self._setup_cache()
        
        # API credit tracking
        self.total_credits_used = 0
        self.cached_requests = 0
        self.failed_requests = 0
        self.start_time = None

    def _log_credit_usage(self, request_type: str, credits: int = CREDITS_PER_REQUEST):
        """Log API credit usage."""
        self.total_credits_used += credits
        logger.info(f"API Credit Usage - {request_type}: {credits} credits (Total: {self.total_credits_used})")

    def _print_credit_summary(self):
        """Print summary of API credit usage."""
        if not self.start_time:
            return
            
        duration = datetime.now() - self.start_time
        logger.info("\nAPI Credit Usage Summary:")
        logger.info("=" * 50)
        logger.info(f"Total Credits Used: {self.total_credits_used}")
        logger.info(f"Cached Requests: {self.cached_requests}")
        logger.info(f"Failed Requests: {self.failed_requests}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Credits per minute: {self.total_credits_used / (duration.total_seconds() / 60):.2f}")
        logger.info("=" * 50)

    def _setup_cache(self):
        """Set up cache directory and clean old cache files."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._clean_old_cache()

    def _clean_old_cache(self):
        """Remove cache files older than CACHE_EXPIRY_DAYS."""
        expiry_date = datetime.now() - timedelta(days=CACHE_EXPIRY_DAYS)
        for cache_file in CACHE_DIR.glob('*.pkl'):
            if cache_file.stat().st_mtime < expiry_date.timestamp():
                cache_file.unlink()

    def _get_cache_key(self, params: Dict[str, Any]) -> str:
        """Generate a cache key from request parameters."""
        param_str = json.dumps(params, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()

    def _get_cached_data(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Retrieve data from cache if available."""
        cache_file = CACHE_DIR / f"{cache_key}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    self.cached_requests += 1
                    self._log_credit_usage("Cached Request", 0)  # No credits used for cached requests
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"Cache read error: {str(e)}")
        return None

    def _save_to_cache(self, cache_key: str, data: List[Dict[str, Any]]):
        """Save data to cache."""
        cache_file = CACHE_DIR / f"{cache_key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.warning(f"Cache write error: {str(e)}")

    def _split_date_range(self, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime]]:
        """Split date range into smaller chunks to optimize API usage."""
        date_ranges = []
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=1), end_date)
            date_ranges.append((current_start, current_end))
            current_start = current_end
            
        return date_ranges

    def _create_schema_if_not_exists(self):
        """Create the news headlines schema and table if they don't exist."""
        try:
            logger.info('Opening DB connection')
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
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
                logger.info("Schema ready")
        except Exception as e:
            logger.error(f"Schema error: {str(e)}")
            raise
        finally:
            logger.info('Closed DB connection')

    def _check_api_limit(self) -> bool:
        """Check if we're approaching the API limit."""
        if self.daily_request_count >= (self.daily_limit * 0.9):
            logger.warning(f"API limit: {self.daily_request_count}/{self.daily_limit}")
            return False
        return True

    def _make_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Make a single API request with retry logic and caching."""
        cache_key = self._get_cache_key(params)
        cached_data = self._get_cached_data(cache_key)
        
        if cached_data is not None:
            logger.info(f"Using cached data for request: {params}")
            return cached_data

        for attempt in range(self.max_retries):
            try:
                if not self._check_api_limit():
                    return []
                    
                response = requests.get(
                    self.api_endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=self.request_timeout
                )
                
                if response.status_code != 200:
                    logger.error(f"API error: {response.status_code}")
                    response.raise_for_status()
                
                self.daily_request_count += 1
                data = response.json()
                articles = data.get('data', [])
                
                # Log credit usage
                self._log_credit_usage("API Request")
                
                # Cache the results
                self._save_to_cache(cache_key, articles)
                
                return articles
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                self.failed_requests += 1
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
        return []

    def fetch_data(self, start_date=None, end_date=None):
        """Fetch news headlines from the API with optimized date range handling."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(hours=24)
        if not end_date:
            end_date = datetime.now(timezone.utc)

        # Initialize credit tracking
        self.start_time = datetime.now()
        self.total_credits_used = 0
        self.cached_requests = 0
        self.failed_requests = 0

        # Split date range into daily chunks
        date_ranges = self._split_date_range(start_date, end_date)
        all_articles = []
        total_articles = 0
        max_articles = 1000  # Limit total articles to prevent excessive fetching

        for chunk_start, chunk_end in date_ranges:
            if total_articles >= max_articles:
                break

            start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%S%z')
            end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%S%z')

            page = 0
            while page < self.max_pages and total_articles < max_articles:
                params = {
                    'limit': self.batch_size,
                    'page': page,
                    'newer_than': start_str,
                    'older_than': end_str
                }

                try:
                    articles = self._make_request(params)
                    if not articles:
                        break

                    filtered_articles = [
                        article for article in articles
                        if chunk_start <= datetime.fromisoformat(article['created_at'].replace('Z', '+00:00')) <= chunk_end
                    ]

                    all_articles.extend(filtered_articles)
                    total_articles += len(filtered_articles)
                    
                    if len(articles) < self.batch_size or total_articles >= max_articles:
                        break

                    page += 1
                    time.sleep(self.rate_limit_delay)

                except Exception as e:
                    logger.error(f"Page {page} error: {str(e)}")
                    break

            # Add delay between date chunks
            time.sleep(self.rate_limit_delay * 2)

        # Print credit usage summary
        self._print_credit_summary()

        logger.info(f"Fetched {total_articles} articles")
        return all_articles

    def save_headlines(self, headlines):
        """Save headlines to the database."""
        if not headlines:
            logger.warning("No headlines to save")
            return

        try:
            logger.info('Opening DB connection')
            with self.engine.connect() as conn:
                for headline in headlines:
                    formatted_headline = {
                        'headline': headline['headline'],
                        'source': headline.get('source'),
                        'created_at': datetime.fromisoformat(headline['created_at'].replace('Z', '+00:00')),
                        'tags': headline.get('tags', []),
                        'tickers': headline.get('tickers', []),
                        'is_major': headline.get('is_major', False),
                        'sentiment': headline.get('sentiment'),
                        'meta': json.dumps(headline.get('meta', {}))
                    }
                    conn.execute(text("""
                        INSERT INTO trading.news_headlines (
                            headline, source, created_at, tags, tickers, is_major, sentiment, meta
                        ) VALUES (
                            :headline, :source, :created_at, :tags, :tickers, :is_major, :sentiment, :meta
                        )
                        ON CONFLICT (headline, source, created_at) DO NOTHING
                    """), formatted_headline)
                conn.commit()
                logger.info(f"Saved {len(headlines)} headlines (duplicates ignored)")
        except Exception as e:
            logger.error(f"Save error: {str(e)}")
            raise
        finally:
            logger.info('Closed DB connection')

    def collect(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        log_heartbeat('news', status='running')
        self.start_time = datetime.now()
        try:
            headlines = self.fetch_data(start_date, end_date)
            self.total_articles = len(headlines)
            self.save_headlines(headlines)
            log_collector_summary(
                collector_name='news',
                start_time=self.start_time,
                end_time=datetime.now(),
                items_collected=self.total_articles,
                api_credits_used=self.total_credits_used,
                task_type='collect',
                status='collected'
            )
        except Exception as e:
            log_error('news', e, task_type='collect')
            raise
        finally:
            logger.info('Disposing SQLAlchemy engine')
            self.engine.dispose()

    def get_all_headlines(self) -> pd.DataFrame:
        """Get all headlines from the database."""
        try:
            with self.engine.connect() as conn:
                query = text("SELECT * FROM trading.news_headlines ORDER BY created_at DESC")
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            raise

    def backfill(self, start_date=None, end_date=None, days=7):
        log_heartbeat('news', status='backfill')
        self.start_time = datetime.now()
        try:
            if not start_date:
                start_date = datetime.now(timezone.utc) - timedelta(days=days)
            if not end_date:
                end_date = datetime.now(timezone.utc)
            
            logger.info(f"Backfilling news headlines from {start_date} to {end_date}")
            
            # Get the last collected date from the database
            try:
                with self.engine.connect() as conn:
                    query = text("SELECT MAX(created_at) as last_date FROM trading.news_headlines")
                    result = conn.execute(query).fetchone()
                    last_collected_date = result[0] if result and result[0] else None
                    
                    if last_collected_date:
                        # Adjust start_date to avoid re-fetching existing data
                        start_date = max(start_date, last_collected_date + timedelta(seconds=1))
                        logger.info(f"Adjusted start date to {start_date} based on last collected date")
            except Exception as e:
                logger.warning(f"Could not determine last collected date: {str(e)}")
            
            self.collect(start_date=start_date, end_date=end_date)
            log_collector_summary(
                collector_name='news',
                start_time=self.start_time,
                end_time=datetime.now(),
                items_collected=self.total_articles,
                api_credits_used=self.total_credits_used,
                task_type='backfill',
                status='collected'
            )
        except Exception as e:
            log_error('news', e, task_type='backfill')
            raise
        finally:
            logger.info('Disposing SQLAlchemy engine')
            self.engine.dispose()

@shared_task
def run_news_collector(minutes: int = 10) -> Dict[str, Any]:
    try:
        log_heartbeat('news', status='running')
        collector = NewsCollector()
        collector.collect()
        return {"status": "success"}
    except Exception as e:
        log_error('news', e, task_type='run_news_collector')
        return {"status": "error", "error": str(e)}

if __name__ == "__main__":
    collector = NewsCollector()
    collector.collect() 