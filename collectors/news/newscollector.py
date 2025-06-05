#!/usr/bin/env python3

import os
import json
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
from celery import shared_task

from collectors.schema_validation import NewsSchemaValidator
from config.db_config import get_db_config
from config.api_config import UW_BASE_URL, DEFAULT_HEADERS, REQUEST_TIMEOUT
from collectors.utils.logging_config import setup_logging, log_collector_summary

# Set up logging
logger = setup_logging('news_collector', 'news_collector.log')

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
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
    )

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

    def _create_schema_if_not_exists(self):
        """Create the news headlines schema and table if they don't exist."""
        try:
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

    def _check_api_limit(self) -> bool:
        """Check if we're approaching the API limit."""
        if self.daily_request_count >= (self.daily_limit * 0.9):
            logger.warning(f"API limit: {self.daily_request_count}/{self.daily_limit}")
            return False
        return True

    def _make_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Make a single API request with retry logic."""
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
                
                return articles
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (2 ** attempt))
        return []

    def fetch_data(self, start_date=None, end_date=None):
        """Fetch news headlines from the API"""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(hours=24)
        if not end_date:
            end_date = datetime.now(timezone.utc)

        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S%z')
        end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S%z')

        all_articles = []
        page = 0
        total_articles = 0
        max_articles = 1000  # Limit total articles to prevent excessive fetching

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
                    if start_date <= datetime.fromisoformat(article['created_at'].replace('Z', '+00:00')) <= end_date
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

        logger.info(f"Fetched {total_articles} articles")
        return all_articles

    def save_headlines(self, headlines):
        """Save headlines to the database."""
        if not headlines:
            logger.warning("No headlines to save")
            return

        try:
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
                    """), formatted_headline)
                conn.commit()
                logger.info(f"Saved {len(headlines)} headlines")
        except Exception as e:
            logger.error(f"Save error: {str(e)}")
            raise

    def collect(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """Collect news headlines and save to database."""
        try:
            headlines = self.fetch_data(start_date, end_date)
            self.save_headlines(headlines)
        except Exception as e:
            logger.error(f"Collection error: {str(e)}")
            raise

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
        """Backfill news headlines for the specified date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=days)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        logger.info(f"Backfilling news headlines from {start_date} to {end_date}")
        self.collect(start_date=start_date, end_date=end_date)

@shared_task
def run_news_collector():
    collector = NewsCollector()
    collector.collect()
    return "News collection completed."

if __name__ == "__main__":
    collector = NewsCollector()
    collector.collect() 