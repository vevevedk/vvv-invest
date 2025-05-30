import logging
# Configure logging to be less verbose
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
# Reduce verbosity of requests and urllib3
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import requests
import psycopg2
from psycopg2.extras import DictCursor
import os
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from celery import Task
from celery.exceptions import MaxRetriesExceededError
import pytz

# Load environment variables
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

from collectors.base_collector import BaseCollector
from flow_analysis.config.api_config import (
    UW_API_TOKEN,
    NEWS_ENDPOINT,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT,
    UW_BASE_URL
)

logger = logging.getLogger(__name__)

class NewsCollector(BaseCollector):
    """Collector for news articles with pagination and date range support."""
    
    def __init__(self):
        super().__init__()
        self.batch_size = 100
        self.max_parallel_requests = 2
        self.request_timeout = 30
        self.retry_delay = 2.0
        self.max_retries = 3
        self.daily_request_count = 0
        self.daily_limit = 15000
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.last_cache_update = {}
        self.rate_limit_delay = 1.0
        self.task_timeout = 300  # 5 minutes max per task

    def get_db_connection(self):
        """Get a database connection using environment variables."""
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME", "flow_analysis"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            sslmode=os.getenv("DB_SSLMODE", "prefer")
        )

    def _get_cached_articles(self, date_str):
        """Get articles from cache if available and not expired."""
        if date_str in self.cache:
            last_update = self.last_cache_update.get(date_str, 0)
            if time.time() - last_update < self.cache_ttl:
                return self.cache[date_str]
        return None

    def _update_cache(self, date_str, articles):
        """Update cache with new articles."""
        self.cache[date_str] = articles
        self.last_cache_update[date_str] = time.time()

    def _check_api_limit(self):
        """Check if we're approaching the API limit."""
        if self.daily_request_count >= self.daily_limit * 0.9:  # 90% of limit
            self.logger.warning(f"Approaching API limit: {self.daily_request_count}/{self.daily_limit}")
            return False
        return True

    def collect(self):
        """Default collection with Celery task handling."""
        start_time = time.time()
        
        try:
            data = self.fetch_data()
            processed_data = self.process_data(data)
            self.save_data(processed_data)
            
            execution_time = time.time() - start_time
            if execution_time > self.task_timeout:
                self.logger.warning(f"Task execution time ({execution_time:.2f}s) exceeded timeout ({self.task_timeout}s)")
                
        except Exception as e:
            self.logger.error(f"Error in news collection: {str(e)}")
            raise

    def fetch_data(self, start_date=None, end_date=None):
        """Fetch news articles with improved error handling and rate limiting."""
        if not self._check_api_limit():
            self.logger.error("API limit reached, skipping collection")
            return []

        # Use today's date if not specified
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d")
        if not end_date:
            end_date = start_date

        # Check cache first
        cached_articles = self._get_cached_articles(start_date)
        if cached_articles:
            self.logger.info(f"Using cached articles for {start_date}")
            return cached_articles

        self.logger.info(f"News collection: {start_date} to {end_date}")
        
        # Adjust start date to noon to avoid timezone issues
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        start_dt = start_dt.replace(hour=12, minute=0, second=0)
        self.logger.info(f"Adjusted start date: {start_dt.isoformat()}")

        all_articles = []
        page = 0
        last_progress_update = 0
        articles_since_last_update = 0

        while True:
            if not self._check_api_limit():
                break

            try:
                articles = self._fetch_page(start_date, page)
                if not articles:
                    break

                # Filter articles by date
                valid_articles = self._filter_by_date(articles, start_dt)
                all_articles.extend(valid_articles)
                articles_since_last_update += len(valid_articles)

                # Update progress every 2 seconds
                current_time = time.time()
                if current_time - last_progress_update >= 2:
                    self.logger.info(f"Progress: {len(all_articles)} articles (page {page + 1}/10)")
                    last_progress_update = current_time
                    articles_since_last_update = 0

                # Rate limiting between requests
                time.sleep(self.rate_limit_delay)

                # Check if we've reached the maximum page limit
                if page >= 9:  # 0-based index, so 9 is the 10th page
                    break

                page += 1

            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                break

        # Update cache
        if all_articles:
            self._update_cache(start_date, all_articles)

        self.logger.info(f"Successfully collected {len(all_articles)} headlines")
        return all_articles

    def _fetch_page(self, start_date, page):
        """Fetch a single page of articles with retry logic."""
        params = {
            "date": start_date,
            "limit": self.batch_size,
            "page": page
        }

        for attempt in range(self.max_retries):
            try:
                response = self._make_request(params)
                if not response:
                    return None
                return response.get("data", [])
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error(f"Failed to fetch page {page} after {self.max_retries} attempts: {str(e)}")
                    return None
        return None

    def _make_request(self, params):
        """Make API request with rate limiting and retry logic."""
        url = f"{UW_BASE_URL}{NEWS_ENDPOINT}"
        self.logger.info(f"Making request to {url}")
        
        try:
            response = requests.get(
                url,
                headers=DEFAULT_HEADERS,
                params=params,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None

    def _filter_by_date(self, articles, start_dt):
        """Filter articles by date."""
        filtered = []
        for article in articles:
            try:
                article_date = datetime.fromisoformat(article.get("published_at", "").replace("Z", "+00:00"))
                if article_date >= start_dt:
                    filtered.append(article)
            except (ValueError, TypeError):
                continue
        return filtered

    def process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process the fetched news data."""
        processed_articles = []
        for article in data:
            try:
                # Convert sentiment string to numeric value
                sentiment_str = article.get('sentiment', 'neutral')
                sentiment_value = {
                    'neutral': 0.0,
                    'positive': 1.0,
                    'negative': -1.0
                }.get(sentiment_str, 0.0)  # Default to neutral if unknown

                # Convert impact_score to numeric value
                impact_str = article.get('impact_score', 'low')
                impact_value = {
                    'high': 10.0,
                    'medium': 5.0,
                    'low': 1.0
                }.get(impact_str, 1.0)  # Default to low if unknown

                processed_article = {
                    'headline': article.get('headline'),
                    'content': article.get('meta', {}).get('additional_info', ''),
                    'url': None,  # API doesn't provide URLs
                    'published_at': article.get('created_at'),
                    'source': article.get('source'),
                    'symbols': article.get('tickers', []),
                    'collected_at': datetime.utcnow().isoformat(),
                    'sentiment': sentiment_value,
                    'impact_score': impact_value,
                }
                processed_articles.append(processed_article)
            except Exception as e:
                logger.error(f"Error processing article: {str(e)}")
                continue
        return processed_articles

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save the processed news data to the database using batch inserts."""
        if not data:
            return
            
        try:
            with self.get_db_connection() as conn:
                # Prepare batch insert
                values = []
                for article in data:
                    values.append({
                        'headline': article.get('headline'),
                        'content': article.get('content'),
                        'url': article.get('url'),
                        'published_at': article.get('published_at'),
                        'source': article.get('source'),
                        'symbols': article.get('symbols'),
                        'sentiment': article.get('sentiment'),
                        'impact_score': article.get('impact_score'),
                        'collected_at': article.get('collected_at')
                    })
                
                # Execute batch insert
                conn.execute(text("""
                    INSERT INTO trading.news_headlines (
                        headline, content, url, published_at, 
                        source, symbols, sentiment, impact_score, collected_at
                    ) VALUES (
                        %(headline)s, %(content)s, %(url)s, %(published_at)s,
                        %(source)s, %(symbols)s, %(sentiment)s, %(impact_score)s, %(collected_at)s
                    )
                """), values)
                conn.commit()
                logger.info(f"Saved {len(values)} articles to database")
                
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise 