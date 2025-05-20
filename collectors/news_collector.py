import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import requests
import psycopg2
from psycopg2.extras import DictCursor
import os
from dotenv import load_dotenv
import time

# Load environment variables
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

from collectors.base_collector import BaseCollector
from flow_analysis.config.api_config import (
    UW_API_TOKEN,
    NEWS_ENDPOINT,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT
)

logger = logging.getLogger(__name__)

class NewsCollector(BaseCollector):
    """Collector for news articles with pagination and date range support."""
    
    def __init__(self):
        super().__init__()
        self.endpoint = NEWS_ENDPOINT
        self.headers = DEFAULT_HEADERS
        self.timeout = REQUEST_TIMEOUT
        self.session = requests.Session()

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

    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch all news articles from the API within the date range [start_date, end_date].
        If no dates are provided, defaults to current UTC day.
        Implements rate limiting and retry logic for 429 errors.
        """
        all_articles = []
        limit = 100  # Use max allowed by API
        page = 0
        max_retries = 5
        base_delay = 1  # seconds
        # Parse date range
        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            start_dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)  # inclusive
        else:
            end_dt = start_dt + timedelta(days=1)
        logger.info(f"Fetching news from {start_dt} to {end_dt}")
        while True:
            params = {"limit": limit, "page": page}
            logger.info(f"Fetching page {page} with params: {params}")
            retries = 0
            while retries <= max_retries:
                try:
                    logger.debug(f"Requesting page {page}, attempt {retries+1}")
                    response = self.session.get(
                        self.endpoint,
                        headers=self.headers,
                        params=params,
                        timeout=self.timeout
                    )
                    if response.status_code == 429:
                        delay = base_delay * (2 ** retries)
                        logger.warning(f"429 Too Many Requests. Sleeping for {delay} seconds (retry {retries+1}/{max_retries})...")
                        time.sleep(delay)
                        retries += 1
                        continue
                    response.raise_for_status()
                    logger.info(f"Successfully fetched page {page} on attempt {retries+1}")
                    break  # Success, exit retry loop
                except requests.RequestException as e:
                    logger.error(f"Error fetching news data (page {page}, attempt {retries+1}): {str(e)}")
                    delay = base_delay * (2 ** retries)
                    logger.info(f"Sleeping for {delay} seconds before retrying...")
                    time.sleep(delay)
                    retries += 1
            else:
                logger.error(f"Max retries exceeded for page {page}. Skipping.")
                break
            data = response.json().get('data', [])
            logger.info(f"Fetched {len(data)} articles from page {page}")
            # Log the date range of this page
            created_dates = []
            for article in data:
                created_at = article.get('created_at')
                if created_at:
                    try:
                        created_dates.append(datetime.fromisoformat(created_at.replace('Z', '+00:00')))
                    except Exception:
                        continue
            if created_dates:
                min_date = min(created_dates)
                max_date = max(created_dates)
                logger.info(f"Page {page} article date range: {min_date} to {max_date}")
            else:
                logger.info(f"Page {page} has no valid created_at dates.")
            if not data:
                logger.info(f"No more data at page {page}")
                break
            # Filter by date range
            filtered = []
            for article in data:
                created_at = article.get('created_at')
                if not created_at:
                    continue
                try:
                    created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except Exception:
                    continue
                if start_dt <= created_dt < end_dt:
                    filtered.append(article)
            all_articles.extend(filtered)
            logger.info(f"Page {page}: {len(filtered)} articles in range, {len(data)} total")
            # Improved stopping condition:
            if created_dates:
                if all(dt < start_dt for dt in created_dates):
                    logger.info(f"All articles on page {page} are older than start date. Stopping.")
                    break
                if all(dt >= end_dt for dt in created_dates):
                    logger.info(f"All articles on page {page} are newer than end date. Stopping.")
                    break
            if len(data) < limit:
                logger.info(f"Last page reached at page {page}")
                break
            page += 1
            logger.info(f"Sleeping for {base_delay} seconds before next page...")
            time.sleep(base_delay)  # Rate limiting between requests
        logger.info(f"Total articles fetched: {len(all_articles)}")
        return all_articles

    def process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process the fetched news data."""
        processed_articles = []
        for article in data:
            try:
                processed_article = {
                    'headline': article.get('headline'),
                    'content': article.get('meta', {}).get('additional_info', ''),
                    'url': None,  # API doesn't provide URLs
                    'published_at': article.get('created_at'),
                    'source': article.get('source'),
                    'symbols': article.get('tickers', []),
                    'collected_at': datetime.utcnow().isoformat(),
                    'sentiment': article.get('sentiment'),
                    'impact_score': article.get('impact_score'),
                }
                processed_articles.append(processed_article)
            except Exception as e:
                logger.error(f"Error processing article: {str(e)}")
                continue
        return processed_articles

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save the processed news data to the database."""
        try:
            with self.get_db_connection() as conn:
                print("DB connection params:", conn.get_dsn_parameters())
                with conn.cursor() as cur:
                    # Diagnostic: check for 'content' column
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'trading' AND table_name = 'news_headlines';
                    """)
                    columns = [row[0] for row in cur.fetchall()]
                    print("Columns in trading.news_headlines:", columns)
                    if 'content' not in columns:
                        print("ERROR: 'content' column not found in trading.news_headlines!")
                    for article in data:
                        cur.execute("""
                            INSERT INTO trading.news_headlines (
                                headline, content, url, published_at, 
                                source, symbols, sentiment, impact_score, collected_at
                            ) VALUES (
                                %(headline)s, %(content)s, %(url)s, %(published_at)s,
                                %(source)s, %(symbols)s, %(sentiment)s, %(impact_score)s, %(collected_at)s
                            )
                        """, {
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
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving news data: {str(e)}")
            raise

    def collect(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Fetch, process, and save news data for a date range.
        If no dates are provided, defaults to current UTC day.
        """
        data = self.fetch_data(start_date=start_date, end_date=end_date)
        processed_data = self.process_data(data)
        self.save_data(processed_data) 