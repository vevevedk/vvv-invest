import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import requests
import psycopg2
from psycopg2.extras import DictCursor
import os
from dotenv import load_dotenv

from collectors.base_collector import BaseCollector
from collectors.config.news_api_config import (
    UW_API_TOKEN,
    NEWS_ENDPOINT,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT
)

# Load environment variables (including from .env.prod if present)
load_dotenv(dotenv_path=os.getenv('ENV_FILE', '.env.prod'))
print("Loaded environment file:", os.getenv('ENV_FILE', '.env.prod'))

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
        """
        all_articles = []
        limit = 100
        page = 0
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
            try:
                response = self.session.get(
                    self.endpoint,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json().get('data', [])
            except Exception as e:
                logger.error(f"Error fetching news data (page {page}): {str(e)}")
                break
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
            # Stop if all articles on this page are older than start_dt
            if all(datetime.fromisoformat(a.get('created_at', '').replace('Z', '+00:00')) < start_dt for a in data if a.get('created_at')):
                logger.info(f"All articles on page {page} are older than start date. Stopping.")
                break
            if len(data) < limit:
                logger.info(f"Last page reached at page {page}")
                break
            page += 1
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