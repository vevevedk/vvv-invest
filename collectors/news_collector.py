import logging
from datetime import datetime
from typing import List, Dict, Any
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
    """Collector for news articles."""
    
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

    def fetch_data(self) -> List[Dict[str, Any]]:
        """Fetch news articles from the API."""
        try:
            response = self.session.get(
                self.endpoint,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            response_data = response.json()
            return response_data.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching news data: {str(e)}")
            raise

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
                    'collected_at': datetime.utcnow().isoformat()
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

    def collect(self):
        """Override collect method to fetch, process, and save news data."""
        data = self.fetch_data()
        processed_data = self.process_data(data)
        self.save_data(processed_data) 