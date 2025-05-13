import logging
from datetime import datetime
from typing import List, Dict, Any

from collectors.base_collector import BaseCollector
from collectors.config.news_api_config import (
    NEWS_ENDPOINT,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT
)

logger = logging.getLogger(__name__)

class NewsCollector(BaseCollector):
    """Collector for news articles."""
    
    def __init__(self):
        super().__init__()
        self.endpoint = NEWS_ENDPOINT
        self.headers = DEFAULT_HEADERS
        self.timeout = REQUEST_TIMEOUT

    def fetch_data(self) -> List[Dict[str, Any]]:
        """Fetch news articles from the API."""
        try:
            response = self.session.get(
                self.endpoint,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching news data: {str(e)}")
            raise

    def process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process the fetched news data."""
        processed_articles = []
        for article in data:
            try:
                processed_article = {
                    'title': article.get('title'),
                    'content': article.get('content'),
                    'url': article.get('url'),
                    'published_at': article.get('published_at'),
                    'source': article.get('source'),
                    'symbols': article.get('symbols', []),
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
                with conn.cursor() as cur:
                    for article in data:
                        cur.execute("""
                            INSERT INTO news_articles (
                                title, content, url, published_at, 
                                source, symbols, collected_at
                            ) VALUES (
                                %(title)s, %(content)s, %(url)s, %(published_at)s,
                                %(source)s, %(symbols)s, %(collected_at)s
                            )
                        """, article)
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving news data: {str(e)}")
            raise 