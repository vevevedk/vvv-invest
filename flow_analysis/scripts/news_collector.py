#!/usr/bin/env python3

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

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, NEWS_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from config.db_config import DB_CONFIG, SCHEMA_NAME
from config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS

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
        
        # Initialize duplicate detection
        self.seen_headlines = set()
        self._load_seen_headlines()
        
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
                    
    def _load_seen_headlines(self):
        """Load previously seen headlines from database"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT headline, published_at
                    FROM {SCHEMA_NAME}.news_headlines
                    WHERE collected_at >= NOW() - INTERVAL '24 hours'
                """)
                for row in cur.fetchall():
                    self.seen_headlines.add((row[0], row[1]))
        except Exception as e:
            logger.error(f"Error loading seen headlines: {str(e)}")
            
    def _analyze_sentiment(self, headline: str) -> float:
        """Analyze sentiment of a news headline"""
        # Simple sentiment analysis based on keywords
        positive_words = {'up', 'rise', 'gain', 'bullish', 'positive', 'strong', 'beat', 'surge'}
        negative_words = {'down', 'fall', 'drop', 'bearish', 'negative', 'weak', 'miss', 'plunge'}
        
        words = set(headline.lower().split())
        positive_count = len(words & positive_words)
        negative_count = len(words & negative_words)
        
        if positive_count + negative_count == 0:
            return 0.0
            
        return (positive_count - negative_count) / (positive_count + negative_count)
        
    def _calculate_impact_score(self, headline: str, sentiment: float) -> int:
        """Calculate impact score for a news headline"""
        # Base score from sentiment
        base_score = abs(sentiment) * 5
        
        # Keywords that indicate high impact
        high_impact_words = {
            'earnings', 'guidance', 'upgrade', 'downgrade', 'initiate',
            'target', 'price', 'rating', 'analyst', 'report'
        }
        
        words = set(headline.lower().split())
        impact_multiplier = 1 + (len(words & high_impact_words) * 0.5)
        
        return int(base_score * impact_multiplier)
        
    def _process_news(self, news_data: List[Dict]) -> pd.DataFrame:
        """Process raw news data into a DataFrame"""
        if not news_data:
            logger.warning("No news data received from API")
            return pd.DataFrame()
            
        logger.info(f"Received {len(news_data)} news items from API")
        
        # Convert to DataFrame
        news = pd.DataFrame(news_data)
        
        # Add collection timestamp
        news['collected_at'] = datetime.now(self.eastern)
        
        # Ensure required columns exist
        required_columns = [
            'headline', 'source', 'published_at', 'symbols',
            'sentiment', 'impact_score', 'collected_at'
        ]
        
        # Create missing columns if they don't exist
        for col in required_columns:
            if col not in news.columns:
                news[col] = None
                
        # Analyze sentiment and impact for each headline
        for idx, row in news.iterrows():
            if pd.isna(row['sentiment']):
                news.at[idx, 'sentiment'] = self._analyze_sentiment(row['headline'])
            if pd.isna(row['impact_score']):
                news.at[idx, 'impact_score'] = self._calculate_impact_score(
                    row['headline'],
                    news.at[idx, 'sentiment']
                )
                
        # Filter out duplicates
        before_dedup = len(news)
        news = news[~news.apply(
            lambda x: (x['headline'], x['published_at']) in self.seen_headlines,
            axis=1
        )]
        
        # Update seen headlines
        for _, row in news.iterrows():
            self.seen_headlines.add((row['headline'], row['published_at']))
            
        logger.info(f"Removed {before_dedup - len(news)} duplicate news items")
        return news[required_columns]
        
    def collect_news(self) -> pd.DataFrame:
        """Collect the latest news headlines"""
        endpoint = f"{self.base_url}{NEWS_ENDPOINT}"
        
        params = {
            "limit": 100,  # Maximum allowed per request
            "symbols": SYMBOLS  # Filter by our watchlist
        }
        
        data = self._make_request(endpoint, params)
        if data and "data" in data:
            return self._process_news(data["data"])
        return pd.DataFrame()
        
    def save_news_to_db(self, news: pd.DataFrame) -> None:
        """Save news headlines to PostgreSQL database"""
        if news.empty:
            logger.warning("No news to save - DataFrame is empty")
            return
            
        try:
            # Ensure database connection is active
            if self.db_conn.closed:
                logger.info("Database connection closed, reconnecting...")
                self.connect_db()
                
            # Start a fresh transaction
            self.db_conn.rollback()
                
            with self.db_conn.cursor() as cur:
                # Prepare data for insertion
                records = news.to_dict('records')
                logger.info(f"Preparing to insert {len(records)} news items")
                
                # Create the insert query
                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.news_headlines (
                        headline, source, published_at, symbols,
                        sentiment, impact_score, collected_at
                    ) VALUES %s
                    ON CONFLICT (headline, published_at) DO NOTHING
                """
                
                # Prepare values for insertion
                values = []
                for record in records:
                    try:
                        value = (
                            record.get('headline'),
                            record.get('source'),
                            record.get('published_at'),
                            record.get('symbols'),
                            record.get('sentiment'),
                            record.get('impact_score'),
                            record.get('collected_at')
                        )
                        values.append(value)
                    except Exception as e:
                        logger.error(f"Error preparing record for insertion: {str(e)}")
                        logger.error(f"Problematic record: {record}")
                        continue
                
                if not values:
                    logger.error("No valid values prepared for insertion")
                    return
                
                try:
                    logger.info(f"Executing insert query with {len(values)} values")
                    # Execute the insert
                    execute_values(cur, insert_query, values)
                    self.db_conn.commit()
                    
                    # Verify the insertion
                    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.news_headlines")
                    total_count = cur.fetchone()[0]
                    logger.info(f"Total news items in database after insertion: {total_count}")
                except psycopg2.Error as e:
                    logger.error(f"Database error during insertion: {str(e)}")
                    self.db_conn.rollback()
                    raise
                
        except Exception as e:
            logger.error(f"Error saving news to database: {str(e)}")
            if not isinstance(e, psycopg2.Error):
                logger.error(f"Error details: {str(e)}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        current_time = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if current_time.weekday() >= 5:
            return False
            
        # Parse market hours
        market_open = current_time.replace(
            hour=int(MARKET_OPEN.split(':')[0]),
            minute=int(MARKET_OPEN.split(':')[1]),
            second=0,
            microsecond=0
        )
        market_close = current_time.replace(
            hour=int(MARKET_CLOSE.split(':')[0]),
            minute=int(MARKET_CLOSE.split(':')[1]),
            second=0,
            microsecond=0
        )
        
        return market_open <= current_time <= market_close
        
    def run(self):
        """Run one collection cycle"""
        logger.info("Starting news collection...")
        start_time = time.time()
        
        news = self.collect_news()
        if not news.empty:
            self.save_news_to_db(news)
            logger.info(f"Collected and saved {len(news)} news items")
        else:
            logger.info("No new news items collected")
        
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(f"Collection cycle completed in {duration_ms}ms")
        
    def __del__(self):
        """Clean up database connection"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

def main():
    parser = argparse.ArgumentParser(description='News Headlines Collector')
    parser.add_argument('--historical', action='store_true', help='Fetch historical data')
    args = parser.parse_args()
    
    collector = NewsCollector()
    
    if args.historical:
        # Override collect_news method temporarily for historical data
        def historical_collect():
            endpoint = f"{collector.base_url}{NEWS_ENDPOINT}"
            params = {
                "limit": 100,
                "symbols": SYMBOLS,
                "start_date": "2025-04-17",  # Example historical date
                "end_date": "2025-04-17"
            }
            logger.info("Fetching historical news data")
            data = collector._make_request(endpoint, params)
            if data and "data" in data:
                return collector._process_news(data["data"])
            return pd.DataFrame()
        
        # Collect and save historical data once
        news = historical_collect()
        if not news.empty:
            collector.save_news_to_db(news)
            logger.info("Historical data collection complete")
        else:
            logger.warning("No historical news items collected")
    else:
        # Single collection cycle
        collector.run()

if __name__ == "__main__":
    main() 