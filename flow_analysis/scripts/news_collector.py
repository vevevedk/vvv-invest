#!/usr/bin/env python3

"""
News Headlines Collector
Collects and processes news headlines from Unusual Whales API
"""

import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import pytz
from typing import Optional, Dict, List, Any
import psycopg2
from psycopg2.extras import execute_values, Json
import requests
import argparse
import json
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import backoff
from tenacity import retry, stop_after_attempt, wait_exponential
import re

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.api_config import (
    UW_API_TOKEN, UW_BASE_URL, NEWS_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_RATE_LIMIT
)
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME
from flow_analysis.config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS
from flow_analysis.scripts.monitoring import MetricsCollector, HealthChecker, create_monitoring_tables
from flow_analysis.scripts.data_validation import DataValidator, create_validation_tables

# Constants
BATCH_SIZE = 100  # Number of records to insert at once
COLLECTION_INTERVAL = 300  # 5 minutes in seconds
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 300  # 5 minutes

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
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NewsCollector:
    """Collect and process news headlines from the UW API."""
    
    def __init__(self):
        """Initialize the collector."""
        self.base_url = UW_BASE_URL
        self.eastern = pytz.timezone('US/Eastern')
        self.db_conn = None
        self.engine = self._create_engine()
        self.connect_db()
        
        # Initialize monitoring and validation
        self.metrics_collector = MetricsCollector(DB_CONFIG, UW_API_TOKEN)
        self.health_checker = HealthChecker(DB_CONFIG, UW_API_TOKEN)
        self.data_validator = DataValidator(DB_CONFIG)
        
        # Create necessary tables
        create_monitoring_tables(DB_CONFIG)
        create_validation_tables(DB_CONFIG)
        self._create_news_table()
        
        # Initialize duplicate detection
        self.seen_headlines = set()
        self._load_seen_headlines()
        
    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        
    @contextmanager
    def get_db_connection(self):
        """Get database connection with automatic reconnection"""
        try:
            if self.db_conn is None or self.db_conn.closed:
                self.connect_db()
            yield self.db_conn
        except psycopg2.OperationalError:
            self.connect_db()
            yield self.db_conn
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def connect_db(self):
        """Establish database connection with retry logic"""
        @backoff.on_exception(
            backoff.expo,
            (psycopg2.OperationalError, psycopg2.InterfaceError),
            max_tries=MAX_RETRIES,
            max_time=300
        )
        def _connect():
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            logger.info("Successfully connected to database")
            
        try:
            _connect()
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=INITIAL_BACKOFF, max=MAX_BACKOFF)
    )
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an API request with retry logic."""
        try:
            time.sleep(REQUEST_RATE_LIMIT)  # Rate limiting
            response = requests.get(
                endpoint,
                headers=DEFAULT_HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during request: {str(e)}")
            raise
                     
    def _load_seen_headlines(self):
        """Load previously seen headlines from database"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT headline, published_at
                        FROM {SCHEMA_NAME}.news_headlines
                        WHERE collected_at >= NOW() - INTERVAL '24 hours'
                    """)
                    for row in cur.fetchall():
                        self.seen_headlines.add((row[0], row[1]))
        except Exception as e:
            logger.error(f"Error loading seen headlines: {str(e)}")
            
    def _process_news(self, news_data: List[Dict]) -> pd.DataFrame:
        """Process news data into a DataFrame"""
        processed_news = []
        
        for item in news_data:
            try:
                # Skip if we've seen this headline
                headline = item.get('headline')
                published_at = item.get('published_at') or item.get('created_at')  # Handle both formats
                
                if not headline or not published_at:
                    logger.warning(f"Invalid news item: {list(item.keys())}")
                    continue
                    
                if (headline, published_at) in self.seen_headlines:
                    continue
                    
                # Extract symbols from tickers
                symbols = item.get('tickers', [])
                
                # Skip if no symbols match our watchlist
                if not any(symbol in SYMBOLS for symbol in symbols):
                    continue
                
                # Process the news item
                processed_news.append({
                    'headline': headline,
                    'published_at': published_at,
                    'source': item.get('source', 'Unknown'),
                    'url': item.get('url', ''),
                    'symbols': symbols,
                    'sentiment': item.get('sentiment', 0.0),
                    'impact_score': item.get('impact_score', 0.0)
                })
                
                # Add to seen headlines
                self.seen_headlines.add((headline, published_at))
                
            except Exception as e:
                logger.warning(f"Error processing news item: {str(e)}")
                continue
                
        return pd.DataFrame(processed_news)
        
    def collect_news(self) -> pd.DataFrame:
        """Collect the latest news headlines from UW API"""
        endpoint = f"{self.base_url}{NEWS_ENDPOINT}"
        
        params = {
            "limit": 100,  # Maximum allowed per request
            "symbols": ','.join(SYMBOLS)  # Filter by our watchlist
        }
        
        data = self._make_request(endpoint, params)
        if data and "data" in data:
            # Log a sample of the data
            logger.info(f"Sample news item: {json.dumps(data['data'][0], indent=2)}")
            return self._process_news(data["data"])
        return pd.DataFrame()
        
    def save_news_to_db(self, news: pd.DataFrame) -> None:
        """Save news data to the database"""
        if news.empty:
            logger.warning("No news data to save")
            return
            
        try:
            # Create table if it doesn't exist using SQLAlchemy
            with self.engine.connect() as conn:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS trading.news_headlines (
                    id SERIAL PRIMARY KEY,
                    headline TEXT NOT NULL,
                    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    source TEXT NOT NULL,
                    url TEXT NOT NULL,
                    symbols TEXT[] NOT NULL,
                    sentiment FLOAT NOT NULL,
                    impact_score FLOAT NOT NULL,
                    collected_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                
            # Prepare data for insertion
            records = []
            for _, row in news.iterrows():
                try:
                    record = {
                        'headline': row['headline'],
                        'published_at': row['published_at'],
                        'source': row['source'],
                        'url': row['url'],
                        'symbols': row['symbols'],
                        'sentiment': float(row['sentiment']),
                        'impact_score': float(row['impact_score']),
                        'collected_at': datetime.now(pytz.UTC)
                    }
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Error preparing record: {str(e)}")
                    continue
                    
            if records:
                # Insert records using raw psycopg2 connection
                raw_conn = self.engine.raw_connection()
                try:
                    with raw_conn.cursor() as cur:
                        insert_sql = """
                        INSERT INTO trading.news_headlines (
                            headline, published_at, source, url,
                            symbols, sentiment, impact_score, collected_at
                        ) VALUES %s
                        """
                        # Convert records to tuples in the correct order
                        values = [(
                            record['headline'],
                            record['published_at'],
                            record['source'],
                            record['url'],
                            record['symbols'],
                            record['sentiment'],
                            record['impact_score'],
                            record['collected_at']
                        ) for record in records]
                        
                        # Use execute_values for bulk insert
                        execute_values(cur, insert_sql, values)
                        raw_conn.commit()
                        logger.info(f"Saved {len(records)} news items to database")
                finally:
                    raw_conn.close()
                    
        except Exception as e:
            logger.error(f"Error saving news to database: {str(e)}")
            raise
            
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        # Get current time in Eastern timezone
        now = datetime.now(pytz.timezone('US/Eastern'))
        current_time = now.time()
        
        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return False
            
        # Check if current time is between market open and close
        return MARKET_OPEN <= current_time <= MARKET_CLOSE
        
    def run(self):
        """Run a single collection cycle"""
        try:
            logger.info("Starting news collection...")
            
            # Check system health
            health_status = self.health_checker.check_health()
            if not health_status.is_healthy:
                logger.error(f"System health check failed: {health_status.errors}")
                return
                
            # Collect system metrics
            metrics = self.metrics_collector.collect_metrics()
            self.metrics_collector.save_metrics(metrics)
            
            # Collect news
            news = self.collect_news()
            if not news.empty:
                self.save_news_to_db(news)
                logger.info(f"Successfully collected and saved {len(news)} news items")
            else:
                logger.info("No new news items to collect")
                
        except Exception as e:
            logger.error(f"Error in collection cycle: {str(e)}")
            raise

    def __del__(self):
        """Clean up resources"""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()

    def _create_news_table(self):
        """Create the news_headlines table if it doesn't exist"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.news_headlines (
                            id SERIAL PRIMARY KEY,
                            headline TEXT NOT NULL,
                            published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            source TEXT NOT NULL,
                            url TEXT NOT NULL,
                            symbols TEXT[] NOT NULL,
                            sentiment FLOAT NOT NULL,
                            impact_score FLOAT NOT NULL,
                            collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    conn.commit()
        except Exception as e:
            logger.error(f"Error creating news_headlines table: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='News Headlines Collector')
    parser.add_argument('--continuous', action='store_true', help='Run continuously during market hours')
    parser.add_argument('--historical', action='store_true', help='Fetch historical data')
    args = parser.parse_args()
    
    collector = NewsCollector()
    
    if args.historical:
        # Override collect_news method temporarily for historical data
        def historical_collect():
            endpoint = f"{collector.base_url}{NEWS_ENDPOINT}"
            params = {
                "limit": 100,
                "symbols": ','.join(SYMBOLS),
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
    elif args.continuous:
        logger.info("Starting continuous collection mode")
        while True:
            collector.run()
            time.sleep(COLLECTION_INTERVAL)
    else:
        # Single collection cycle
        collector.run()

if __name__ == '__main__':
    main() 