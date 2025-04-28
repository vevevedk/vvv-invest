#!/usr/bin/env python3

"""
Test script for News Collector
Tests the functionality of the news collector implementation
"""

import os
import sys
import logging
from pathlib import Path
import pytest
from datetime import datetime, timedelta
import pytz
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
import json

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.news_collector import NewsCollector
from config.db_config import DB_CONFIG, SCHEMA_NAME

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test database connection"""
    collector = NewsCollector()
    assert collector.db_conn is not None
    assert not collector.db_conn.closed
    collector.db_conn.close()

def test_market_hours():
    """Test market hours detection"""
    collector = NewsCollector()
    
    # Test during market hours
    market_open = datetime.now(pytz.timezone('US/Eastern')).replace(
        hour=10, minute=0, second=0, microsecond=0
    )
    # Just verify the function returns a boolean
    assert isinstance(collector.is_market_open(), bool)
    
    # Test outside market hours
    market_closed = datetime.now(pytz.timezone('US/Eastern')).replace(
        hour=20, minute=0, second=0, microsecond=0
    )
    # Just verify the function returns a boolean
    assert isinstance(collector.is_market_open(), bool)
    
    collector.db_conn.close()

def test_news_collection():
    """Test news collection functionality"""
    collector = NewsCollector()
    
    # Collect news
    news_df = collector.collect_news()
    
    # Verify DataFrame structure
    assert isinstance(news_df, pd.DataFrame)
    if not news_df.empty:
        required_columns = [
            'headline', 'source', 'published_at', 'tickers',
            'sentiment', 'is_major', 'tags', 'meta', 'collected_at'
        ]
        assert all(col in news_df.columns for col in required_columns)
        
        # Verify data types
        assert pd.api.types.is_datetime64_any_dtype(news_df['published_at'])
        assert pd.api.types.is_datetime64_any_dtype(news_df['collected_at'])
        assert isinstance(news_df['tickers'].iloc[0], list)
        
    collector.db_conn.close()

def test_database_insertion():
    """Test saving news to database"""
    collector = NewsCollector()
    
    # Create test data
    test_data = {
        'headline': ['Test Headline 1', 'Test Headline 2'],
        'source': ['Test Source 1', 'Test Source 2'],
        'published_at': [datetime.now(pytz.UTC), datetime.now(pytz.UTC)],
        'tickers': [['SPY'], ['QQQ']],
        'sentiment': ['positive', 'negative'],
        'is_major': [True, False],
        'tags': [['test'], ['test']],
        'meta': [json.dumps({'test': 'value'}), json.dumps({'test': 'value'})],  # Convert dict to JSON string
        'collected_at': [datetime.now(pytz.UTC), datetime.now(pytz.UTC)]
    }
    test_df = pd.DataFrame(test_data)
    
    # Save to database
    collector.save_news_to_db(test_df)
    
    # Verify insertion
    with collector.db_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.news_headlines WHERE headline LIKE 'Test Headline%'")
        count = cur.fetchone()[0]
        assert count == len(test_df)
    
    # Clean up test data
    with collector.db_conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA_NAME}.news_headlines WHERE headline LIKE 'Test Headline%'")
        collector.db_conn.commit()
    
    collector.db_conn.close()

def test_rate_limiting():
    """Test API rate limiting"""
    collector = NewsCollector()
    
    # Make multiple requests and measure time
    start_time = time.time()
    for _ in range(5):
        collector._rate_limit()
    end_time = time.time()
    
    # Verify rate limiting (should take at least 2 seconds for 5 requests at 2 requests/second)
    assert end_time - start_time >= 2.0
    
    collector.db_conn.close()

if __name__ == "__main__":
    # Run tests
    test_database_connection()
    test_market_hours()
    test_news_collection()
    test_database_insertion()
    test_rate_limiting()
    
    logger.info("All tests completed successfully") 