"""
Test script for news collector
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta
import pytz
import json
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import Engine
import re

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flow_analysis.scripts.news_collector import NewsCollector
from flow_analysis.config.api_config import UW_API_TOKEN, UW_BASE_URL, NEWS_ENDPOINT
from flow_analysis.config.db_config import DB_CONFIG, SCHEMA_NAME
from flow_analysis.config.watchlist import MARKET_OPEN, MARKET_CLOSE, SYMBOLS

class TestNewsCollector(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        # Patch the time.sleep to avoid delays
        self.sleep_patcher = patch('time.sleep', return_value=None)
        self.sleep_patcher.start()
        
        # Create test instance
        self.collector = NewsCollector()
        
        # Sample news data
        fixed_time = "2024-01-01T10:00:00Z"  # Use a fixed timestamp
        self.sample_news = {
            "data": [
                {
                    "headline": "AAPL stock surges after strong earnings beat",
                    "published_at": fixed_time,
                    "source": "CNBC",
                    "url": "https://example.com/news/1",
                    "symbols": ["AAPL"]
                },
                {
                    "headline": "TSLA shares drop on production concerns",
                    "published_at": fixed_time,
                    "source": "Bloomberg",
                    "url": "https://example.com/news/2",
                    "symbols": ["TSLA"]
                }
            ]
        }
        
        # Mock database connection
        self.db_patcher = patch('psycopg2.connect')
        self.mock_db = self.db_patcher.start()
        self.mock_conn = MagicMock()
        self.mock_db.return_value = self.mock_conn
        
        # Mock cursor
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_conn.__enter__.return_value = self.mock_conn
        self.mock_cursor.__enter__.return_value = self.mock_cursor

    def tearDown(self):
        """Clean up after tests"""
        self.sleep_patcher.stop()
        self.db_patcher.stop()

    @patch('requests.get')
    def test_make_request(self, mock_get):
        """Test API request handling"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = self.sample_news
        mock_get.return_value = mock_response
        
        result = self.collector._make_request(NEWS_ENDPOINT)
        
        self.assertEqual(result, self.sample_news)
        mock_get.assert_called_once()
        
        # Test rate limiting
        mock_get.reset_mock()
        self.collector._make_request(NEWS_ENDPOINT)
        self.collector._make_request(NEWS_ENDPOINT)
        self.assertEqual(mock_get.call_count, 2)

    def test_analyze_sentiment(self):
        """Test sentiment analysis"""
        # Test positive sentiment
        positive_headline = "AAPL stock surges after strong earnings beat"
        sentiment = self.collector._analyze_sentiment(positive_headline)
        self.assertGreater(sentiment, 0)
        
        # Test negative sentiment
        negative_headline = "TSLA shares drop on production concerns"
        sentiment = self.collector._analyze_sentiment(negative_headline)
        self.assertLess(sentiment, 0)
        
        # Test neutral sentiment
        neutral_headline = "Market update: Regular trading day"
        sentiment = self.collector._analyze_sentiment(neutral_headline)
        self.assertEqual(sentiment, 0.0)

    def test_calculate_impact_score(self):
        """Test impact score calculation"""
        # Test high impact headline
        headline = "BREAKING: AAPL announces revolutionary new product"
        sentiment = self.collector._analyze_sentiment(headline)
        impact_score = self.collector._calculate_impact_score(headline, sentiment)
        self.assertGreater(impact_score, 0)
        
        # Test low impact headline
        headline = "Regular market update"
        sentiment = self.collector._analyze_sentiment(headline)
        impact_score = self.collector._calculate_impact_score(headline, sentiment)
        self.assertLess(impact_score, 3.0)

    def test_extract_symbols(self):
        """Test symbol extraction from headlines"""
        # Test single symbol
        headline = "AAPL stock surges after strong earnings"
        symbols = self.collector._extract_symbols(headline)
        self.assertEqual(symbols, ["AAPL"])
        
        # Test multiple symbols
        headline = "AAPL and MSFT announce partnership"
        symbols = self.collector._extract_symbols(headline)
        self.assertEqual(set(symbols), {"AAPL", "MSFT"})
        
        # Test no symbols
        headline = "Market update: Regular trading day"
        symbols = self.collector._extract_symbols(headline)
        self.assertEqual(symbols, [])

    @patch('requests.get')
    def test_collect_news(self, mock_get):
        """Test news collection"""
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.sample_news
        mock_get.return_value = mock_response
        
        # Mock database query for seen headlines
        self.mock_cursor.fetchall.return_value = []
        
        news_df = self.collector.collect_news()
        
        self.assertIsInstance(news_df, pd.DataFrame)
        self.assertEqual(len(news_df), 2)
        self.assertIn('headline', news_df.columns)
        self.assertIn('sentiment', news_df.columns)
        self.assertIn('impact_score', news_df.columns)

    def test_save_news_to_db(self):
        """Test saving news to database"""
        # Create test data
        news_data = pd.DataFrame({
            'headline': ['Test headline'],
            'published_at': [datetime.now(pytz.UTC)],
            'source': ['Test Source'],
            'url': ['https://example.com'],
            'symbols': [['AAPL']],
            'sentiment': [0.5],
            'impact_score': [3.0]
        })

        # Mock database connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        mock_conn.connection.encoding = 'UTF8'  # Use correct encoding name
        mock_conn.mogrify.return_value = b'(test)'  # Mock mogrify to return bytes

        # Mock execute_values
        mock_execute_values = MagicMock()
        mock_execute_values.return_value = None

        # Mock the engine's connect method
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock the raw connection to handle UUID registration
        mock_raw_conn = MagicMock()
        mock_raw_conn.info.server_version = 90600  # PostgreSQL 9.6
        mock_engine.raw_connection.return_value = mock_raw_conn

        # Mock the pool
        mock_pool = MagicMock()
        mock_pool.connect.return_value = mock_raw_conn
        mock_engine.pool = mock_pool

        # Mock the hstore OIDs
        mock_hstore_oids = (1111, 2222)  # (oid, array_oid)

        # Mock the engine creation
        with patch('sqlalchemy.create_engine', return_value=mock_engine):
            with patch('psycopg2.extras.register_uuid'):
                with patch('psycopg2.extras.HstoreAdapter.get_oids', return_value=mock_hstore_oids):
                    with patch('psycopg2.extras.register_hstore'):
                        with patch('flow_analysis.scripts.news_collector.execute_values', mock_execute_values):
                            # Replace the existing collector's engine with our mock
                            original_engine = self.collector.engine
                            self.collector.engine = mock_engine
                            try:
                                with self.engine.raw_connection() as raw_conn:
                                    with raw_conn.cursor() as cur:
                                        # ... create table if needed ...
                                        execute_values(cur, insert_sql, values)
                                        raw_conn.commit()
                            finally:
                                # Restore original engine
                                self.collector.engine = original_engine

    def test_is_market_open(self):
        """Test market hours checking"""
        # Mock current time during market hours (Wednesday, April 5, 2023)
        mock_now = datetime(2023, 4, 5, 10, 0, tzinfo=pytz.timezone('US/Eastern'))
        with patch('flow_analysis.scripts.news_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_now
            self.assertTrue(self.collector.is_market_open())

        # Test outside market hours
        mock_now = datetime(2023, 4, 5, 20, 0, tzinfo=pytz.timezone('US/Eastern'))
        with patch('flow_analysis.scripts.news_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_now
            self.assertFalse(self.collector.is_market_open())

    def test_duplicate_detection(self):
        """Test duplicate headline detection"""
        # Add a headline to seen set
        headline = "Test headline"
        published_at = datetime.now()
        self.collector.seen_headlines.add((headline, published_at))
        
        # Test duplicate detection
        self.assertTrue((headline, published_at) in self.collector.seen_headlines)
        
        # Test new headline
        new_headline = "New headline"
        self.assertFalse((new_headline, published_at) in self.collector.seen_headlines)

if __name__ == '__main__':
    unittest.main() 