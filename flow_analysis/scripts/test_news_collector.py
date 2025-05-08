#!/usr/bin/env python3

"""
Test script for News Collector
Tests the functionality of the news collector implementation
"""

import os
import sys
import logging
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
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

class TestNewsCollector(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.collector = NewsCollector()
        self.sample_news = [
            {
                "headline": "Test News 1",
                "source": "Test Source",
                "published_at": "2025-04-17T10:00:00Z",
                "symbols": ["SPY", "QQQ"],
                "sentiment": 0.75,
                "impact_score": 5,
                "is_major": True,
                "tags": ["test"],
                "meta": {"test": "value"}
            },
            {
                "headline": "Test News 2",
                "source": "Test Source",
                "published_at": "2025-04-17T11:00:00Z",
                "symbols": ["SPY"],
                "sentiment": -0.25,
                "impact_score": 3,
                "is_major": False,
                "tags": ["test"],
                "meta": {"test": "value"}
            }
        ]

    def test_database_connection(self):
        """Test database connection"""
        assert self.collector.db_conn is not None
        assert not self.collector.db_conn.closed

    def test_market_hours(self):
        """Test market hours detection"""
        # Test during market hours
        market_open = datetime.now(pytz.timezone('US/Eastern')).replace(
            hour=10, minute=0, second=0, microsecond=0
        )
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = market_open
            self.assertTrue(self.collector.is_market_open())
        
        # Test outside market hours
        market_closed = datetime.now(pytz.timezone('US/Eastern')).replace(
            hour=20, minute=0, second=0, microsecond=0
        )
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = market_closed
            self.assertFalse(self.collector.is_market_open())
        
        # Test weekend
        weekend = datetime.now(pytz.timezone('US/Eastern')).replace(
            hour=10, minute=0, second=0, microsecond=0
        )
        weekend = weekend.replace(day=weekend.day + (5 - weekend.weekday()))
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = weekend
            self.assertFalse(self.collector.is_market_open())

    def test_news_collection(self):
        """Test news collection functionality"""
        with patch('requests.get') as mock_get:
            # Mock successful API response
            mock_response = MagicMock()
            mock_response.json.return_value = {"data": self.sample_news}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            # Test collection
            news = self.collector.collect_news()
            self.assertIsInstance(news, pd.DataFrame)
            self.assertEqual(len(news), 2)
            
            # Verify DataFrame structure
            required_columns = [
                'headline', 'source', 'published_at', 'symbols',
                'sentiment', 'impact_score', 'is_major', 'tags',
                'meta', 'collected_at'
            ]
            self.assertTrue(all(col in news.columns for col in required_columns))
            
            # Verify data types
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(news['published_at']))
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(news['collected_at']))
            self.assertIsInstance(news['symbols'].iloc[0], list)

    def test_database_operations(self):
        """Test database operations"""
        # Create test data
        test_df = pd.DataFrame(self.sample_news)
        test_df['collected_at'] = datetime.now(pytz.UTC)
        
        try:
            # Save to database
            self.collector.save_news_to_db(test_df)
            
            # Verify insertion
            with self.collector.db_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.news_headlines WHERE headline LIKE 'Test News%'")
                count = cur.fetchone()[0]
                self.assertEqual(count, len(test_df))
                
                # Verify data integrity
                cur.execute(f"""
                    SELECT headline, source, symbols, sentiment, impact_score, is_major, tags, meta
                    FROM {SCHEMA_NAME}.news_headlines
                    WHERE headline = 'Test News 2'
                """)
                row = cur.fetchone()
                self.assertEqual(row[0], "Test News 2")
                self.assertEqual(row[1], "Test Source")
                self.assertEqual(row[2], ["SPY"])
                self.assertEqual(row[3], -0.25)
                self.assertEqual(row[4], 3)
                self.assertEqual(row[5], False)
                self.assertEqual(row[6], ["test"])
                self.assertEqual(row[7], {"test": "value"})
            
            # Clean up test data
            with self.collector.db_conn.cursor() as cur:
                cur.execute(f"DELETE FROM {SCHEMA_NAME}.news_headlines WHERE headline LIKE 'Test News%'")
                self.collector.db_conn.commit()
                
        except Exception as e:
            self.fail(f"Database operation failed: {str(e)}")

    def test_rate_limiting(self):
        """Test API rate limiting"""
        # Make multiple requests and measure time
        start_time = time.time()
        for _ in range(5):
            self.collector._rate_limit()
        end_time = time.time()
        
        # Verify rate limiting (should take at least 2 seconds for 5 requests at 2 requests/second)
        self.assertGreaterEqual(end_time - start_time, 2.0)

    def test_error_handling(self):
        """Test error handling scenarios"""
        # Test API request failure
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("API Error")
            news = self.collector.collect_news()
            self.assertTrue(news.empty)
        
        # Test database connection failure
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection Error")
            with self.assertRaises(psycopg2.Error):
                self.collector.connect_db()

    def test_historical_data(self):
        """Test historical data collection"""
        with patch('requests.get') as mock_get:
            # Mock historical data response
            mock_response = MagicMock()
            mock_response.json.return_value = {"data": self.sample_news}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            # Test historical collection
            news = self.collector.collect_news(historical=True)
            self.assertIsInstance(news, pd.DataFrame)
            self.assertEqual(len(news), 2)

    def tearDown(self):
        """Clean up test environment"""
        if self.collector.db_conn and not self.collector.db_conn.closed:
            self.collector.db_conn.close()

if __name__ == "__main__":
    unittest.main()
