#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta
import pytz
from news_collector import NewsCollector
import json
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
                "impact_score": 5
            },
            {
                "headline": "Test News 2",
                "source": "Test Source",
                "published_at": "2025-04-17T11:00:00Z",
                "symbols": ["SPY"],
                "sentiment": -0.25,
                "impact_score": 3
            }
        ]
        
    def test_collect_news(self):
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
            self.assertEqual(list(news.columns), [
                'headline', 'source', 'published_at', 'symbols',
                'sentiment', 'impact_score', 'collected_at'
            ])
            
    def test_rate_limiting(self):
        """Test rate limiting functionality"""
        start_time = datetime.now()
        self.collector._rate_limit()
        self.collector._rate_limit()
        end_time = datetime.now()
        
        # Verify minimum time between requests
        min_interval = 1.0 / self.collector.REQUEST_RATE_LIMIT
        actual_interval = (end_time - start_time).total_seconds()
        self.assertGreaterEqual(actual_interval, min_interval)
        
    def test_database_operations(self):
        """Test database operations"""
        # Create test data
        test_df = pd.DataFrame(self.sample_news)
        test_df['collected_at'] = datetime.now(pytz.timezone('US/Eastern'))
        
        # Test save operation
        try:
            self.collector.save_news_to_db(test_df)
            
            # Verify data was saved
            with self.collector.db_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.collector.SCHEMA_NAME}.news_headlines")
                count = cur.fetchone()[0]
                self.assertGreater(count, 0)
                
                # Verify data integrity
                cur.execute(f"""
                    SELECT headline, source, symbols, sentiment, impact_score
                    FROM {self.collector.SCHEMA_NAME}.news_headlines
                    ORDER BY published_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                self.assertEqual(row[0], "Test News 2")
                self.assertEqual(row[1], "Test Source")
                self.assertEqual(row[2], ["SPY"])
                self.assertEqual(row[3], -0.25)
                self.assertEqual(row[4], 3)
                
        except Exception as e:
            self.fail(f"Database operation failed: {str(e)}")
            
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
                
    def test_market_hours(self):
        """Test market hours checking"""
        # Test during market hours
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 4, 17, 10, 0)  # 10 AM ET
            self.assertTrue(self.collector.is_market_open())
            
        # Test outside market hours
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 4, 17, 4, 0)  # 4 AM ET
            self.assertFalse(self.collector.is_market_open())
            
        # Test weekend
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 4, 19, 10, 0)  # Saturday
            self.assertFalse(self.collector.is_market_open())
            
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