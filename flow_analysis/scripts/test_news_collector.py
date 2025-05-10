#!/usr/bin/env python3

"""
Test suite for the News Collector
Tests the functionality of the news collector including monitoring and validation
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta
import pytz
import psycopg2
import requests
from sqlalchemy import create_engine
import json
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from scripts.news_collector import NewsCollector
from scripts.monitoring import MetricsCollector, HealthChecker
from scripts.data_validation import DataValidator
from config.api_config import UW_API_TOKEN, UW_BASE_URL, NEWS_ENDPOINT
from config.db_config import DB_CONFIG, SCHEMA_NAME

class TestNewsCollector(unittest.TestCase):
    """Test cases for the News Collector"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        # Mock database configuration for testing
        cls.test_db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432'
        }
        
        # Sample news data for testing
        cls.sample_news_data = {
            "data": [
                {
                    "headline": "AAPL Reports Strong Q4 Earnings",
                    "source": "Bloomberg",
                    "published_at": "2024-04-17T14:30:00Z",
                    "symbols": ["AAPL"],
                    "sentiment": 0.8,
                    "impact_score": 7
                },
                {
                    "headline": "TSLA Stock Drops on Production Issues",
                    "source": "Reuters",
                    "published_at": "2024-04-17T15:00:00Z",
                    "symbols": ["TSLA"],
                    "sentiment": -0.6,
                    "impact_score": 5
                }
            ]
        }
        
    def setUp(self):
        """Set up each test case"""
        # Create mock objects
        self.mock_db = Mock()
        self.mock_cursor = Mock()
        self.mock_db.cursor.return_value = self.mock_cursor
        
        # Patch database connection
        self.db_patcher = patch('psycopg2.connect', return_value=self.mock_db)
        self.mock_db_connect = self.db_patcher.start()
        
        # Patch requests
        self.requests_patcher = patch('requests.get')
        self.mock_requests = self.requests_patcher.start()
        
        # Create collector instance
        self.collector = NewsCollector()
        
    def tearDown(self):
        """Clean up after each test"""
        self.db_patcher.stop()
        self.requests_patcher.stop()
        
    def test_initialization(self):
        """Test collector initialization"""
        self.assertIsNotNone(self.collector)
        self.assertIsNotNone(self.collector.metrics_collector)
        self.assertIsNotNone(self.collector.health_checker)
        self.assertIsNotNone(self.collector.data_validator)
        
    def test_database_connection(self):
        """Test database connection handling"""
        # Test successful connection
        self.collector.connect_db()
        self.mock_db_connect.assert_called_once_with(**DB_CONFIG)
        
        # Test connection error
        self.mock_db_connect.side_effect = psycopg2.OperationalError
        with self.assertRaises(psycopg2.OperationalError):
            self.collector.connect_db()
            
    def test_api_request(self):
        """Test API request handling"""
        # Mock successful response
        mock_response = Mock()
        mock_response.json.return_value = self.sample_news_data
        mock_response.raise_for_status = Mock()
        self.mock_requests.return_value = mock_response
        
        # Test successful request
        result = self.collector._make_request(NEWS_ENDPOINT)
        self.assertEqual(result, self.sample_news_data)
        
        # Test request error
        self.mock_requests.side_effect = requests.exceptions.RequestException
        with self.assertRaises(requests.exceptions.RequestException):
            self.collector._make_request(NEWS_ENDPOINT)
            
    def test_news_processing(self):
        """Test news data processing"""
        # Create sample news DataFrame
        news_df = pd.DataFrame(self.sample_news_data["data"])
        
        # Test processing
        processed_news = self.collector._process_news(self.sample_news_data["data"])
        self.assertIsInstance(processed_news, pd.DataFrame)
        self.assertEqual(len(processed_news), 2)
        
        # Verify required columns
        required_columns = [
            'headline', 'source', 'published_at', 'symbols',
            'sentiment', 'impact_score', 'collected_at'
        ]
        for col in required_columns:
            self.assertIn(col, processed_news.columns)
            
    def test_sentiment_analysis(self):
        """Test sentiment analysis"""
        # Test positive sentiment
        positive_headline = "AAPL Stock Surges on Strong Earnings"
        sentiment = self.collector._analyze_sentiment(positive_headline)
        self.assertGreater(sentiment, 0)
        
        # Test negative sentiment
        negative_headline = "TSLA Stock Plunges on Production Issues"
        sentiment = self.collector._analyze_sentiment(negative_headline)
        self.assertLess(sentiment, 0)
        
        # Test neutral sentiment
        neutral_headline = "Market Update: Regular Trading Day"
        sentiment = self.collector._analyze_sentiment(neutral_headline)
        self.assertEqual(sentiment, 0.0)
        
    def test_impact_score_calculation(self):
        """Test impact score calculation"""
        # Test high impact
        high_impact_headline = "AAPL Announces Major Product Launch"
        sentiment = self.collector._analyze_sentiment(high_impact_headline)
        impact_score = self.collector._calculate_impact_score(high_impact_headline, sentiment)
        self.assertGreater(impact_score, 5)
        
        # Test low impact
        low_impact_headline = "Regular Market Update"
        sentiment = self.collector._analyze_sentiment(low_impact_headline)
        impact_score = self.collector._calculate_impact_score(low_impact_headline, sentiment)
        self.assertLess(impact_score, 3)
        
    def test_duplicate_detection(self):
        """Test duplicate news detection"""
        # Add a headline to seen_headlines
        test_headline = "Test Headline"
        test_time = datetime.now(pytz.UTC)
        self.collector.seen_headlines.add((test_headline, test_time))
        
        # Create news with duplicate
        news_data = [
            {
                "headline": test_headline,
                "published_at": test_time,
                "source": "Test Source",
                "symbols": ["TEST"]
            },
            {
                "headline": "New Headline",
                "published_at": datetime.now(pytz.UTC),
                "source": "Test Source",
                "symbols": ["TEST"]
            }
        ]
        
        # Process news
        processed_news = self.collector._process_news(news_data)
        self.assertEqual(len(processed_news), 1)  # Only new headline should remain
        
    def test_database_save(self):
        """Test saving news to database"""
        # Create sample news DataFrame
        news_df = pd.DataFrame(self.sample_news_data["data"])
        
        # Mock database operations
        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchone = Mock(return_value=(2,))  # Mock count query
        
        # Test save operation
        self.collector.save_news_to_db(news_df)
        
        # Verify database operations
        self.mock_cursor.execute.assert_called()
        self.mock_db.commit.assert_called()
        
    def test_market_hours_check(self):
        """Test market hours checking"""
        # Test during market hours
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 4, 17, 14, 30)  # 2:30 PM
            self.assertTrue(self.collector.is_market_open())
            
        # Test outside market hours
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 4, 17, 20, 0)  # 8:00 PM
            self.assertFalse(self.collector.is_market_open())
            
        # Test weekend
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 4, 20, 14, 30)  # Saturday
            self.assertFalse(self.collector.is_market_open())
            
    def test_health_check_integration(self):
        """Test health check integration"""
        # Mock health check
        mock_health_status = Mock()
        mock_health_status.is_healthy = True
        self.collector.health_checker.check_health = Mock(return_value=mock_health_status)
        
        # Test run with healthy system
        self.collector.run()
        self.collector.health_checker.check_health.assert_called_once()
        
        # Test run with unhealthy system
        mock_health_status.is_healthy = False
        mock_health_status.errors = ["Database connection failed"]
        self.collector.run()  # Should log error and return
        
    def test_metrics_collection(self):
        """Test metrics collection integration"""
        # Mock metrics collection
        mock_metrics = Mock()
        self.collector.metrics_collector.collect_system_metrics = Mock(return_value=mock_metrics)
        self.collector.metrics_collector.save_metrics = Mock()
        
        # Test run with metrics collection
        self.collector.run()
        self.collector.metrics_collector.collect_system_metrics.assert_called_once()
        self.collector.metrics_collector.save_metrics.assert_called_once_with(mock_metrics)
        
    def test_data_validation_integration(self):
        """Test data validation integration"""
        # Create test news data
        test_news = pd.DataFrame([{
            "headline": "Test Headline",
            "source": "Test Source",
            "published_at": datetime.now(pytz.UTC),
            "symbols": ["TEST"],
            "sentiment": 0.5,
            "impact_score": 5
        }])
        
        # Mock validation
        mock_validation_result = Mock()
        mock_validation_result.is_valid = True
        mock_validation_result.cleaned_data = test_news.iloc[0].to_dict()
        self.collector.data_validator.validate_news_data = Mock(return_value=mock_validation_result)
        
        # Test validation
        processed_news = self.collector._process_news([test_news.iloc[0].to_dict()])
        self.assertEqual(len(processed_news), 1)
        self.collector.data_validator.validate_news_data.assert_called_once()

if __name__ == '__main__':
    unittest.main()
