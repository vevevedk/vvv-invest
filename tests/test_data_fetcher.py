"""
Test script for data fetcher
"""

import os
import sys
import datetime
import unittest
from unittest.mock import patch, MagicMock
import json
import pandas as pd
from pathlib import Path

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options_flow.scripts.data_fetcher import OptionsDataFetcher
from options_flow.config.api_config import POLYGON_API_KEY, RAW_DATA_DIR, PROCESSED_DATA_DIR
from options_flow.config.watchlist import WATCHLIST

class TestOptionsDataFetcher(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        # Patch the time.sleep to avoid delays
        self.sleep_patcher = patch('time.sleep', return_value=None)
        self.sleep_patcher.start()
        
        self.fetcher = OptionsDataFetcher(POLYGON_API_KEY)
        self.test_date = datetime.date(2023, 4, 5)  # Use a past date with known data
        self.test_ticker = WATCHLIST[0]  # SPY
        
        # Sample response data
        self.sample_contract = {
            "ticker": "SPY250405C00400000",
            "underlying_ticker": "SPY",
            "expiration_date": "2025-04-05",
            "strike_price": 400.0,
            "contract_type": "call",
            "open_interest": 1000,
            "implied_volatility": 0.25
        }
        
        self.sample_trade = {
            "ticker": "SPY250405C00400000",
            "price": 1.25,
            "size": 100,
            "timestamp": "2023-04-05T10:00:00Z",
            "exchange": "CBOE",
            "conditions": ["R"]
        }
        
        self.sample_aggregate = {
            "ticker": "SPY250405C00400000",
            "open": 1.20,
            "high": 1.30,
            "low": 1.15,
            "close": 1.25,
            "volume": 1000,
            "vwap": 1.23
        }

        self.sample_dark_pool = {
            "ticker": "SPY",
            "date": "2023-04-05",
            "estimated_volume": 1000000,
            "estimated_price": 400.50,
            "confidence": 0.85
        }

    def tearDown(self):
        """Clean up after tests"""
        self.sleep_patcher.stop()

    @patch('requests.get')
    def test_fetch_options_contracts(self, mock_get):
        """Test fetching options contracts"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [self.sample_contract],
            "next_url": None
        }
        mock_get.return_value = mock_response
        
        contracts = self.fetcher.fetch_options_contracts(self.test_ticker)
        
        # Verify results
        self.assertIsInstance(contracts, list)
        self.assertEqual(len(contracts), 1)
        self.assertEqual(contracts[0]["ticker"], self.sample_contract["ticker"])
        self.assertEqual(contracts[0]["underlying_ticker"], self.sample_contract["underlying_ticker"])

    @patch('requests.get')
    def test_fetch_options_trades(self, mock_get):
        """Test fetching options trades"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [self.sample_trade],
            "next_url": None
        }
        mock_get.return_value = mock_response
        
        trades = self.fetcher.fetch_options_trades(self.test_ticker, self.test_date)
        
        # Verify results
        self.assertIsInstance(trades, list)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["ticker"], self.sample_trade["ticker"])
        self.assertEqual(trades[0]["price"], self.sample_trade["price"])

    @patch('requests.get')
    def test_fetch_options_aggregates(self, mock_get):
        """Test fetching options aggregates"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [self.sample_aggregate],
            "next_url": None
        }
        mock_get.return_value = mock_response
        
        aggregates = self.fetcher.fetch_options_aggregates(self.test_ticker, self.test_date)
        
        # Verify results
        self.assertIsInstance(aggregates, list)
        self.assertEqual(len(aggregates), 1)
        self.assertEqual(aggregates[0]["ticker"], self.sample_aggregate["ticker"])
        self.assertEqual(aggregates[0]["volume"], self.sample_aggregate["volume"])

    @patch('requests.get')
    def test_invalid_ticker(self, mock_get):
        """Test handling of invalid ticker"""
        # Mock empty response
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response
        
        invalid_ticker = "INVALID"
        contracts = self.fetcher.fetch_options_contracts(invalid_ticker)
        self.assertEqual(contracts, [])

    @patch('requests.get')
    def test_invalid_date(self, mock_get):
        """Test handling of invalid date"""
        # Mock error response
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_response
        
        invalid_date = datetime.date(1900, 1, 1)  # Too far in the past
        trades = self.fetcher.fetch_options_trades(self.test_ticker, invalid_date)
        self.assertEqual(trades, [])

    @patch('requests.get')
    def test_fetch_dark_pool_estimates(self, mock_get):
        """Test fetching dark pool estimates"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [self.sample_dark_pool],
            "next_url": None
        }
        mock_get.return_value = mock_response
        
        estimates = self.fetcher.fetch_dark_pool_estimates(self.test_ticker, self.test_date)
        
        # Verify results
        self.assertIsInstance(estimates, list)
        self.assertEqual(len(estimates), 1)
        self.assertEqual(estimates[0]["ticker"], self.sample_dark_pool["ticker"])
        self.assertEqual(estimates[0]["estimated_volume"], self.sample_dark_pool["estimated_volume"])

    @patch('requests.get')
    def test_fetch_all_data(self, mock_get):
        """Test fetching all data for a date"""
        # Mock successful responses for all endpoints
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [self.sample_contract],
            "next_url": None
        }
        mock_get.return_value = mock_response
        
        # Test fetching all data
        result = self.fetcher.fetch_all_data(self.test_date)
        
        # Verify results
        self.assertIsInstance(result, dict)
        self.assertIn('contracts', result)
        self.assertIn('trades', result)
        self.assertIn('aggregates', result)
        self.assertIn('dark_pool', result)

    @patch('requests.get')
    def test_pagination(self, mock_get):
        """Test handling of paginated responses"""
        # Mock first page response
        first_page = MagicMock()
        first_page.json.return_value = {
            "results": [self.sample_contract],
            "next_url": "https://api.polygon.io/v3/next_page"
        }
        
        # Mock second page response
        second_page = MagicMock()
        second_page.json.return_value = {
            "results": [self.sample_contract],
            "next_url": None
        }
        
        # Set up mock to return different responses
        mock_get.side_effect = [first_page, second_page]
        
        contracts = self.fetcher.fetch_options_contracts(self.test_ticker)
        
        # Verify results
        self.assertIsInstance(contracts, list)
        self.assertEqual(len(contracts), 2)  # Should have results from both pages
        self.assertEqual(mock_get.call_count, 2)  # Should have made two API calls

    @patch('requests.get')
    def test_api_error_handling(self, mock_get):
        """Test handling of API errors"""
        # Mock failed response
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_get.return_value = mock_response
        
        # Test error handling
        contracts = self.fetcher.fetch_options_contracts(self.test_ticker)
        self.assertEqual(contracts, [])  # Should return empty list on error

    def test_directory_creation(self):
        """Test creation of data directories"""
        # Create test directories if they don't exist
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        
        # Initialize fetcher (should not raise any errors)
        try:
            fetcher = OptionsDataFetcher(POLYGON_API_KEY)
        except Exception as e:
            self.fail(f"Failed to initialize fetcher: {e}")
        
        # Verify directories exist
        self.assertTrue(os.path.exists(RAW_DATA_DIR))
        self.assertTrue(os.path.exists(PROCESSED_DATA_DIR))
        
        # Verify directories are writable
        test_file = os.path.join(RAW_DATA_DIR, "test.txt")
        try:
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
        except Exception as e:
            self.fail(f"Failed to write to directory: {e}")

if __name__ == "__main__":
    unittest.main() 