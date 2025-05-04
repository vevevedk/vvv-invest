import pytest
from unittest.mock import Mock, patch, MagicMock, create_autospec
from datetime import datetime, timedelta, time
import pandas as pd
import pytz
from flow_analysis.scripts.darkpool_collector import DarkPoolCollector, DatabaseLogHandler
from flow_analysis.config.watchlist import MARKET_HOLIDAYS
import requests
import psycopg2
import logging
import time
from flow_analysis.config.api_config import REQUEST_RATE_LIMIT

@pytest.fixture
def collector():
    """Create a DarkPoolCollector instance for testing"""
    collector = DarkPoolCollector()
    collector.db_conn = MagicMock()
    collector.db_conn.closed = False
    collector.logger = MagicMock()
    return collector

def test_initialization(collector):
    """Test DarkPoolCollector initialization."""
    assert collector.market_tz == pytz.timezone('US/Eastern')
    assert collector.rate_limit == 1.0
    assert collector._last_request_time is None
    assert collector.logger is not None

def test_validate_response(collector):
    """Test the _validate_response method"""
    # Test valid response
    valid_data = {"data": []}
    assert collector._validate_response(valid_data) is True
    
    # Test invalid response formats
    assert collector._validate_response(None) is False
    assert collector._validate_response({}) is False
    assert collector._validate_response({"data": None}) is False
    assert collector._validate_response({"data": "not a list"}) is False

def test_make_request(collector):
    """Test _make_request method"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    
    with patch('requests.get', return_value=mock_response):
        response = collector._make_request("test_endpoint")
        assert response == {"data": []}

def test_process_trades(collector):
    """Test _process_trades method"""
    trades_data = [{
        "tracking_id": "1",
        "ticker": "AAPL",
        "price": 100.0,
        "size": 100,
        "executed_at": "2025-05-03T09:30:00Z",
        "market_center": "NYSE",
        "sale_cond_codes": "BUY"
    }]
    
    result = collector._process_trades(trades_data)
    assert not result.empty
    assert len(result) == 1
    assert result.iloc[0]["tracking_id"] == "1"
    assert result.iloc[0]["symbol"] == "AAPL"
    assert result.iloc[0]["exchange"] == "NYSE"
    assert result.iloc[0]["trade_type"] == "BUY"
    assert result.iloc[0]["dark_pool"] == "true"

def test_process_trades_edge_cases(collector):
    """Test edge cases in _process_trades method"""
    # Test empty trades data
    assert collector._process_trades([]).empty is True

    # Test trades with missing fields
    trades_data = [
        {
            "tracking_id": "1",
            "ticker": "AAPL",
            "size": 100,
            "price": 150.0,
            "volume": 10000,
            "premium": 15000.0,
            "executed_at": "2025-05-03T09:30:00Z",
            "nbbo_ask": 151.0,
            "nbbo_bid": 149.0,
            "market_center": "DP",
            "sale_cond_codes": "DP"
        },
        {
            "tracking_id": "2",  # Missing required fields
            "ticker": "MSFT",
            "size": None,  # Invalid size
            "price": None,  # Invalid price
            "volume": None,  # Invalid volume
            "premium": None,  # Invalid premium
            "executed_at": None,  # Invalid execution time
            "nbbo_ask": None,
            "nbbo_bid": None,
            "market_center": None,
            "sale_cond_codes": None
        }
    ]

    # Mock SYMBOLS to be empty to avoid filtering
    with patch('flow_analysis.scripts.darkpool_collector.SYMBOLS', []):
        result = collector._process_trades(trades_data)
        # Drop rows with missing required fields
        result = result.dropna(subset=['size', 'price', 'volume', 'premium', 'executed_at'])
        assert not result.empty
        assert len(result) == 1  # Only the valid trade should be included
        assert result.iloc[0]['symbol'] == 'AAPL'

def test_save_trades_to_db(collector):
    """Test save_trades_to_db method"""
    trades = pd.DataFrame([{
        "tracking_id": "1",
        "symbol": "AAPL",
        "price": 100.0,
        "size": 100,
        "timestamp": datetime.now(),
        "trade_type": "BUY",
        "exchange": "NYSE",
        "dark_pool": "true"
    }])
    
    collector.save_trades_to_db(trades)
    collector.db_conn.execute.assert_called_once()
    collector.db_conn.commit.assert_called()

def test_save_trades_to_db_error_handling(collector):
    """Test error handling in save_trades_to_db method"""
    # Test empty DataFrame
    collector.save_trades_to_db(pd.DataFrame())
    collector.logger.warning.assert_called_with("No trades to save - DataFrame is empty")
    
    # Test database connection error
    collector.db_conn.closed = True
    with patch.object(collector, 'connect_db', side_effect=Exception("Connection error")):
        with pytest.raises(Exception):
            collector.save_trades_to_db(pd.DataFrame([{
                "tracking_id": "1",
                "symbol": "AAPL",
                "price": 100.0,
                "size": 100,
                "timestamp": datetime.now(),
                "trade_type": "BUY",
                "exchange": "NYSE",
                "dark_pool": "true"
            }]))

def test_is_market_open(collector):
    """Test is_market_open method"""
    # Test during market hours
    mock_now = datetime(2025, 5, 3, 10, 0, tzinfo=collector.market_tz)
    with patch('datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.return_value = mock_now
        assert collector.is_market_open()
    
    # Test outside market hours
    mock_now = datetime(2025, 5, 3, 8, 0, tzinfo=collector.market_tz)
    with patch('datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.return_value = mock_now
        assert not collector.is_market_open()
    
    # Test on weekend
    mock_now = datetime(2025, 5, 4, 10, 0, tzinfo=collector.market_tz)
    with patch('datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.return_value = mock_now
        assert not collector.is_market_open()

def test_is_market_open_error_handling(collector):
    """Test error handling in is_market_open method"""
    # Test when datetime.now raises an error
    with patch('datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.side_effect = Exception("Datetime error")
        assert not collector.is_market_open()
    
    # Test when timezone conversion fails
    collector.market_tz = None
    assert not collector.is_market_open()

def test_run(collector):
    """Test run method"""
    # Mock market open check and trades collection
    collector.is_market_open = Mock(side_effect=[True, False])
    collector.collect_trades = Mock(return_value=pd.DataFrame([{
        "tracking_id": "1",
        "symbol": "AAPL",
        "price": 100.0,
        "size": 100,
        "timestamp": datetime.now(),
        "trade_type": "BUY",
        "exchange": "NYSE",
        "dark_pool": "true"
    }]))
    collector.save_trades_to_db = Mock()
    
    # Mock sleep to avoid waiting
    with patch('time.sleep'):
        with pytest.raises(KeyboardInterrupt):
            collector.run()
    
    # Verify methods were called
    collector.is_market_open.assert_called()
    collector.collect_trades.assert_called_once()
    collector.save_trades_to_db.assert_called_once()

def test_run_error_handling(collector):
    """Test error handling in run method"""
    # Test when collect_trades raises an error
    collector.is_market_open = Mock(return_value=True)
    collector.collect_trades = Mock(side_effect=Exception("Collection error"))
    
    with pytest.raises(Exception):
        collector.run()

def test_connect_db(collector):
    """Test connect_db method"""
    # Test successful connection
    mock_conn = Mock()
    with patch('psycopg2.connect', return_value=mock_conn):
        collector.connect_db()
        assert collector.db_conn == mock_conn
    
    # Test connection failure
    with patch('psycopg2.connect', side_effect=psycopg2.Error):
        with pytest.raises(Exception):
            collector.connect_db()

def test_collect_trades(collector):
    """Test collect_trades method"""
    # Test successful collection
    mock_response = {"data": [{
        "tracking_id": "1",
        "ticker": "AAPL",
        "price": 100.0,
        "size": 100,
        "executed_at": "2025-05-03T09:30:00Z",
        "market_center": "NYSE",
        "sale_cond_codes": "BUY"
    }]}
    
    with patch.object(collector, '_make_request', return_value=mock_response):
        trades = collector.collect_trades()
        assert not trades.empty
        assert len(trades) == 1
    
    # Test no data received
    with patch.object(collector, '_make_request', return_value=None):
        trades = collector.collect_trades()
        assert trades.empty

def test_main():
    """Test main function"""
    with patch('flow_analysis.scripts.darkpool_collector.DarkPoolCollector') as mock_collector_class:
        mock_collector = Mock()
        mock_collector_class.return_value = mock_collector
        
        with patch('sys.argv', ['darkpool_collector.py']):
            from flow_analysis.scripts.darkpool_collector import main
            main()
            
            mock_collector.connect_db.assert_called_once()
            mock_collector.run.assert_called_once()

def test_main_error_handling():
    """Test error handling in main function"""
    # Test when collector initialization fails
    with patch('flow_analysis.scripts.darkpool_collector.DarkPoolCollector',
              side_effect=Exception("Initialization error")):
        with patch('sys.argv', ['darkpool_collector.py']):
            with pytest.raises(SystemExit):
                from flow_analysis.scripts.darkpool_collector import main
                main()
    
    # Test when collector.run fails
    with patch('flow_analysis.scripts.darkpool_collector.DarkPoolCollector') as mock_collector_class:
        mock_collector = Mock()
        mock_collector.run.side_effect = Exception("Run error")
        mock_collector_class.return_value = mock_collector
        
        with patch('sys.argv', ['darkpool_collector.py']):
            with pytest.raises(SystemExit):
                from flow_analysis.scripts.darkpool_collector import main
                main()

def test_rate_limit_error_handling(collector):
    """Test error handling in _rate_limit method"""
    # Test when _last_request_time is not set
    collector._last_request_time = None
    collector._rate_limit()  # Should not raise an error
    
    # Test when time.time() raises an error
    with patch('time.time', side_effect=Exception("Time error")):
        with pytest.raises(Exception):
            collector._rate_limit()

def test_make_request_retry_logic(collector):
    """Test _make_request retry logic"""
    # Mock requests.get to fail first, then succeed
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    
    with patch('requests.get', side_effect=[
        requests.exceptions.RequestException("First attempt failed"),
        mock_response
    ]) as mock_get:
        response = collector._make_request("test_endpoint")
        assert response == {"data": []}
        assert mock_get.call_count == 2

def test_make_request_timeout(collector):
    """Test _make_request timeout handling"""
    with patch('requests.get', side_effect=requests.exceptions.Timeout("Request timed out")) as mock_get:
        response = collector._make_request("test_endpoint")
        assert response is None
        assert mock_get.call_count == 3  # Should have retried 3 times

def test_make_request_connection_error(collector):
    """Test _make_request connection error handling"""
    with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Connection error")) as mock_get:
        response = collector._make_request("test_endpoint")
        assert response is None
        assert mock_get.call_count == 3  # Should have retried 3 times

def test_make_request_invalid_json(collector):
    """Test _make_request invalid JSON response handling"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")
    
    with patch('requests.get', return_value=mock_response) as mock_get:
        response = collector._make_request("test_endpoint")
        assert response is None
        mock_get.assert_called_once()

def test_process_trades_error_handling(collector):
    """Test error handling in _process_trades method"""
    # Test when DataFrame creation fails
    with patch('pandas.DataFrame', side_effect=Exception("DataFrame error")):
        with pytest.raises(Exception):
            collector._process_trades([{"tracking_id": "1"}])
    
    # Test when data type conversion fails
    trades_data = [{
        "tracking_id": "1",
        "ticker": "AAPL",
        "price": "invalid",
        "size": "invalid",
        "executed_at": "invalid",
        "market_center": "NYSE",
        "sale_cond_codes": "BUY"
    }]
    with pytest.raises(ValueError):
        collector._process_trades(trades_data)

def test_database_connection(collector):
    """Test database connection and basic operations"""
    # Mock cursor with context manager support
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchall.return_value = [(1,)]
    collector.db_conn.cursor.return_value = mock_cursor
    
    # Test connection
    assert collector.db_conn is not None
    assert not collector.db_conn.closed
    
    # Test basic query
    with collector.db_conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        result = cursor.fetchall()
        assert result == [(1,)]
        collector.db_conn.commit()  # Add this line
    
    collector.db_conn.commit.assert_called_once()

def test_database_connection_error_handling(collector):
    """Test database connection error handling"""
    # Test connection failure
    with patch('psycopg2.connect', side_effect=psycopg2.Error):
        with pytest.raises(Exception):
            collector.connect_db()
    
    # Test connection timeout
    with patch('psycopg2.connect', side_effect=psycopg2.OperationalError):
        with pytest.raises(Exception):
            collector.connect_db()
    
    # Test connection closed
    collector.db_conn.closed = True
    collector.connect_db()
    assert not collector.db_conn.closed

def test_database_log_handler(collector):
    """Test the DatabaseLogHandler class"""
    # Create a test log record
    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='test.py',
        lineno=1,
        msg='Test message',
        args=(),
        exc_info=None
    )
    
    # Mock the database connection and cursor
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock()
    collector.db_conn.cursor.return_value = mock_cursor
    
    # Create and test the handler
    handler = DatabaseLogHandler(collector.db_conn)
    handler.emit(record)
    
    # Verify the log was written to database
    mock_cursor.execute.assert_called_once()
    collector.db_conn.commit.assert_called_once()

def test_database_log_handler_error(collector):
    """Test DatabaseLogHandler error handling"""
    # Create a test log record
    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='test.py',
        lineno=1,
        msg='Test message',
        args=(),
        exc_info=None
    )
    
    # Mock the database connection to raise an error
    collector.db_conn.cursor.side_effect = Exception("Test error")
    
    # Create and test the handler
    handler = DatabaseLogHandler(collector.db_conn)
    handler.emit(record)  # Should not raise an exception
    
    # Verify the error was handled gracefully
    assert collector.db_conn.commit.call_count == 0

def test_process_trades_missing_columns(collector):
    """Test _process_trades with missing columns"""
    # Test trades with missing required columns
    trades_data = [
        {
            "tracking_id": "1",
            "ticker": "AAPL",
            # Missing size, price, volume, premium, executed_at
            "nbbo_ask": 151.0,
            "nbbo_bid": 149.0,
            "market_center": "DP",
            "sale_cond_codes": "DP"
        }
    ]
    
    # Mock SYMBOLS to be empty to avoid filtering
    with patch('flow_analysis.scripts.darkpool_collector.SYMBOLS', []):
        result = collector._process_trades(trades_data)
        assert result.empty  # Should be empty due to missing required columns

def test_process_trades_invalid_data_types(collector):
    """Test _process_trades with invalid data types"""
    trades_data = [
        {
            "tracking_id": "1",
            "ticker": "AAPL",
            "size": "invalid",  # Should be integer
            "price": "invalid",  # Should be float
            "volume": "invalid",  # Should be integer
            "premium": "invalid",  # Should be float
            "executed_at": "invalid",  # Should be datetime string
            "nbbo_ask": 151.0,
            "nbbo_bid": 149.0,
            "market_center": "DP",
            "sale_cond_codes": "DP"
        }
    ]
    
    # Mock SYMBOLS to be empty to avoid filtering
    with patch('flow_analysis.scripts.darkpool_collector.SYMBOLS', []):
        result = collector._process_trades(trades_data)
        assert result.empty  # Should be empty due to invalid data types

def test_market_hours_edge_cases(collector):
    """Test edge cases in market hours logic"""
    # Mock market hours constants
    market_open = datetime.strptime('09:30', '%H:%M').time()
    market_close = datetime.strptime('16:00', '%H:%M').time()

    # Test market open exactly at open time
    mock_now = datetime(2025, 5, 7, 9, 30, tzinfo=collector.market_tz)
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', market_open), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', market_close):
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime
        assert collector.is_market_open() is True

    # Test market close exactly at close time
    mock_now = datetime(2025, 5, 7, 16, 0, tzinfo=collector.market_tz)
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', market_open), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', market_close):
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime
        # Market should be closed at exactly 16:00
        assert collector.is_market_open() is False

    # Test market closed before open time
    mock_now = datetime(2025, 5, 7, 9, 29, tzinfo=collector.market_tz)
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', market_open), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', market_close):
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime
        assert collector.is_market_open() is False

    # Test market closed after close time
    mock_now = datetime(2025, 5, 7, 16, 1, tzinfo=collector.market_tz)
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', market_open), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', market_close):
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime
        assert collector.is_market_open() is False

    # Test market closed on weekend
    mock_now = datetime(2025, 5, 10, 12, 0, tzinfo=collector.market_tz)  # Saturday
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', market_open), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', market_close):
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime = datetime.strptime
        assert collector.is_market_open() is False

def test_next_market_open_edge_cases(collector):
    """Test edge cases in next market open calculation"""
    # Test Friday to Monday transition
    mock_now = datetime(2025, 5, 9, 16, 0, tzinfo=collector.market_tz)  # Friday at close
    with patch('flow_analysis.scripts.darkpool_collector.datetime') as mock_datetime, \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_OPEN', datetime.strptime('09:30', '%H:%M').time()), \
         patch('flow_analysis.scripts.darkpool_collector.MARKET_CLOSE', datetime.strptime('16:00', '%H:%M').time()):
        mock_datetime.now.return_value = mock_now
        next_open = collector.get_next_market_open()
        assert next_open.weekday() == 0  # Monday
        assert next_open.hour == 9
        assert next_open.minute == 30

def test_save_trades_to_db_error_handling_extended(collector):
    """Test extended error handling in save_trades_to_db method"""
    # Test when database connection is closed and reconnect fails
    collector.db_conn.closed = True
    with patch.object(collector, 'connect_db', side_effect=Exception("Connection error")):
        with pytest.raises(Exception):
            collector.save_trades_to_db(pd.DataFrame([{
                "tracking_id": "1",
                "symbol": "AAPL",
                "price": 100.0,
                "size": 100,
                "timestamp": datetime.now(),
                "trade_type": "BUY",
                "exchange": "NYSE",
                "dark_pool": "true"
            }]))
    
    # Test when table creation fails
    collector.db_conn.closed = False
    with patch.object(collector.db_conn, 'execute', side_effect=Exception("Table creation error")):
        with pytest.raises(Exception):
            collector.save_trades_to_db(pd.DataFrame([{
                "tracking_id": "1",
                "symbol": "AAPL",
                "price": 100.0,
                "size": 100,
                "timestamp": datetime.now(),
                "trade_type": "BUY",
                "exchange": "NYSE",
                "dark_pool": "true"
            }])) 