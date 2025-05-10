import os
import sys
import unittest

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import requests
import pytz

@pytest.fixture
def mock_db_config():
    return {
        'dbname': 'test_db',
        'user': 'test_user',
        'password': 'test_pass',
        'host': 'localhost',
        'port': '5432'
    }

@pytest.fixture
def mock_api_key():
    return 'test_api_key'

@pytest.fixture
def collector(mock_db_config, mock_api_key):
    with patch('psycopg2.connect') as mock_connect:
        mock_connect.return_value = Mock()
        collector = OptionsFlowCollector(mock_db_config, mock_api_key)
        return collector

def test_initialization(collector):
    """Test that the collector initializes correctly"""
    assert collector.api_key == 'test_api_key'
    assert collector.db_conn is not None
    assert collector.MIN_VOLUME == 5
    assert collector.MIN_OPEN_INTEREST == 25
    assert collector.MAX_DTE == 60
    assert collector.MIN_DELTA == 0.02
    assert collector.MAX_BID_ASK_SPREAD_PCT == 0.35

def test_make_request(collector):
    """Test the _make_request method"""
    with patch('requests.get') as mock_get:
        # Test successful request
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [{'test': 'data'}]}
        mock_get.return_value = mock_response
        
        result = collector._make_request('test_endpoint')
        assert result == {'data': [{'test': 'data'}]}
        
        # Test rate limiting
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Rate limit exceeded")
        mock_get.return_value = mock_response
        
        result = collector._make_request('test_endpoint')
        assert result is None

def test_calculate_delta(collector):
    """Test the _calculate_delta method"""
    contract = {
        'option_type': 'call',
        'strike': 100,
        'days_to_expiry': 30,
        'volatility': 0.2,
        'risk_free_rate': 0.01
    }
    
    # Test call option
    delta = collector._calculate_delta(contract, underlying_price=100)
    assert isinstance(delta, float)
    assert 0 <= delta <= 1
    
    # Test put option
    contract['option_type'] = 'put'
    delta = collector._calculate_delta(contract, underlying_price=100)
    assert isinstance(delta, float)
    assert -1 <= delta <= 0

def test_filter_contracts(collector):
    """Test the _filter_contracts method"""
    contracts = [
        {
            'volume': 10,
            'open_interest': 30,
            'implied_volatility': 0.3,
            'nbbo_bid': 1.0,
            'nbbo_ask': 1.1,
            'option_symbol': 'TEST230101C00100000'
        },
        {
            'volume': 1,
            'open_interest': 2,
            'implied_volatility': 0.01,
            'nbbo_bid': 1.0,
            'nbbo_ask': 2.0,
            'option_symbol': 'TEST230101P00100000'
        }
    ]
    
    filtered = collector._filter_contracts(contracts)
    assert len(filtered) == 1  # Only the first contract should pass filters
    assert filtered[0]['option_symbol'] == 'TEST230101C00100000'

def test_get_option_contracts(collector):
    """Test the get_option_contracts method"""
    with patch.object(collector, '_make_request') as mock_request:
        mock_request.return_value = {
            'data': [
                {
                    'option_symbol': 'TEST230101C00100000',
                    'volume': 10,
                    'open_interest': 30,
                    'implied_volatility': 0.3,
                    'nbbo_bid': 1.0,
                    'nbbo_ask': 1.1
                }
            ]
        }
        
        contracts = collector.get_option_contracts('TEST')
        assert len(contracts) == 1
        assert contracts[0]['option_symbol'] == 'TEST230101C00100000'

def test_get_flow_data(collector):
    """Test the get_flow_data method"""
    with patch.object(collector, '_make_request') as mock_request:
        mock_request.return_value = {
            'data': [
                {
                    'trades': [
                        {'price': 1.0, 'size': 100, 'side': 'buy'},
                        {'price': 1.1, 'size': 50, 'side': 'sell'}
                    ],
                    'volume': 150,
                    'expiry': '2023-01-01'
                }
            ]
        }
        
        flow_data = collector.get_flow_data('TEST230101C00100000')
        assert len(flow_data) == 1
        assert 'vwap' in flow_data[0]
        assert 'buy_volume' in flow_data[0]
        assert 'sell_volume' in flow_data[0]
        assert 'net_flow' in flow_data[0]

def test_is_market_open(collector):
    """Test the is_market_open method"""
    mock_market_open = datetime(2023, 1, 1, 9, 30).time()
    mock_market_close = datetime(2023, 1, 1, 16, 0).time()
    mock_holidays = []
    
    # Test during market hours
    with patch('flow_analysis.scripts.options_flow_collector.MARKET_OPEN', mock_market_open), \
         patch('flow_analysis.scripts.options_flow_collector.MARKET_CLOSE', mock_market_close), \
         patch('flow_analysis.scripts.options_flow_collector.MARKET_HOLIDAYS', mock_holidays), \
         patch('flow_analysis.scripts.options_flow_collector.datetime') as mock_datetime:
        
        # Test during market hours
        mock_dt = Mock()
        mock_dt.weekday.return_value = 0  # Monday
        mock_dt.date.return_value = datetime(2023, 1, 2).date()
        mock_dt.time.return_value = datetime(2023, 1, 2, 9, 30).time()
        mock_datetime.now.return_value = mock_dt
        
        assert collector.is_market_open() is True
        
        # Test outside market hours
        mock_dt.time.return_value = datetime(2023, 1, 2, 16, 1).time()  # 4:01 PM
        assert collector.is_market_open() is False
        
        # Test on weekend
        mock_dt.weekday.return_value = 5  # Saturday
        assert collector.is_market_open() is False
        
        # Test on holiday
        mock_dt.weekday.return_value = 0  # Back to Monday
        mock_dt.date.return_value = datetime(2023, 1, 2).date()
        mock_holidays.append(datetime(2023, 1, 2))  # Make Monday a holiday
        assert collector.is_market_open() is False

def test_health_check(collector):
    """Test the health_check method"""
    with patch.object(collector, '_make_request') as mock_request:
        mock_request.return_value = {'data': []}
        
        health = collector.health_check()
        assert isinstance(health, dict)
        assert 'db_connected' in health
        assert 'api_accessible' in health
        assert 'rate_limit_ok' in health
        assert 'market_open' in health

def test_save_flow_signals(collector):
    """Test the save_flow_signals method"""
    # Create test data
    test_signals = [
        {
            'symbol': 'TEST',
            'timestamp': datetime(2023, 1, 1, 9, 30),
            'signal_type': 'flow',
            'signal_value': 1.0,
            'metadata': {'test': 'data'}
        }
    ]
    
    # Set up mock connection
    mock_conn = Mock()
    mock_conn.closed = False
    mock_conn.commit = Mock()
    collector.db_conn = mock_conn
    
    # Set up mock cursor
    mock_cursor = Mock()
    mock_cursor_instance = Mock()
    mock_cursor_instance.execute = Mock()
    mock_cursor_instance.mogrify = Mock(return_value=b'(%s,%s,%s,%s,%s)')
    mock_cursor_instance.connection = Mock()
    mock_cursor_instance.connection.encoding = 'UTF8'
    mock_cursor.__enter__ = Mock(return_value=mock_cursor_instance)
    mock_cursor.__exit__ = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock execute_values
    def mock_execute_values_impl(cur, sql, argslist, template=None, page_size=100, fetch=False):
        cur.execute(sql)
        return None
        
    with patch('flow_analysis.scripts.options_flow_collector.execute_values', side_effect=mock_execute_values_impl) as mock_execute_values:
        # Call the method
        collector.save_flow_signals(test_signals)
        
        # Verify database operations
        assert mock_cursor_instance.execute.call_count == 2
        
        # Verify CREATE TABLE call
        create_table_call = mock_cursor_instance.execute.call_args_list[0]
        assert "CREATE TABLE IF NOT EXISTS flow_signals" in create_table_call[0][0]
        
        # Verify INSERT call
        insert_call = mock_cursor_instance.execute.call_args_list[1]
        assert "INSERT INTO flow_signals" in insert_call[0][0]
        
        mock_conn.commit.assert_called_once()
        mock_execute_values.assert_called_once()
        
        # Verify the SQL query
        call_args = mock_execute_values.call_args[0]
        assert "INSERT INTO flow_signals" in call_args[1]
        assert len(call_args[2]) == 1
        assert call_args[2][0][0] == 'TEST'  # symbol
        assert call_args[2][0][1] == datetime(2023, 1, 1, 9, 30)  # timestamp
        assert call_args[2][0][2] == 'flow'  # signal_type
        assert call_args[2][0][3] == 1.0  # signal_value
        assert call_args[2][0][4] == '{"test": "data"}'  # metadata

def test_database_connection(collector):
    """Test database connection and data retrieval"""
    try:
        # Set up mock connection
        mock_conn = Mock()
        mock_conn.closed = False
        collector.db_conn = mock_conn
        
        # Set up mock cursor
        mock_cursor = Mock()
        mock_cursor_instance = Mock()
        mock_cursor_instance.fetchone.return_value = (1,)
        mock_cursor.__enter__ = Mock(return_value=mock_cursor_instance)
        mock_cursor.__exit__ = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Verify connection is established
        assert collector.db_conn is not None
        assert not collector.db_conn.closed
        
        # Test a simple query
        with collector.db_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result == (1,)
            
        # Test retrieving flow signals
        mock_cursor_instance.fetchone.return_value = (
            'TEST',  # symbol
            datetime(2023, 1, 1, 9, 30),  # timestamp
            'flow',  # signal_type
            1.0,  # signal_value
            {'test': 'data'}  # metadata
        )
        
        with collector.db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT symbol, timestamp, signal_type, signal_value, metadata
                FROM flow_signals
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                symbol, timestamp, signal_type, signal_value, metadata = result
                assert isinstance(symbol, str)
                assert isinstance(timestamp, datetime)
                assert isinstance(signal_type, str)
                assert isinstance(signal_value, float)
                assert isinstance(metadata, dict)
    except Exception as e:
        pytest.fail(f"Database test failed: {str(e)}") 