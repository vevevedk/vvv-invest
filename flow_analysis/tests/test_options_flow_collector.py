import pytest
from datetime import datetime, timedelta
import pandas as pd
import pytz
from unittest.mock import patch, MagicMock
import os
import psycopg2

from flow_analysis.scripts.options_flow_collector import OptionsFlowCollector

@pytest.fixture
def collector():
    """Create an OptionsFlowCollector instance with mocked database connection."""
    with patch('psycopg2.connect') as mock_connect, \
         patch('psycopg2.extras.execute_values') as mock_execute_values:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock execute_values
        mock_cursor.execute_values = mock_execute_values
        
        # Mock connection encoding
        mock_conn.encoding = 'utf-8'
        
        # Mock connection
        mock_cursor.connection = mock_conn
        
        collector = OptionsFlowCollector(
            db_config={
                'host': 'localhost',
                'port': '5432',
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            },
            api_key='test_api_key'
        )
        yield collector

def test_initialization(collector):
    """Test that the collector is initialized correctly."""
    assert collector.api_key == 'test_api_key'
    assert collector.db_config['host'] == 'localhost'
    assert collector.db_config['port'] == '5432'
    assert collector.db_config['database'] == 'test_db'
    assert collector.db_config['user'] == 'test_user'
    assert collector.db_config['password'] == 'test_password'

def test_initialization_missing_token():
    """Test that initialization fails when API key is missing."""
    with pytest.raises(ValueError):
        OptionsFlowCollector(
            db_config={
                'host': 'localhost',
                'port': '5432',
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            },
            api_key=''
        )

def test_calculate_delta(collector):
    """Test delta calculation."""
    contract = {
        'option_type': 'call',
        'strike': 100,
        'underlying_price': 100,
        'days_to_expiry': 30,
        'volatility': 0.2,
        'risk_free_rate': 0.01
    }
    delta = collector._calculate_delta(contract, 100)
    assert isinstance(delta, float)
    assert 0 <= delta <= 1

def test_calculate_call_delta(collector):
    """Test call delta calculation."""
    contract = {
        'option_type': 'call',
        'strike': 100,
        'underlying_price': 100,
        'days_to_expiry': 30,
        'volatility': 0.2,
        'risk_free_rate': 0.01
    }
    delta = collector._calculate_delta(contract, 100)
    assert delta > 0

def test_calculate_put_delta(collector):
    """Test put delta calculation."""
    contract = {
        'option_type': 'put',
        'strike': 100,
        'underlying_price': 100,
        'days_to_expiry': 30,
        'volatility': 0.2,
        'risk_free_rate': 0.01
    }
    delta = collector._calculate_delta(contract, 100)
    assert delta < 0

def test_filter_contracts(collector):
    """Test contract filtering."""
    contracts = [
        {
            'volume': 10,
            'open_interest': 50,
            'delta': 0.1,
            'bid': 1.0,
            'ask': 1.1
        },
        {
            'volume': 1,
            'open_interest': 10,
            'delta': 0.01,
            'bid': 1.0,
            'ask': 1.5
        }
    ]
    filtered = collector._filter_contracts(contracts)
    assert len(filtered) == 1
    assert filtered[0]['volume'] == 10

def test_get_option_contracts(collector):
    """Test getting option contracts."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {
            'data': {
                'options': {
                    'calls': [],
                    'puts': []
                }
            }
        }
        contracts = collector.get_option_contracts('AAPL')
        assert isinstance(contracts, list)

def test_get_flow_data(collector):
    """Test getting flow data."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {
            'data': {
                'flow': []
            }
        }
        flow_data = collector.get_flow_data('AAPL')
        assert isinstance(flow_data, list)

def test_save_flow_signals(collector):
    """Test saving flow signals."""
    signals = [
        {
            'symbol': 'AAPL',
            'timestamp': datetime.now(pytz.UTC),
            'signal_type': 'flow',
            'signal_value': 1.0,
            'metadata': {'test': 'data'}
        }
    ]
    collector.save_flow_signals(signals)
    collector.db_conn.cursor.return_value.execute.assert_called_once()

def test_is_market_open(collector):
    """Test market open check."""
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2024, 1, 2, 10, 0, tzinfo=collector.eastern)
        assert collector.is_market_open() is True
        
        mock_datetime.now.return_value = datetime(2024, 1, 2, 8, 0, tzinfo=collector.eastern)
        assert collector.is_market_open() is False

def test_collect_flow(collector):
    """Test flow collection."""
    with patch.object(collector, 'get_option_contracts') as mock_get_contracts, \
         patch.object(collector, 'get_flow_data') as mock_get_flow, \
         patch.object(collector, 'save_flow_signals') as mock_save_signals:
        mock_get_contracts.return_value = []
        mock_get_flow.return_value = []
        
        collector.collect_flow('AAPL')
        
        mock_get_contracts.assert_called_once_with('AAPL')
        mock_get_flow.assert_called_once_with('AAPL')
        mock_save_signals.assert_called_once()

def test_run(collector):
    """Test run method."""
    with patch.object(collector, 'collect_flow') as mock_collect_flow:
        collector.run(['AAPL', 'MSFT'])
        assert mock_collect_flow.call_count == 2
        mock_collect_flow.assert_any_call('AAPL', None)
        mock_collect_flow.assert_any_call('MSFT', None) 