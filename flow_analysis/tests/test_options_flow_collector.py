import pytest
from datetime import datetime, timedelta
import pandas as pd
import pytz
from unittest.mock import patch, MagicMock
import os

from flow_analysis.scripts.options_flow_collector import OptionsFlowCollector

@pytest.fixture
def collector():
    with patch.dict(os.environ, {'UW_API_TOKEN': 'test_token'}):
        collector = OptionsFlowCollector()
        collector.db_conn = MagicMock()
        return collector

def test_initialization():
    with patch.dict(os.environ, {'UW_API_TOKEN': 'test_token'}):
        collector = OptionsFlowCollector()
        assert collector.api_key == 'test_token'
        assert collector.base_url == "https://api.unusualwhales.com/api"
        assert collector.eastern == pytz.timezone('US/Eastern')

def test_initialization_missing_token():
    with pytest.raises(ValueError):
        OptionsFlowCollector()

def test_validate_flow_data(collector):
    # Test empty dataframe
    df = pd.DataFrame()
    is_valid, msg = collector._validate_flow_data(df)
    assert is_valid
    assert msg == "Empty dataframe"

    # Test valid dataframe
    df = pd.DataFrame({
        'symbol': ['AAPL'],
        'strike': [150.0],
        'expiry': ['2024-04-19'],
        'flow_type': ['CALL'],
        'premium': [1000.0],
        'contract_size': [100],
        'iv_rank': [50.0],
        'collected_at': [datetime.now()]
    })
    is_valid, msg = collector._validate_flow_data(df)
    assert is_valid
    assert msg == "Validation successful"

    # Test missing required columns
    df = pd.DataFrame({'symbol': ['AAPL']})
    is_valid, msg = collector._validate_flow_data(df)
    assert not is_valid
    assert "Missing required columns" in msg

def test_process_flow(collector):
    # Test empty flow data
    flow_data = []
    df = collector._process_flow(flow_data)
    assert df.empty

    # Test valid flow data
    flow_data = [{
        'symbol': 'AAPL',
        'strike': 150.0,
        'expiry': '2024-04-19',
        'flow_type': 'CALL',
        'premium': 1000.0,
        'contract_size': 100,
        'iv_rank': 50.0,
        'timestamp': '2024-04-17T10:00:00Z'
    }]
    df = collector._process_flow(flow_data)
    assert not df.empty
    assert 'collected_at' in df.columns
    assert df['symbol'].iloc[0] == 'AAPL'

def test_is_market_open(collector):
    # Mock current time to be during market hours
    mock_time = datetime(2024, 4, 17, 10, 30, tzinfo=collector.eastern)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mock_time
        assert collector.is_market_open()

    # Mock current time to be outside market hours
    mock_time = datetime(2024, 4, 17, 8, 30, tzinfo=collector.eastern)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mock_time
        assert not collector.is_market_open()

    # Mock weekend
    mock_time = datetime(2024, 4, 20, 10, 30, tzinfo=collector.eastern)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mock_time
        assert not collector.is_market_open()

def test_get_next_market_open(collector):
    # Test during market hours
    mock_time = datetime(2024, 4, 17, 10, 30, tzinfo=collector.eastern)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mock_time
        next_open = collector.get_next_market_open()
        assert next_open.date() == mock_time.date() + timedelta(days=1)
        assert next_open.hour == 9
        assert next_open.minute == 30

    # Test before market hours
    mock_time = datetime(2024, 4, 17, 8, 30, tzinfo=collector.eastern)
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = mock_time
        next_open = collector.get_next_market_open()
        assert next_open.date() == mock_time.date()
        assert next_open.hour == 9
        assert next_open.minute == 30

def test_collect_historical_data(collector):
    historical_date = datetime(2024, 4, 17)
    with patch.object(collector, 'collect_flow') as mock_collect_flow:
        collector.collect_historical_data(historical_date)
        mock_collect_flow.assert_called_once_with(historical_date=historical_date)

def test_save_flow_to_db(collector):
    df = pd.DataFrame({
        'symbol': ['AAPL'],
        'strike': [150.0],
        'expiry': ['2024-04-19'],
        'flow_type': ['CALL'],
        'premium': [1000.0],
        'contract_size': [100],
        'iv_rank': [50.0],
        'collected_at': [datetime.now()]
    })
    
    collector._save_flow_to_db(df)
    collector.db_conn.cursor.assert_called_once()
    collector.db_conn.commit.assert_called_once() 