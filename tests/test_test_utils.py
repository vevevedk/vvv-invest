import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tests.test_utils import (
    create_test_dataframe,
    assert_dataframes_equal,
    create_mock_response
)

def test_create_test_dataframe():
    # Test basic DataFrame creation
    columns = ['price', 'volume', 'date']
    df = create_test_dataframe(columns, num_rows=5)
    
    # Check shape and columns
    assert df.shape == (5, 3)
    assert list(df.columns) == columns
    
    # Check data types
    assert df['price'].dtype == np.float64
    assert df['volume'].dtype == np.int64
    assert isinstance(df.index, pd.DatetimeIndex)
    
    # Test with specific dates
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 5)
    df = create_test_dataframe(
        columns,
        num_rows=5,
        start_date=start_date,
        end_date=end_date
    )
    
    # Check date range
    assert df.index[0] == start_date
    assert df.index[-1] == end_date
    
    # Test reproducibility with random seed
    df1 = create_test_dataframe(columns, random_seed=123)
    df2 = create_test_dataframe(columns, random_seed=123)
    assert_dataframes_equal(df1, df2)

def test_assert_dataframes_equal():
    # Create two identical DataFrames
    df1 = pd.DataFrame({
        'A': [1.0, 2.0, 3.0],
        'B': [4, 5, 6]
    })
    df2 = df1.copy()
    
    # Should not raise any exception
    assert_dataframes_equal(df1, df2)
    
    # Test with different values
    df2['A'][0] = 1.00001
    with pytest.raises(AssertionError):
        assert_dataframes_equal(df1, df2)
    
    # Test with relaxed tolerance
    assert_dataframes_equal(df1, df2, rtol=1e-4)
    
    # Test with different dtypes
    df2['B'] = df2['B'].astype(float)
    with pytest.raises(AssertionError):
        assert_dataframes_equal(df1, df2)
    
    # Test ignoring dtypes
    assert_dataframes_equal(df1, df2, check_dtypes=False)

def test_create_mock_response():
    # Test successful response
    response = create_mock_response(
        status_code=200,
        json_data={'key': 'value'},
        text='{"key": "value"}'
    )
    
    assert response['status_code'] == 200
    assert response['json']() == {'key': 'value'}
    assert response['text'] == '{"key": "value"}'
    response['raise_for_status']()  # Should not raise
    
    # Test error response
    response = create_mock_response(status_code=404)
    assert response['status_code'] == 404
    with pytest.raises(Exception) as exc_info:
        response['raise_for_status']()
    assert 'HTTP 404' in str(exc_info.value)
    
    # Test empty response
    response = create_mock_response()
    assert response['status_code'] == 200
    assert response['json']() == {}
    assert response['text'] == '' 