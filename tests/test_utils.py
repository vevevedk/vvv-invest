import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

def create_test_dataframe(
    columns: List[str],
    num_rows: int = 10,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    random_seed: int = 42
) -> pd.DataFrame:
    """
    Create a test DataFrame with random data.
    
    Args:
        columns: List of column names
        num_rows: Number of rows to generate
        start_date: Start date for datetime index (if None, uses current date)
        end_date: End date for datetime index (if None, uses start_date + num_rows days)
        random_seed: Random seed for reproducibility
        
    Returns:
        pd.DataFrame: Generated test DataFrame
    """
    np.random.seed(random_seed)
    
    if start_date is None:
        start_date = datetime.now()
    if end_date is None:
        end_date = start_date + timedelta(days=num_rows)
    
    date_range = pd.date_range(start=start_date, end=end_date, periods=num_rows)
    
    data = {}
    for col in columns:
        if col.endswith('_price'):
            data[col] = np.random.uniform(10, 100, num_rows)
        elif col.endswith('_volume'):
            data[col] = np.random.randint(100, 10000, num_rows)
        elif col.endswith('_date'):
            data[col] = date_range
        else:
            data[col] = np.random.rand(num_rows)
    
    return pd.DataFrame(data, index=date_range)

def assert_dataframes_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    check_dtypes: bool = True,
    check_index_type: bool = True,
    check_column_type: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8
) -> None:
    """
    Assert that two DataFrames are equal, with configurable precision.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        check_dtypes: Whether to check column dtypes
        check_index_type: Whether to check index type
        check_column_type: Whether to check column type
        rtol: Relative tolerance for floating point comparison
        atol: Absolute tolerance for floating point comparison
    """
    pd.testing.assert_frame_equal(
        df1,
        df2,
        check_dtypes=check_dtypes,
        check_index_type=check_index_type,
        check_column_type=check_column_type,
        rtol=rtol,
        atol=atol
    )

def create_mock_response(
    status_code: int = 200,
    json_data: Optional[Dict[str, Any]] = None,
    text: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a mock response object for testing API calls.
    
    Args:
        status_code: HTTP status code
        json_data: JSON response data
        text: Raw text response
        
    Returns:
        Dict[str, Any]: Mock response object
    """
    return {
        'status_code': status_code,
        'json': lambda: json_data if json_data else {},
        'text': text if text else '',
        'raise_for_status': lambda: None if status_code < 400 else Exception(f'HTTP {status_code}')
    } 