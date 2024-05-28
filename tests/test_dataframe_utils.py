"""
This module contains tests for the dataframe_utils module, specifically for the df_to_str function.
"""

import pytest
import pandas as pd
from datetime import date, datetime
from cashctrl_ledger import df_to_str

def test_basic_functionality():
    data = {'B': [3, 1, 2], 'A': [1, 3, 2]}
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,B,1,3,2,2,3,1"
    assert result == expected

def test_empty_dataframe():
    df = pd.DataFrame()
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,"
    assert result == expected

def test_single_column():
    data = {'A': [1, 3, 2]}
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,1,2,3"
    assert result == expected

def test_single_row():
    data = {'B': [3], 'A': [1]}
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,B,1,3"
    assert result == expected

def test_large_dataframe():
    data = {
        'D': [1, 3, 2],
        'C': [4, 6, 5],
        'B': [7, 9, 8],
        'A': [10, 12, 11]
    }
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,B,C,D,10,7,4,1,11,8,5,2,12,9,6,3"
    assert result == expected

def test_complex_dataframe():
    data = {
        'C': [1, 2, 3, 4],
        'B': [5, 6, 7, 8],
        'A': [9, 10, 11, 12]
    }
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,B,C,9,5,1,10,6,2,11,7,3,12,8,4"
    assert result == expected

def test_nan_values():
    data = {
        'A': [1, 2, None],
        'B': [None, 2, 3]
    }
    df = pd.DataFrame(data)
    result = df_to_str(date(2024, 5, 28), df)
    expected = "2024-05-28,A,B,1.0,,2.0,2.0,,3.0"
    assert result == expected
