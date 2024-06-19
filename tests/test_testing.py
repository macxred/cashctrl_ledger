"""
Unit tests for functions that help in testing.
"""

import pytest
import pandas as pd
from cashctrl_ledger import assert_frame_equal

def test_assert_frame_equal_basic_equality():
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    assert_frame_equal(df1, df2)

def test_assert_frame_equal_different_indices():
    df1 = pd.DataFrame({'A': [1, 2, 3]}, index=[0, 1, 2])
    df2 = pd.DataFrame({'A': [1, 2, 3]}, index=[3, 4, 5])
    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2)
    assert_frame_equal(df1, df2, ignore_index=True)

def test_assert_frame_equal_ignore_columns():
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
    df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [10, 11, 12]})
    assert_frame_equal(df1, df2, ignore_columns=['C'])

def test_assert_frame_equal_ignore_columns_with_inequality():
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
    df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [10, 11, 12]})
    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2)
    assert_frame_equal(df1, df2, ignore_columns=['C'])

def test_assert_frame_equal_inequality():
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 7]})
    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2)

def test_assert_frame_equal_empty_dataframes():
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    assert_frame_equal(df1, df2)

def test_assert_frame_equal_different_shapes():
    df1 = pd.DataFrame({'A': [1, 2, 3]})
    df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    with pytest.raises(AssertionError):
        assert_frame_equal(df1, df2)