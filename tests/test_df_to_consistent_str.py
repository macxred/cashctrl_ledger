"""
This module tests the df_to_consistent_str function.
"""

import pandas as pd
from cashctrl_ledger import df_to_consistent_str

# Test with multi-row DataFrame
def test_simple_multi_row():
    df = pd.DataFrame({
        'B': [3, 1, 2],
        'A': [1, 3, 2],
        'D': [pd.Timestamp(dt) for dt in ['2023-01-01', '2023-01-03', '2023-01-02']],
        'C': [2.2, 0.00, 3.3]})
    expected = 'A,B,C,D\n1,3,2.2,2023-01-01\n2,2,3.3,2023-01-02\n3,1,0.0,2023-01-03'
    assert df_to_consistent_str(df) == expected

# Test with NaN values
def test_nan_values():
    df = pd.DataFrame({
        'B': [3, 1, pd.NA],
        'A': [1.0, None, 2.0],
        'C': [2, 1, 3]})
    expected = 'A,B,C\n1.0,3,2\n2.0,,3\n,1,1'
    assert df_to_consistent_str(df) == expected

# Test dfs with identical content but differing row or column order have same
# string representation
def test_reshuffled_dfs():
    df1 = pd.DataFrame({'B': [3, 1, pd.NA], 'A': [1, pd.NA, 2], 'C': [2, 1, 3]})
    df2 = df1.sample(frac=1)[['A', 'C', 'B']]
    assert df_to_consistent_str(df1) == df_to_consistent_str(df2)

# Test dfs with identical content but differing row or column order have same
# string representation when including indexes
def test_reshuffled_dfs_with_indexes():
    df1 = pd.DataFrame({'B': [3, 1, pd.NA], 'A': [1, pd.NA, 2], 'C': [2, 1, 3]})
    df2 = df1.sample(frac=1)[['A', 'C', 'B']]
    assert df_to_consistent_str(df1, index=True) == df_to_consistent_str(df2, index=True)

# Test dfs with equivalent content of different type have same string representation
def test_equivalent_content():
    df1 = pd.DataFrame({
        'B': pd.Series([3, 1.010, pd.NA], dtype=pd.Float64Dtype()),
        'A': pd.Series([1, pd.NA, 2.0], dtype=pd.Float64Dtype()),
        'C': pd.Series(['2', '1', '3'], dtype=pd.StringDtype())})
    df2 = pd.DataFrame({
        'B': pd.Series([3.00, 1.01, None], dtype=pd.Float64Dtype()),
        'A': pd.Series([1.0, pd.NA, 2], dtype=pd.Float64Dtype()),
        'C': pd.Series([2, 1, 3], dtype=pd.StringDtype())})
    assert df_to_consistent_str(df1) == df_to_consistent_str(df2.sample(frac=1))

