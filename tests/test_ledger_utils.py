"""
This module contains tests for the dataframe_utils module, specifically for the df_to_str function.
"""

import pandas as pd
from cashctrl_ledger import ledger_to_str

# Test with simple multi-row DataFrame
def test_simple_multi_row():
    data = {'B': [3, 1, 2], 'A': [1, 3, 2], 'C': [2, 1, 3]}
    df = pd.DataFrame(data)
    expected = 'A,B,C\n1,3,2\n2,2,3\n3,1,1'
    assert ledger_to_str(df) == expected

# Test with date columns
def test_date_columns():
    data = {'B': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03'), pd.Timestamp('2023-01-02')],
            'A': [1, 3, 2]}
    df = pd.DataFrame(data)
    expected = 'A,B\n1,2023-01-01\n2,2023-01-02\n3,2023-01-03'
    assert ledger_to_str(df) == expected

# Test with float columns
def test_float_columns():
    data = {'B': [3.1, 1.2, 2.3], 'A': [1, 3, 2], 'C': [2.2, 1.1, 3.3]}
    df = pd.DataFrame(data)
    expected = 'A,B,C\n1,3.1,2.2\n2,2.3,3.3\n3,1.2,1.1'
    assert ledger_to_str(df) == expected

# Test with NaN values
def test_nan_values():
    data = {'B': [3, 1, pd.NA], 'A': [1, pd.NA, 2], 'C': [2, 1, 3]}
    df = pd.DataFrame(data)
    expected = 'A,B,C\n1,3,2\n2,,3\n,1,1'
    assert ledger_to_str(df) == expected