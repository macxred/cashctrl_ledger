"""
This module provides utilities for handling and manipulating pandas DataFrames.
"""

import pandas as pd

# rename to ledger_to_str
def ledger_to_str(target: pd.DataFrame) -> str:
    """
    Converts a DataFrame to a string, sorting columns and rows
    for consistent comparison.

    Parameter:
        target (pandas.DataFrame): The DataFrame to be converted to a string.

    Returns:
        str: A string representation of the DataFrame.
    """
    sorted_df = target.reindex(sorted(target.columns), axis=1)
    sorted_df = sorted_df.sort_values(by=sorted_df.columns.tolist(), na_position='last')
    return sorted_df.to_csv(index=False, header=True, sep=',').strip()