"""
This module provides utilities for handling and manipulating pandas DataFrames.
"""

import pandas as pd
from datetime import date

def df_to_str(date_obj: date, target: pd.DataFrame) -> str:
    """
    Converts a DataFrame to a CSV string with a date prepended, sorting columns and rows
    for consistent comparison.

    Parameters:
        date_obj (datetime.date): The date to prepend to the resulting string.
        target (pandas.DataFrame): The DataFrame to be converted to a string.

    Returns:
        str: A CSV string representation of the DataFrame with the date prepended.
    """
    sorted_df = target.sort_index(axis=1)
    sorted_df = sorted_df.sort_values(by=list(sorted_df.columns))
    df_string = sorted_df.to_csv(index=False, header=True, sep=',')
    df_string_single_line = df_string.replace('\n', ',').strip(',')
    return f"{date_obj},{df_string_single_line}".strip()
