"""This module provides utilities for handling and manipulating pandas DataFrames."""

import pandas as pd


def df_to_consistent_str(df: pd.DataFrame, index: bool = False) -> str:
    """Converts a DataFrame to a unique string representation,
    regardless of the column or row order. This function is helpful to identify
    exact or close matches between collections of data frames.

    Args:
        df (pd.DataFrame): The DataFrame to be converted to a string.
        index (bool): If True, include the DataFrame's index in the string representation.

    Returns:
        str: A string representation of the DataFrame.
    """
    sorted_df = df.reindex(sorted(df.columns), axis=1)
    sorted_df = sorted_df.sort_values(by=sorted_df.columns.tolist(), na_position="last")
    return sorted_df.to_csv(index=index, header=True, sep=",").strip()
