"""Module for functions that help in testing."""

import pandas as pd


def assert_frame_equal(
    left: pd.DataFrame, right: pd.DataFrame, *args, ignore_index: bool = False,
    ignore_columns: list = None, **kwargs
):
    """Extend the pandas method to compare two DataFrames for equality with options
    to ignore index or specific columns.

    Args:
        left (pd.DataFrame): First DataFrame to compare.
        right (pd.DataFrame): Second DataFrame to compare.
        ignore_index (bool, optional): Whether to ignore the index in the comparison.
                                       Defaults to False.
        ignore_columns (list, optional): List of column names to ignore (drop) in the comparison.
                                         Defaults to None.
        *args: Additional positional arguments to pass to pandas.testing.assert_frame_equal.
        **kwargs: Additional keyword arguments to pass to pandas.testing.assert_frame_equal.
    """
    if ignore_index:
        left = left.reset_index(drop=True)
        right = right.reset_index(drop=True)

    if ignore_columns:
        left = left.drop(columns=ignore_columns, errors="ignore")
        right = right.drop(columns=ignore_columns, errors="ignore")

    pd.testing.assert_frame_equal(left, right, *args, **kwargs)
