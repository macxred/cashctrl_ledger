import pandas as pd

def assert_frame_equal(left, right, *args, ignore_index=False, ignore_columns=None, **kwargs):
    """
    Extend the pandas method to compare two DataFrames for equality with options to ignore index or specific columns.

    Parameters:
    left : DataFrame
        First DataFrame to compare.
    right : DataFrame
        Second DataFrame to compare.
    *args : tuple
        Additional positional arguments to pass to pandas.testing.assert_frame_equal.
    ignore_index : bool, default False
        Whether to ignore the index in the comparison.
    ignore_columns : list, default None
        List of column names to ignore (drop) in the comparison.
    **kwargs : additional keyword arguments
        Additional arguments to pass to pandas.testing.assert_frame_equal.
    """
    if ignore_index:
        left = left.reset_index(drop=True)
        right = right.reset_index(drop=True)

    if ignore_columns:
        left = left.drop(columns=ignore_columns, errors='ignore')
        right = right.drop(columns=ignore_columns, errors='ignore')

    pd.testing.assert_frame_equal(left, right, *args, **kwargs)