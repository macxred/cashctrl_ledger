"""Module for converting nested DataFrames into long format and vice versa."""

from typing import List
import pandas as pd


def nest(df: pd.DataFrame, columns: List[str], key: str = "data") -> pd.DataFrame:
    """Nest specified columns of a DataFrame.

    Nesting groups a DataFrame, keeps a single row for every unique combination
    of grouping columns, and nests columns other than the grouping columns
    into a column of DataFrames. All nested (inner) data frames share an identical
    column schema. Each row of the original data frame becomes a row in one of the
    inner data frames.

    Args:
        df (pd.DataFrame): The input DataFrame in long format.
        columns (List[str]): Columns to nest; these will appear in the inner data frames.
        key (str): The name of the resulting nested column. Defaults to 'data'.

    Returns:
        pd.DataFrame: A DataFrame with the data grouped into nested DataFrames.

    Example:
    >>> data = {
    >>>     'id': [10, 10, 16, 16],
    >>>     'text': ['hello', 'hello', 'world', 'world'],
    >>>     'sub_id': [33, 33, 20, 16],
    >>>     'sub_text': ['test1', 'test2', 'test3', 'test4']
    >>> }
    >>> df = pd.DataFrame(data)
    >>> nest(df, columns=['sub_id', 'sub_text'], key='items')
       id   text                                              items
    0  10  hello  sub_id sub_text
    0     33    test1
    1     33    test2
    1  16  world  sub_id sub_text
    0     20    test3
    1     16    test4
    """
    # Check if required columns are present
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"`columns` missing in df: {missing_columns}")

    # Ensure that the key column does not already exist
    if key in df.columns and key not in columns:
        raise ValueError(f"The key column '{key}' already exists in `df`.")

    if df.empty:
        empty_result = df.drop(columns=columns)
        empty_result[key] = []
        return empty_result

    group_columns = [col for col in df.columns if col not in columns]
    grouped = df.groupby(group_columns, dropna=False)
    nested_dfs = grouped.apply(lambda x: [x[columns].reset_index(drop=True)], include_groups=False)
    result = pd.DataFrame(grouped[group_columns].first())
    result[key] = [df[0] for df in nested_dfs.reset_index(drop=True)]
    return result.reset_index(drop=True)


def unnest(df: pd.DataFrame, key: str = "data") -> pd.DataFrame:
    """Expands nested DataFrames into long format.

    Unnest expands a specified data frame column that contains a list of
    nested data frames into rows and columns of the outer data frame.
    Each row or column of the nested data frames becomes a row or column in
    the resulting data frame.

    Args:
        df (pd.DataFrame): The input DataFrame with nested data.
        key (str): The name of the column with the nested DataFrames to unnest. Defaults to 'data'.

    Returns:
        pd.DataFrame: A DataFrame with the nested data flattened into long format.

    Example:
    >>> data = {
    >>>     'id': [10, 16],
    >>>     'text': ['hello', 'world'],
    >>>     'items': [
    >>>         pd.DataFrame({'sub_id': [33, 33], 'sub_text': ['test1', 'test2']}),
    >>>         pd.DataFrame({'sub_id': [20, 16], 'sub_text': ['test3', 'test4']})
    >>>     ]
    >>> }
    >>> df = pd.DataFrame(data)
    >>> unnest(df, key='items')
       id   text  sub_id sub_text
    0  10  hello      33    test1
    1  10  hello      33    test2
    2  16  world      20    test3
    3  16  world      16    test4
    """
    if df.empty:
        return df.drop(columns=[key])

    # Check if the key column exists in the DataFrame
    if key not in df.columns:
        raise ValueError(f"Key column '{key}' not found in `df`.")

    # Ensure 'key' column contains only DataFrames
    if not all(isinstance(item, pd.DataFrame) or item is None for item in df[key]):
        raise ValueError(f"All items in column '{key}' must be DataFrames.")

    df_reset = df.reset_index(drop=True)
    if all(item is None for item in df[key]):
        flat_dfs = pd.DataFrame()
    else:
        flat_dfs = pd.concat(df_reset[key].to_dict())
        flat_dfs.index = flat_dfs.index.get_level_values(0)

    result = (
        df_reset.drop(columns=[key])
        .join(flat_dfs, how="right", validate="1:m", rsuffix="_nested")
    )
    # Restore original index
    result.index = df.index[result.index]
    return result
