"""Unit tests for the unnest function."""

from cashctrl_ledger import nest, unnest
import pandas as pd
import pytest


def test_unnest_basic():
    df = pd.DataFrame({
        "id": [10, 16],
        "text": ["hello", "world"],
        "items": [
            pd.DataFrame({"sub_id": [33, 33], "sub_text": ["test1", "test2"]}),
            pd.DataFrame({"sub_id": [20, 16], "sub_text": ["test3", "test4"]})
        ]
    })
    result = unnest(df, key="items")
    expected_data = {
        "id": [10, 10, 16, 16],
        "text": ["hello", "hello", "world", "world"],
        "sub_id": [33, 33, 20, 16],
        "sub_text": ["test1", "test2", "test3", "test4"]
    }
    expected_df = pd.DataFrame(expected_data, index=[0, 0, 1, 1])
    pd.testing.assert_frame_equal(result, expected_df)


def test_unnest_with_matching_columns_in_inner_and_outer_dfs():
    df = pd.DataFrame({
        "id": [10, 16],
        "text": ["hello", "world"],
        "items": [
            pd.DataFrame({"id": [33, 33], "sub_text": ["test1", "test2"]}),
            pd.DataFrame({"id": [20, 16], "sub_text": ["test3", "test4"]})
        ]
    })
    result = unnest(df, key="items")
    expected_data = {
        "id": [10, 10, 16, 16],
        "text": ["hello", "hello", "world", "world"],
        "id_nested": [33, 33, 20, 16],
        "sub_text": ["test1", "test2", "test3", "test4"]
    }
    expected_df = pd.DataFrame(expected_data, index=[0, 0, 1, 1])
    pd.testing.assert_frame_equal(result, expected_df)


def test_unnest_with_empty_nested_dataframe():
    df = pd.DataFrame({
        "id": [10],
        "text": ["hello"],
        "items": [pd.DataFrame(columns=["sub_id", "sub_text"])]
    })
    result = unnest(df, key="items")
    expected_df = pd.DataFrame(columns=["id", "text", "sub_id", "sub_text"])
    expected_df["id"] = expected_df["id"].astype(df["id"].dtype)
    expected_df["text"] = expected_df["text"].astype(df["text"].dtype)
    pd.testing.assert_frame_equal(result, expected_df)


def test_unnest_with_none_in_key_column():
    data = {
        "id": [10, 16, 42],
        "text": ["hello", "my", "world"],
        "items": [
            pd.DataFrame({"sub_id": [33, 33], "sub_text": ["test1", "test2"]}),
            None,  # None should be handled gracefully
            pd.DataFrame({"sub_id": [42], "sub_text": ["test42"]})
        ]
    }
    df = pd.DataFrame(data)
    result = unnest(df, key="items")
    expected_data = {
        "id": [10, 10, 42],
        "text": ["hello", "hello", "world"],
        "sub_id": [33, 33, 42],
        "sub_text": ["test1", "test2", "test42"]
    }
    expected_df = pd.DataFrame(expected_data, index=[0, 0, 2])
    pd.testing.assert_frame_equal(result, expected_df)


def test_unnest_with_only_none_in_key_columns():
    df = pd.DataFrame({
        "id": [10],
        "text": ["hello"],
        "items": [None]
    })
    result = unnest(df, key="items")
    expected_df = pd.DataFrame(columns=["id", "text"])
    expected_df["id"] = expected_df["id"].astype(df["id"].dtype)
    expected_df["text"] = expected_df["text"].astype(df["text"].dtype)
    pd.testing.assert_frame_equal(result, expected_df)


def test_unnest_with_empty_dataframe():
    df = pd.DataFrame(columns=["id", "text", "items"])
    result = unnest(df, key="items")
    expected_df = df.drop(columns=["items"])
    pd.testing.assert_frame_equal(result, expected_df)


def test_successive_unnest_and_nest_returns_original_df():
    data = {
        "id": [10, 16],
        "text": ["hello", "world"],
        "data": [
            pd.DataFrame({"sub_id": [33, 33], "sub_text": ["test1", "test2"]}),
            pd.DataFrame({"sub_id": [20, 16], "sub_text": ["test3", "test4"]})
        ]
    }
    df = pd.DataFrame(data)
    unnested_df = unnest(df)
    nested_df = nest(unnested_df, columns=["sub_id", "sub_text"])
    pd.testing.assert_frame_equal(df, nested_df)


def test_unnest_with_wrong_type_key_column_raises_error():
    data = {
        "id": [10, 16],
        "text": ["hello", "world"],
        "data": [1, 2]  # This should raise an error
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="All items in column 'data' must be DataFrames."):
        unnest(df)


def test_unnest_with_non_dataframe_objects():
    data = {
        "id": [10, 16],
        "text": ["hello", "world"],
        "test": [
            pd.DataFrame({"sub_id": [33, 33], "sub_text": ["test1", "test2"]}),
            "not a dataframe"  # This should raise an error
        ]
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="All items in column 'test' must be DataFrames."):
        unnest(df, key="test")


def test_unnest_with_missing_key_column_raises_error():
    data = {
        "id": [10, 16],
        "text": ["hello", "world"],
        # No 'items' column here
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="Key column 'items' not found in `df`."):
        unnest(df, key="items")
