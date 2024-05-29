"""
Unit tests for the nest function.
"""
import pytest
import pandas as pd
from cashctrl_ledger import nest, unnest

def test_nest_basic():
    df = pd.DataFrame({
        'id': [10, 10, 16, 16],
        'text': ['hello', 'hello', 'world', 'world'],
        'sub_id': [33, 33, 20, 16],
        'sub_text': ['test1', 'test2', 'test3', 'test4']
    })
    result = nest(df, columns=['sub_id', 'sub_text'], key='items')
    expected_data = {
        'id': [10, 16],
        'text': ['hello', 'world'],
        'items': [
            pd.DataFrame({'sub_id': [33, 33], 'sub_text': ['test1', 'test2']}),
            pd.DataFrame({'sub_id': [20, 16], 'sub_text': ['test3', 'test4']})
        ]
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)

def test_nest_with_empty_dataframe():
    df = pd.DataFrame(columns=['id', 'text', 'sub_id', 'sub_text'])
    result = nest(df, columns=['sub_id', 'sub_text'], key='items')
    expected_df = pd.DataFrame(columns=['id', 'text'])
    expected_df['items'] = []
    pd.testing.assert_frame_equal(result, expected_df)

def test_nest_with_single_group():
    df = pd.DataFrame({
        'id': [10, 10],
        'text': ['hello', 'hello'],
        'sub_id': [33, 33],
        'sub_text': ['test1', 'test2']
    })
    result = nest(df, columns=['sub_id', 'sub_text'], key='items')
    expected_data = {
        'id': [10],
        'text': ['hello'],
        'items': [pd.DataFrame({'sub_id': [33, 33], 'sub_text': ['test1', 'test2']})]
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)

def test_nest_with_multiple_groups():
    df = pd.DataFrame({
        'id': [10, 10, 16, 16],
        'text': ['hello', 'hello', 'world', 'world'],
        'sub_id': [33, 33, 20, 16],
        'sub_text': ['test1', 'test2', 'test3', 'test4']
    })
    result = nest(df, columns=['sub_id', 'sub_text'], key='items')
    expected_data = {
        'id': [10, 16],
        'text': ['hello', 'world'],
        'items': [
            pd.DataFrame({'sub_id': [33, 33], 'sub_text': ['test1', 'test2']}),
            pd.DataFrame({'sub_id': [20, 16], 'sub_text': ['test3', 'test4']})
        ]
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)

def test_nest_with_na_grouping_values():
    df = pd.DataFrame({
        'id': [10, 10, None, 16, None],
        'text': ['hello', 'hello', 'world', 'world', 'world'],
        'sub_id': [33, 33, 20, 16, 40],
        'sub_text': ['test1', 'test2', 'test3', 'test4', 'test5']
    })
    result = nest(df, columns=['sub_id', 'sub_text'], key='items')
    expected_df = pd.DataFrame({
        'id': [10, 16, None],
        'text': ['hello', 'world', 'world'],
        'items': [
            pd.DataFrame({'sub_id': [33, 33], 'sub_text': ['test1', 'test2']}),
            pd.DataFrame({'sub_id': [16], 'sub_text': ['test4']}),
            pd.DataFrame({'sub_id': [20, 40], 'sub_text': ['test3', 'test5']})
        ]
    })
    pd.testing.assert_frame_equal(result, expected_df)

def test_successive_nest_and_unnest_results_in_original_df():
    data = {
        'id': [10, 10, 16, 16],
        'text': ['hello', 'hello', 'world', 'world'],
        'sub_id': [33, 33, 20, 16],
        'sub_text': ['test1', 'test2', 'test3', 'test4']
    }
    df = pd.DataFrame(data)
    nested_df = nest(df, columns=['sub_id', 'sub_text'])
    unnested_df = unnest(nested_df)
    pd.testing.assert_frame_equal(df, unnested_df.reset_index(drop=True))

def test_nest_with_missing_columns_raises_error():
    data = {
        'id': [10, 10, 16, 16],
        'text': ['hello', 'hello', 'world', 'world'],
        'sub_id': [33, 33, 20, 16],
        'sub_text': ['test1', 'test2', 'test3', 'test4']
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="`columns` missing in df:"):
        nest(df, columns=['sub_id', 'missing_col'], key='items')

def test_nest_with_existing_key_column_raises_error():
    data = {
        'id': [10, 10, 16, 16],
        # This column name conflicts with the default key 'data'
        'data': [1, 2, 3, 4],
        'sub_id': [33, 33, 20, 16],
        'sub_text': ['test1', 'test2', 'test3', 'test4']
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="The key column 'data' already exists"):
        nest(df, columns=['sub_id', 'sub_text'])