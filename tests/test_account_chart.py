"""
Unit tests for account chart accessor and mutator methods.
"""

import pytest
import requests
import pandas as pd
from cashctrl_ledger import CashCtrlLedger

# Fixture that creates VAT code with expected code on the start
# of the test and deletes that VAT code at the end of test
@pytest.fixture(scope="module")
def add_and_delete_vat_code():
    # Creates VAT code
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        code="TestCodeAccounts",
        text='VAT 2%',
        account=2200,
        rate=0.02,
        inclusive=True,
    )

    yield

    # Deletes VAT code
    cashctrl_ledger.delete_vat_code(code="TestCodeAccounts")

def test_account_mutators(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()

    # Ensure there is no account '1145' or '1146' on the remote system
    cashctrl_ledger.delete_account(1145, allow_missing=True)
    cashctrl_ledger.delete_account(1146, allow_missing=True)
    account_chart = cashctrl_ledger.account_chart()
    assert 1145 not in account_chart.index
    assert 1146 not in account_chart.index

    # Test adding an account
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1145,
        'currency': 'CHF',
        'text': 'test create account',
        'vat_code': 'TestCodeAccounts',
        'group': '/Assets/Anlagevermögen'
    }
    cashctrl_ledger.add_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how='outer', indicator=True)
    created_accounts = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['text'].item() == new_account['text']
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['currency'].item() == new_account['currency']
    assert created_accounts['vat_code'].item() == 'TestCodeAccounts'
    assert created_accounts['group'].item() == new_account['group']

    # Test adding an account without VAT
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1146,
        'currency': 'CHF',
        'text': 'test create account',
        'vat_code': None,
        'group': '/Assets/Anlagevermögen'
    }
    cashctrl_ledger.add_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how='outer', indicator=True)
    created_accounts = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['text'].item() == new_account['text']
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['currency'].item() == new_account['currency']
    assert pd.isna(created_accounts['vat_code'].item())
    assert created_accounts['group'].item() == new_account['group']

    # Test updating an account.
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1146,
        'currency': 'CHF',
        'text': 'test update account',
        'vat_code': 'TestCodeAccounts',
        'group': '/Assets/Anlagevermögen'
    }
    cashctrl_ledger.update_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how='outer', indicator=True)
    modified_accounts = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(modified_accounts) == 1, "Expected exactly one updated row"
    assert modified_accounts['account'].item() == new_account['account']
    assert modified_accounts['text'].item() == new_account['text']
    assert modified_accounts['account'].item() == new_account['account']
    assert modified_accounts['currency'].item() == new_account['currency']
    assert modified_accounts['vat_code'].item() == 'TestCodeAccounts'
    assert modified_accounts['group'].item() == new_account['group']

    # Test updating an account without VAT code.
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1145,
        'currency': 'USD',
        'text': 'test update account without VAT',
        'vat_code': None,
        'group': '/Assets/Anlagevermögen'
    }
    cashctrl_ledger.update_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how='outer', indicator=True)
    created_accounts = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['text'].item() == new_account['text']
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['currency'].item() == new_account['currency']
    assert pd.isna(created_accounts['vat_code'].item())
    assert created_accounts['group'].item() == new_account['group']

    # Test deleting the accounts added above.
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(account=1145)
    cashctrl_ledger.delete_account(account=1146)
    updated_accounts = cashctrl_ledger.account_chart()
    assert 1145 not in updated_accounts.index
    assert 1146 not in updated_accounts.index

# Test deleting a non existent account should raise an error.
def test_delete_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    # Ensure there is no account '1141'
    cashctrl_ledger.delete_account(1141, allow_missing=True)
    assert 1141 not in cashctrl_ledger.account_chart().index
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_account(1141)

# Test adding an already existing account should raise an error.
def test_add_pre_existing_account_raise_error(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(requests.exceptions.RequestException):
        cashctrl_ledger.add_account(account=1200, currency='EUR',
            text='test account', vat_code='TestCodeAccounts', group='/Assets/Anlagevermögen'
        )

# Test adding an account with invalid currency should raise an error.
def test_add_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=1142, currency='',
            text='test account', vat_code=None, group='/Assets/Anlagevermögen'
        )

# Test adding an account with invalid VAT code should raise an error.
def test_add_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1143, currency='USD',
            text='test account', vat_code='Non-Existing Tax Code', group='/Assets/Anlagevermögen'
        )

# Test adding an account with invalid group should raise an error.
def test_add_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=999999, currency='USD',
            text='test account', vat_code='MwSt. 2.6%', group='/Assets/Anlagevermögen/ABC'
        )

# Test updating a non existing account should raise an error.
def test_update_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1147, currency='CHF',
            text='test account', vat_code='MwSt. 2.6%', group='/Assets/Anlagevermögen'
        )

# Test updating an account with invalid currency should raise an error.
def test_update_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1148, currency='not-existing-currency',
            text='test account', vat_code=None, group='/Assets/Anlagevermögen'
        )

# Test updating an account with invalid VAT code should raise an error.
def test_update_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1149, currency='USD',
            text='test create account', vat_code='Non-Existing Tax Code', group='/Assets/Anlagevermögen'
        )

# Test updating an account with invalid group should raise an error.
def test_update_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1149, currency='USD',
            text='test create account', vat_code='MwSt. 2.6%', group='/ABC'
        )

# Tests the mirroring functionality of accounts.
def test_mirror_accounts(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()

    account = pd.DataFrame({
        "account": [1, 2],
        "currency": ["CHF", "EUR"],
        "text": ["Test Account 1", "Test Account 2"],
        "vat_code": ["TestCodeAccounts", None],
        "group": ["/Assets", "/Assets/Anlagevermögen/xyz"],
    })
    target_df = pd.concat([account, initial_accounts])

    # Mirror test accounts onto server with delete=False
    cashctrl_ledger.mirror_account_chart(target_df, delete=False)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how='left', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

    # Mirror target accounts onto server with delete=True
    cashctrl_ledger.mirror_account_chart(target_df, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

    # Reshuffle target data randomly
    target_df = target_df.sample(frac=1).reset_index(drop=True)

    # Mirror target accounts onto server with updating
    target_df.loc[target_df['account'] == 2, 'text'] = "New_Test_Text"
    cashctrl_ledger.mirror_account_chart(target_df, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

    # Mirror initial accounts onto server with delete=True to restore original state
    cashctrl_ledger.mirror_account_chart(initial_accounts, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = initial_accounts.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

# Tests the mirroring functionality of accounts with root category.
def test_mirror_accounts_with_root_category(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    expected = initial_accounts[~initial_accounts['group'].str.startswith('/Balance')]
    initial_categories = cashctrl_ledger._client.list_categories('account', include_system=True)
    categories_dict = initial_categories.set_index('path')['number'].to_dict()

    assert not (initial_accounts[initial_accounts['group'].str.startswith('/Balance')]).empty, (
        'There are no remote accounts placed in /Balance node'
    )

    # Mirror accounts with discarded accounts that have /Balance group
    # onto server with delete=True should delete all leaf categories
    # and leave root category
    cashctrl_ledger.mirror_account_chart(expected.copy(), delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    updated_categories = cashctrl_ledger._client.list_categories('account', include_system=True)
    updated_categories_dict = updated_categories.set_index('path')['number'].to_dict()
    difference = set(categories_dict.keys()) - set(updated_categories_dict.keys())
    initial_sub_nodes = [key for key in difference if key.startswith('/Balance') and key != '/Balance']

    assert (mirrored_df[mirrored_df['group'].str.startswith('/Balance')]).empty, (
        'Accounts placed in /Balance node were not deleted'
    )
    assert len(initial_sub_nodes) > 0, 'Sub-nodes were not deleted'
    assert updated_categories_dict['/Balance'] == categories_dict['/Balance'], (
        'Root node /Balance was deleted'
    )

    # Restore initial state
    cashctrl_ledger.mirror_account_chart(initial_accounts.copy(), delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    updated_categories = cashctrl_ledger._client.list_categories('account', include_system=True)
    updated_categories_dict = initial_categories.set_index('path')['number'].to_dict()
    pd.testing.assert_frame_equal(initial_accounts, mirrored_df)
    assert updated_categories_dict == categories_dict, (
        'Some categories were not restored'
    )