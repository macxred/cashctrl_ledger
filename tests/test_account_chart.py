"""
Unit tests for account chart accessor and mutator methods.
"""

import pytest
import requests
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

def test_account_mutators():
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
        'vat_code': 'MwSt. 2.6%',
        'group': '/Anlagevermögen'
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
    assert created_accounts['vat_code'].item() == '<values><de>MwSt. 2.6%</de><en>VAT 2.6%</en><fr>TVA 2.6%</fr><it>IVA 2.6%</it></values>'
    assert created_accounts['group'].item() == new_account['group']

    # Test adding an account without VAT
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1146,
        'currency': 'CHF',
        'text': 'test create account',
        'vat_code': None,
        'group': '/Anlagevermögen'
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
        'vat_code': 'MwSt. 2.6%',
        'group': '/Anlagevermögen'
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
    assert modified_accounts['vat_code'].item() == '<values><de>MwSt. 2.6%</de><en>VAT 2.6%</en><fr>TVA 2.6%</fr><it>IVA 2.6%</it></values>'
    assert modified_accounts['group'].item() == new_account['group']

    # Test updating an account without VAT code.
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1145,
        'currency': 'USD',
        'text': 'test update account without VAT',
        'vat_code': None,
        'group': '/Anlagevermögen'
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
def test_add_pre_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(requests.exceptions.RequestException):
        cashctrl_ledger.add_account(account=1000, currency='EUR',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test adding an account with invalid currency should raise an error.
def test_add_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=1142, currency='',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test adding an account with invalid VAT code should raise an error.
def test_add_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1143, currency='USD',
            text='test account', vat_code='Non-Existing Tax Code', group='/Anlagevermögen'
        )

# Test adding an account with invalid group should raise an error.
def test_add_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=1144, currency='USD',
            text='test account', vat_code='MwSt. 2.6%', group='/ABC'
        )

# Test updating a non existing account should raise an error.
def test_update_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1147, currency='CHF',
            text='test account', vat_code='MwSt. 2.6%', group='/Anlagevermögen'
        )

# Test updating an account with invalid currency should raise an error.
def test_update_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1148, currency='not-existing-currency',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test updating an account with invalid VAT code should raise an error.
def test_update_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1149, currency='USD',
            text='test create account', vat_code='Non-Existing Tax Code', group='/Anlagevermögen'
        )

# Test updating an account with invalid group should raise an error.
def test_update_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1149, currency='USD',
            text='test create account', vat_code='MwSt. 2.6%', group='/ABC'
        )

# Tests the mirroring functionality of accounts.
def test_mirror_accounts():
    target_df = (pd.read_csv('tests/initial_accounts.csv', skipinitialspace=True))
    standardized_df = StandaloneLedger.standardize_account_chart(target_df).reset_index()
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        code="TestCodeAccounts",
        text='VAT 2%',
        account=2200,
        rate=0.02,
        inclusive=True,
    )

    # Save initial accounts
    initial_accounts = cashctrl_ledger.account_chart().reset_index()

    # Mirror test accounts onto server with delete=False
    cashctrl_ledger.mirror_account_chart(target_df, delete=False)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = standardized_df.merge(mirrored_df, how='left', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

    # Mirror target accounts onto server with delete=True
    cashctrl_ledger.mirror_account_chart(target_df, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = standardized_df.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target accounts were not mirrored'
        )

    # Updating account that has VAT code to avoid error
    target_df.loc[target_df.index[0], 'text'] = "New_Test_Text"

    # Reshuffle target data randomly
    target_df = target_df.sample(frac=1).reset_index(drop=True)

    # Mirror target accounts onto server with updating
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

    cashctrl_ledger.delete_vat_code(code="TestCodeAccounts")
