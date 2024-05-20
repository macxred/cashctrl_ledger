"""
Unit tests for account chart accessor and mutator methods.
"""

import pytest
import requests
import pandas as pd
from cashctrl_ledger import CashCtrlLedger

# Ensure there is no '7777' account on the remote system
def test_delete_account_non_existent():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(7777, allow_missing=True)
    assert 7777 not in cashctrl_ledger.account_chart().index

# Test adding an account
def test_add_account():
    cashctrl_ledger = CashCtrlLedger()
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
    assert created_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test adding an account without VAT
def test_add_account():
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 7777,
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
    assert created_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test updating an account.
def test_update_account():
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 7777,
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
    assert modified_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test updating an account without VAT code.
def test_update_account_without_vat_not_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 1140,
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
    assert created_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test deleting an account.
def test_delete_account():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(account=7777)
    updated_accounts = cashctrl_ledger.account_chart()
    assert 7777 not in updated_accounts.index

# Test deleting a non existent account should raise an error.
def test_delete_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_account(7777)

# Test adding an already existing account should raise an error.
def test_add_pre_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(requests.exceptions.RequestException):
        cashctrl_ledger.add_account(account=1045, currency='EUR',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test adding an account with invalid currency should raise an error.
def test_add_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=1146, currency='',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test adding an account with invalid VAT code should raise an error.
def test_add_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1147, currency='USD',
            text='test account', vat_code='Non-Existing Tax Code', group='/Anlagevermögen'
        )

# Test adding an account with invalid group should raise an error.
def test_add_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(account=1148, currency='USD',
            text='test account', vat_code='MwSt. 2.6%', group='/ABC'
        )

# Test updating a non existing account should raise an error.
def test_update_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=8888, currency='CHF',
            text='test account', vat_code='MwSt. 2.6%', group='/Anlagevermögen'
        )

# Test updating an account with invalid currency should raise an error.
def test_update_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1140, currency='not-existing-currency',
            text='test account', vat_code=None, group='/Anlagevermögen'
        )

# Test updating an account with invalid VAT code should raise an error.
def test_update_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1140, currency='USD',
            text='test create account', vat_code='Non-Existing Tax Code', group='/Anlagevermögen'
        )

# Test updating an account with invalid group should raise an error.
def test_update_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=1140, currency='USD',
            text='test create account', vat_code='MwSt. 2.6%', group='/ABC'
        )
