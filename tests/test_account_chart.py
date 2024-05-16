"""
Unit tests for account charts accessor and mutator methods.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger

# Ensure there is no '2222' account on the remote system
def test_delete_account_non_existent():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(7777, allow_missing=True)
    assert 7777 not in cashctrl_ledger.account_chart().index

# Test adding a valid account
def test_add_account():
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 7777,
        'currency': 'CHF',
        'text': 'test create account',
        'vat_code': 'MwSt. 2.6%',
        'group': '/Anlagevermögen'
    }
    cashctrl_ledger.add_account(**new_account)
    updated_accounts= cashctrl_ledger.account_chart().reset_index()
    created_accounts = updated_accounts[
        ~updated_accounts.apply(tuple, 1).isin(initial_accounts.apply(tuple, 1))]

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['text'].item() == new_account['text']
    assert created_accounts['account'].item() == new_account['account']
    assert created_accounts['currency'].item() == new_account['currency']
    assert created_accounts['vat_code'].item() == '<values><de>MwSt. 2.6%</de><en>VAT 2.6%</en><fr>TVA 2.6%</fr><it>IVA 2.6%</it></values>'
    assert created_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test updating a account chart with valid inputs.
def test_update_account():
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        'account': 7777,
        'currency': 'CHF',
        'text': 'test create account updated',
        'vat_code': 'MwSt. 2.6%',
        'group': '/Anlagevermögen'
    }
    cashctrl_ledger.update_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    modified_accounts = updated_accounts[
        ~updated_accounts.apply(tuple, 1).isin(initial_accounts.apply(tuple, 1))]

    assert len(modified_accounts) == 1, "Expected exactly one updated row"
    assert modified_accounts['account'].item() == new_account['account']
    assert modified_accounts['text'].item() == new_account['text']
    assert modified_accounts['account'].item() == new_account['account']
    assert modified_accounts['currency'].item() == new_account['currency']
    assert modified_accounts['vat_code'].item() == '<values><de>MwSt. 2.6%</de><en>VAT 2.6%</en><fr>TVA 2.6%</fr><it>IVA 2.6%</it></values>'
    assert modified_accounts['group'].item() == new_account['group']+'/'+new_account['text']

# Test deleting an existent account.
def test_delete_account():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(account=7777)
    updated_accounts = cashctrl_ledger.account_chart()

    assert 7777 not in updated_accounts.index

# Test deleting a non existent account should raise an error.
def test_delete_non_existent_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_account(7777)

# Test updating a non existent account should raise an error.
def test_update_non_existent_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=7777, currency='CHF',
            text='test create account', vat_code='MwSt. 2.6%', group='/Anlagevermögen'
        )

# Test adding an account with not valid vat code should raise an error.
def test_update_non_existent_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=7777, currency='',
            text='test create account', vat_code='MwSt. 2.6%', group='/Anlagevermögen'
        )


# Test updating an account with not valid vat code should raise an error.
def test_update_account_with_not_valid_currency_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(account=7777, currency='',
            text='test create account', vat_code='MwSt. 2.6%', group='/Anlagevermögen'
        )
