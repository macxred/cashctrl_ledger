"""
Unit tests for vat codes operation methods.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger

def test_delete_vat_non_existent():
    """
    Test deleting a non existent VAT code.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code("TestCode", allow_missing=True)

def test_add_vat_code():
    """ Test adding a VAT code with valid inputs. """
    cashctrl_ledger = CashCtrlLedger()
    initial_vat_codes = cashctrl_ledger.vat_codes()
    new_vat_code = {
        'code': "TestCode",
        'text': 'VAT 20%',
        'account': 1,
        'rate': 0.02,
        'inclusive': True,
    }
    cashctrl_ledger.add_vat_code(**new_vat_code)

    updated_vat_codes = cashctrl_ledger.vat_codes()
    created_rows = updated_vat_codes[~updated_vat_codes.index.isin(initial_vat_codes.index)]
    created_row = created_rows.iloc[0]

    assert all([
        created_rows.index[0] == new_vat_code['code'],
        created_row['text'] == new_vat_code['text'],
        created_row['account'] == new_vat_code['account'],
        created_row['rate'] == new_vat_code['rate'],
        created_row['inclusive'] == new_vat_code['inclusive']
    ])

def test_update_vat_code():
    """ Test updating a VAT code with valid inputs. """
    cashctrl_ledger = CashCtrlLedger()
    new_vat_code = {
        'code': "TestCode",
        'text': 'VAT 20%',
        'account': 1,
        'rate': 0.03,
        'inclusive': True,
    }
    cashctrl_ledger.update_vat_code(**new_vat_code)

    updated_vat_codes = cashctrl_ledger.vat_codes()
    updated_row = updated_vat_codes.loc[new_vat_code['code']]

    assert all([
        updated_row['text'] == new_vat_code['text'],
        updated_row['account'] == new_vat_code['account'],
        updated_row['rate'] == new_vat_code['rate'],
        updated_row['inclusive'] == new_vat_code['inclusive']
    ])

def test_delete_vat_code():
    """ Test deleting an existent VAT code. """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code(code='TestCode')
    updated_vat_codes = cashctrl_ledger.vat_codes()
    deleted = 'TestCode' not in updated_vat_codes.index

    assert deleted

def test_delete_non_existent_vat_raise_error():
    """
    Test deleting a non existent VAT code should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_vat_code("TestCode")

def test_update_non_existent_vat_raise_error():
    """
    Test updating a non existent VAT code should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.update_vat_code(code="TestCode", text='VAT 20%',
            account=1, rate=0.02, inclusive=True
        )

def test_add_vat_with_not_valid_account_raise_error():
    """
    Test adding a VAT code with non existent account should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.add_vat_code(code="TestCode", text='VAT 20%',
            account=7777, rate=0.02, inclusive=True
        )

def test_update_vat_with_not_valid_account_raise_error():
    """
    Test updating a VAT code with non existent account should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.update_vat_code(code="TestCode", text='VAT 20%',
            account=7777, rate=0.02, inclusive=True
        )


def test_delete_vat_with_not_valid_input_raise_error():
    """
    Test deleting a VAT code with not valid inputs should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.delete_vat_code(code="")

def test_add_vat_with_not_valid_input_raise_error():
    """
    Test adding a VAT code with not valid inputs should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.add_vat_code()

def test_update_vat_with_not_valid_input_raise_error():
    """
    Test updating a VAT code with not valid inputs should raise an error.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.update_vat_code()
