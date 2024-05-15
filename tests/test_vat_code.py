"""
Unit tests for vat codes operation methods.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger

# Ensure there is no 'TestCode' vat_code on the remote account
def test_delete_vat_non_existent():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code("TestCode", allow_missing=True)
    assert "TestCode" not in cashctrl_ledger.vat_codes().index

# Test adding a valid vat_code
def test_add_vat_code():
    cashctrl_ledger = CashCtrlLedger()
    initial_vat_codes = cashctrl_ledger.vat_codes().reset_index()
    new_vat_code = {
        'code': "TestCode",
        'text': 'VAT 2%',
        'account': 2200,
        'rate': 0.02,
        'inclusive': True,
    }
    cashctrl_ledger.add_vat_code(**new_vat_code)
    updated_vat_codes = cashctrl_ledger.vat_codes().reset_index()
    created_vat_codes = updated_vat_codes[
        ~updated_vat_codes.apply(tuple, 1).isin(initial_vat_codes.apply(tuple, 1))]

    assert len(created_vat_codes) == 1, "Expected exactly one row to be added"
    assert created_vat_codes['id'].item() == new_vat_code['code']
    assert created_vat_codes['text'].item() == new_vat_code['text']
    assert created_vat_codes['account'].item() == new_vat_code['account']
    assert created_vat_codes['rate'].item() == new_vat_code['rate']
    assert created_vat_codes['inclusive'].item() == new_vat_code['inclusive']

# Test updating a VAT code with valid inputs.
def test_update_vat_code():
    cashctrl_ledger = CashCtrlLedger()
    initial_vat_codes = cashctrl_ledger.vat_codes().reset_index()
    new_vat_code = {
        'code': "TestCode",
        'text': 'VAT 20%',
        'account': 2000,
        'rate': 0.20,
        'inclusive': True,
    }
    cashctrl_ledger.update_vat_code(**new_vat_code)
    updated_vat_codes = cashctrl_ledger.vat_codes().reset_index()
    modified_vat_codes = updated_vat_codes[
        ~updated_vat_codes.apply(tuple, 1).isin(initial_vat_codes.apply(tuple, 1))]

    assert len(modified_vat_codes) == 1, "Expected exactly one updated row"
    assert modified_vat_codes['id'].item() == new_vat_code['code']
    assert modified_vat_codes['text'].item() == new_vat_code['text']
    assert modified_vat_codes['account'].item() == new_vat_code['account']
    assert modified_vat_codes['rate'].item() == new_vat_code['rate']
    assert modified_vat_codes['inclusive'].item() == new_vat_code['inclusive']

# Test deleting an existent VAT code.
def test_delete_vat_code():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code(code='TestCode')
    updated_vat_codes = cashctrl_ledger.vat_codes()

    assert 'TestCode' not in updated_vat_codes.index

# Test deleting a non existent VAT code should raise an error.
def test_delete_non_existent_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_vat_code("TestCode")

# Test updating a non existent VAT code should raise an error.
def test_update_non_existent_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(code="TestCode", text='VAT 20%',
            account=1, rate=0.02, inclusive=True
        )

# Test adding a VAT code with non existent account should raise an error.
def test_add_vat_with_not_valid_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(code="TestCode", text='VAT 20%',
            account=7777, rate=0.02, inclusive=True
        )

# Test updating a VAT code with non existent account should raise an error.
def test_update_vat_with_not_valid_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(code="TestCode", text='VAT 20%',
            account=7777, rate=0.02, inclusive=True
        )

# Test deleting a VAT code with not valid inputs should raise an error.
def test_delete_vat_with_not_valid_input_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_vat_code(code="")
   
# Test adding a VAT code with not valid inputs should raise an error.
def test_add_vat_with_not_valid_input_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(code="", text='',
            account=7777, rate='kkk', inclusive=3
        )

# Test updating a VAT code with not valid inputs should raise an error.
def test_update_vat_with_not_valid_input_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(code="", text='',
            account=7777, rate='kkk', inclusive=3
        )
