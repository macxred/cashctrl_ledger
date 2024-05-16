"""
Unit tests for vat codes accessor, mutator, and mirror methods.
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
            account=2200, rate=0.02, inclusive=True
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

# Tests the mirroring functionality of VAT codes.
def test_mirror_vat_codes():
    cashctrl_ledger = CashCtrlLedger()
    initial_vat_codes = cashctrl_ledger.vat_codes()
    updated_vat_codes = initial_vat_codes.copy()
    updated_vat_codes.loc['test_mirror'] = ["VAT 20%", 2200, 0.02000, True, '1900-01-01', None]
    updated_vat_codes.at[updated_vat_codes.index[0], 'text'] = "test_vat_name"

    cashctrl_ledger.mirror_vat_codes(updated_vat_codes)
    mirrored_vat_codes = cashctrl_ledger.vat_codes()
    updated_vat_codes_reset_index = updated_vat_codes.astype(str).reset_index(drop=True)
    mirrored_vat_codes_str_reset_index  = mirrored_vat_codes.astype(str).reset_index(drop=True)
    assert updated_vat_codes_reset_index.equals(mirrored_vat_codes_str_reset_index), "Mirroring failed: VAT codes do not match expected state"

    cashctrl_ledger.mirror_vat_codes(target_state=initial_vat_codes)
    rolled_back_vat_codes = cashctrl_ledger.vat_codes()
    assert rolled_back_vat_codes.equals(initial_vat_codes), "Rollback failed: VAT codes do not match initial state"
