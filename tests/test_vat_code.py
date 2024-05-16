"""
Unit tests for vat codes accessor, mutator, and mirror methods.
"""

import pytest
from io import StringIO
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

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
    target_csv = """
    id,account,rate,inclusive,text
    OutStd,2200,0.077,True,VAT at the regular 7.7% rate on goods or services
    OutRed,2200,0.025,True,VAT at the reduced 2.5% rate on goods or services
    OutAcc,2200,0.038,True,XXXXX
    OutStdEx,2200,0.077,False,VAT at the regular 7.7% rate on goods or services
    InStd,1170,0.077,True,Input Tax (Vorsteuer) at the regular 7.7% rate on
    InRed,1170,0.025,True,Input Tax (Vorsteuer) at the reduced 2.5% rate on
    InAcc,1170,0.038,True,YYYYY
    """

    target_state = pd.read_csv(StringIO(target_csv), skipinitialspace=True)
    standardized_target_state = StandaloneLedger.standardize_vat_codes(target_state)
    cashctrl_ledger = CashCtrlLedger()
    initial_vat_codes = cashctrl_ledger.vat_codes()

    cashctrl_ledger.mirror_vat_codes(standardized_target_state, delete=False)
    mirrored_vat_codes = cashctrl_ledger.vat_codes()
    reset_standardized_target_state = standardized_target_state.reset_index(drop=False)
    reset_mirrored_vat_codes = mirrored_vat_codes.reset_index(drop=False)
    merged_vat_codes = reset_standardized_target_state.merge(
        reset_mirrored_vat_codes, how='left', indicator=True
    )
    missing_codes_after_mirroring = merged_vat_codes[
        merged_vat_codes['_merge'] == 'left_only'
    ]
    assert missing_codes_after_mirroring.empty, 'Mirroring error: Some target VAT codes were not mirrored'

    # Mirroring VAT codes with deletion
    cashctrl_ledger.mirror_vat_codes(standardized_target_state, delete=True)
    mirrored_vat_codes_after_deletion = cashctrl_ledger.vat_codes()
    reset_mirrored_vat_codes_after_deletion = mirrored_vat_codes_after_deletion.reset_index(drop=False)
    merged_vat_codes_after_deletion = reset_standardized_target_state.merge(
        reset_mirrored_vat_codes_after_deletion, how='left', indicator=True
    )
    missing_codes_after_deletion = merged_vat_codes_after_deletion[
        merged_vat_codes_after_deletion['_merge'] == 'left_only'
    ]
    assert len(standardized_target_state) == len(mirrored_vat_codes_after_deletion), (
        'Mirroring error: The number of VAT codes after deletion does not match the target'
    )
    assert missing_codes_after_deletion.empty, 'Mirroring error: Some target VAT codes were not mirrored after deletion'

    cashctrl_ledger.mirror_vat_codes(initial_vat_codes, delete=True)
    restored_initial_vat_codes = cashctrl_ledger.vat_codes()
    reset_initial_vat_codes = initial_vat_codes.reset_index(drop=False)
    reset_restored_initial_vat_codes = restored_initial_vat_codes.reset_index(drop=False)
    merged_initial_vat_codes = reset_initial_vat_codes.merge(
        reset_restored_initial_vat_codes, how='left', indicator=True
    )
    missing_initial_codes = merged_initial_vat_codes[
        merged_initial_vat_codes['_merge'] == 'left_only'
    ]
    assert len(initial_vat_codes) == len(restored_initial_vat_codes), (
        'Restoration error: The number of initial VAT codes after restoration does not match the original'
    )
    assert missing_initial_codes.empty, 'Restoration error: Some initial VAT codes were not restored'

