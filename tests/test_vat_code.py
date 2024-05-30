"""
Unit tests for vat codes accessor, mutator, and mirror methods.
"""

import pytest
from io import StringIO
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

# Ensure there is no 'TestCode' vat_code on the remote account
@pytest.mark.skip()
def test_delete_vat_non_existent():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code("TestCode", allow_missing=True)
    assert "TestCode" not in cashctrl_ledger.vat_codes().index

# Test adding a valid vat_code
@pytest.mark.skip()
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
    outer_join = pd.merge(initial_vat_codes, updated_vat_codes, how='outer', indicator=True)
    created_vat_codes = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(created_vat_codes) == 1, "Expected exactly one row to be added"
    assert created_vat_codes['id'].item() == new_vat_code['code']
    assert created_vat_codes['text'].item() == new_vat_code['text']
    assert created_vat_codes['account'].item() == new_vat_code['account']
    assert created_vat_codes['rate'].item() == new_vat_code['rate']
    assert created_vat_codes['inclusive'].item() == new_vat_code['inclusive']

# Test updating a VAT code with valid inputs.
@pytest.mark.skip()
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
    outer_join = pd.merge(initial_vat_codes, updated_vat_codes, how='outer', indicator=True)
    modified_vat_codes = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    assert len(modified_vat_codes) == 1, "Expected exactly one updated row"
    assert modified_vat_codes['id'].item() == new_vat_code['code']
    assert modified_vat_codes['text'].item() == new_vat_code['text']
    assert modified_vat_codes['account'].item() == new_vat_code['account']
    assert modified_vat_codes['rate'].item() == new_vat_code['rate']
    assert modified_vat_codes['inclusive'].item() == new_vat_code['inclusive']

# Test deleting an existent VAT code.
@pytest.mark.skip()
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
    cashctrl_ledger.delete_account(8888, allow_missing=True)
    assert 8888 not in cashctrl_ledger.account_chart().index
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(code="TestCode", text='VAT 20%',
            account=8888, rate=0.02, inclusive=True
        )

# Test updating a VAT code with non existent account should raise an error.
def test_update_vat_with_not_valid_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(8888, allow_missing=True)
    assert 8888 not in cashctrl_ledger.account_chart().index
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(code="TestCode", text='VAT 20%',
            account=8888, rate=0.02, inclusive=True
        )

# Tests the mirroring functionality of VAT codes.
@pytest.mark.skip()
def test_mirror_vat_codes():
    target_csv = """
    id,account,rate,inclusive,text
    OutStd,2200,0.038,True,VAT at the regular 7.7% rate on goods or services
    OutRed,2200,0.025,True,VAT at the reduced 2.5% rate on goods or services
    OutAcc,2200,0.038,True,XXXXX
    OutStdEx,2200,0.077,False,VAT at the regular 7.7% rate on goods or services
    InStd,1170,0.077,True,Input Tax (Vorsteuer) at the regular 7.7% rate on
    InRed,1170,0.025,True,Input Tax (Vorsteuer) at the reduced 2.5% rate on
    InAcc,1170,0.038,True,YYYYY
    """
    target_df = pd.read_csv(StringIO(target_csv), skipinitialspace=True)
    standardized_df = StandaloneLedger.standardize_vat_codes(target_df).reset_index()
    cashctrl_ledger = CashCtrlLedger()

    # Save initial VAT codes
    initial_vat_codes = cashctrl_ledger.vat_codes().reset_index()

    # Mirror test vat codes onto server with delete=False
    cashctrl_ledger.mirror_vat_codes(target_df, delete=False)
    mirrored_df = cashctrl_ledger.vat_codes().reset_index()
    m = standardized_df.merge(mirrored_df, how='left', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target VAT codes were not mirrored'
        )

    # Mirror target vat codes onto server with delete=True
    cashctrl_ledger.mirror_vat_codes(target_df, delete=True)
    mirrored_df = cashctrl_ledger.vat_codes().reset_index()
    m = standardized_df.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target VAT codes were not mirrored'
        )

    # Reshuffle target data randomly
    target_df = target_df.sample(frac=1).reset_index(drop=True)

    # Mirror target vat codes onto server with updating
    target_df.loc[target_df['id'] == 'OutStdEx', 'rate'] = 0.9
    cashctrl_ledger.mirror_vat_codes(target_df, delete=True)
    mirrored_df = cashctrl_ledger.vat_codes().reset_index()
    m = target_df.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target VAT codes were not mirrored'
        )

    # Mirror initial vat codes onto server with delete=True to restore original state
    cashctrl_ledger.mirror_vat_codes(initial_vat_codes, delete=True)
    mirrored_df = cashctrl_ledger.vat_codes().reset_index()
    m = initial_vat_codes.merge(mirrored_df, how='outer', indicator=True)
    assert (m['_merge'] == 'both').all(), (
            'Mirroring error: Some target VAT codes were not mirrored'
        )

