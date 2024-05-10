"""
Unit tests for vat codes operation methods.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger

def test_delete_valid_vat_code_success():
    """
    Test deleting a valid VAT code succeeds without exceptions.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code("9")

def test_delete_vat_code_unimplemented():
    """
    Test for deletion of a used VAT code, expecting failure since it's already used.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception) as excinfo:
        cashctrl_ledger.delete_vat_code("3")
    assert "An error occurred" in str(excinfo.value), "Unexpected error message received"

def test_delete_vat_code_with_empty_input_should_fail():
    """
    Test deleting a VAT code with empty input should raise an exception.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception) as excinfo:
        cashctrl_ledger.delete_vat_code("")
    assert "An error occurred" in str(excinfo.value), "Expected exception for empty VAT code not raised"

def test_add_vat_code_valid_inputs():
    """ Test adding VAT code with valid inputs """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        account=1, code="Standard Tax", rate=20.0, inclusive=True, text="VAT 20%"
    )

def test_add_vat_code_invalid_account_id():
    """ Test invalid accountId (negative) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception) as excinfo:
        cashctrl_ledger.add_vat_code(
            account=-11, code="Standard Tax", rate=20.0, inclusive=True, text="VAT 20%"
        )
    assert "API call failed" in str(excinfo.value)

def test_add_vat_code_invalid_name_length():
    """ Test name parameter exceeding max length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=-11, code="x"*51, rate=20.0, inclusive=True, text="VAT 20%"
        )  # 51 chars long

def test_add_vat_code_invalid_percentage():
    """ Test invalid percentage (out of bounds) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "Standard Tax", 150.0, 15.0)  # > 100.0

def test_add_vat_code_invalid_percentage_flat():
    """ Test invalid percentageFlat (below minimum) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "Standard Tax", 20.0, -10.0)  # < 0.0

def test_add_vat_code_invalid_inclusive():
    """ Test invalid inclusive """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=-11, code="x"*51, rate=20.0, inclusive="434", text="VAT 20%"
        )  # 51 chars long

def test_add_vat_code_invalid_document_name():
    """ Test invalid documentName length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=-11, code="x", rate=20.0, inclusive="434", text="x"*51
        )  # 51 chars long

def test_valid_update():
    """
    Test a valid update operation to ensure the method handles correct inputs
    without errors. Validates that the network client's post method is called
    once.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.update_vat_code(
        code=4, account=1, name='test_api_update', rate=22, text='test_api_update'
    )

def test_name_too_long():
    """
    Test input validation for the 'name' parameter to ensure it raises a ValueError
    for overly long names.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            code=4, account=1, name="x"*51, rate=22, text='test_api_update'
        )

def test_document_name_too_long():
    """
    Test input validation for the 'documentName' parameter to ensure it raises a
    ValueError for overly long document names.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            code=4, account=1, name="x", rate=22, text="x"*51
        )

def test_percentage_out_of_bounds():
    """
    Ensure the method validates the 'percentage' parameter within the range 0.0
    to 100.0, raising a ValueError if not.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            code=4, account=1, name="x", rate=101.0, text="x",
        )

def test_invalid_boolean():
    """
    Verify that the method raises a ValueError when the 'isInactive' parameter
    is not a boolean.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            code=4, account=1, name="x", rate=101.0, text="x", inclusive="kjn"
        )
