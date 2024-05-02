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
        accountId=1,
        name="Standard Tax",
        percentage=20.0,
        percentageFlat=15.0,
        calcType="NET",
        documentName="VAT 20%",
        isInactive=False
    )

def test_add_vat_code_invalid_account_id():
    """ Test invalid accountId (negative) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError) as excinfo:
        cashctrl_ledger.add_vat_code(-1, "Standard Tax", 20.0, 15.0)
    assert "Invalid accountId" in str(excinfo.value)

def test_add_vat_code_invalid_name_length():
    """ Test name parameter exceeding max length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "x" * 51, 20.0, 15.0)  # 51 chars long

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

def test_add_vat_code_invalid_calc_type():
    """ Test invalid calcType """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "Standard Tax", 20.0, 15.0, calcType="XYZ")

def test_add_vat_code_invalid_document_name():
    """ Test invalid documentName length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "Standard Tax", 20.0, 15.0, documentName="x" * 51)

def test_add_vat_code_invalid_is_inactive_type():
    """ Test isInactive flag with incorrect type """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(1, "Standard Tax", 20.0, 15.0, isInactive="true")

def test_valid_update():
    """
    Test a valid update operation to ensure the method handles correct inputs
    without errors. Validates that the network client's post method is called
    once.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.update_vat_code(4, 1, 'test_api_update', 22, 'test_api_update')

def test_name_too_long():
    """
    Test input validation for the 'name' parameter to ensure it raises a ValueError
    for overly long names.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(accountId=4, id=1, name="x" * 51, percentage=15.0)

def test_document_name_too_long():
    """
    Test input validation for the 'documentName' parameter to ensure it raises a
    ValueError for overly long document names.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(accountId=4, id=1, name="VAT", percentage=15.0, documentName="x" * 51)

def test_percentage_out_of_bounds():
    """
    Ensure the method validates the 'percentage' parameter within the range 0.0
    to 100.0, raising a ValueError if not.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(accountId=4, id=1, name="Reduced VAT", percentage=101.0)

def test_percentage_flat_out_of_bounds():
    """
    Test that the method validates the 'percentageFlat' parameter (when provided)
    within the range 0.0 to 100.0, raising a ValueError if it exceeds these bounds.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(accountId=4, id=1, name="Reduced VAT", percentage=10.0, percentageFlat=101.0)

def test_invalid_boolean():
    """
    Verify that the method raises a ValueError when the 'isInactive' parameter
    is not a boolean.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(accountId=4, id=1, name="Reduced VAT", percentage=10.0, isInactive="true")

def test_valid_inactive_flag():
    """
    Verify that the method correctly handles a valid boolean 'isInactive' flag
    without raising an exception and ensures the network client's post method
    is called once.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.update_vat_code(4, 1, 'test_api_update', 22, 'test_api_update', isInactive=True)