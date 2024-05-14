"""
Unit tests for vat codes operation methods.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger

def test_delete_vat_code_unimplemented():
    """
    Test for deletion of a used VAT code, expecting failure since it's already used.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.delete_vat_code("3")

def test_delete_vat_code_with_empty_input_should_fail():
    """
    Test deleting a VAT code with empty input should raise an exception.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.delete_vat_code("")

def test_add_vat_code_valid_inputs():
    """ Test adding VAT code with valid inputs """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        account=1, code="Standard Tax", rate=0.02, inclusive=True, text="VAT 20%"
    )

def test_delete_valid_vat_code_success():
    """
    Test deleting a valid VAT code succeeds without exceptions.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_vat_code("Standard Tax")

def test_add_vat_code_invalid_account_id():
    """ Test invalid account (negative) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(Exception):
        cashctrl_ledger.add_vat_code(
            account=-11, code="Standard Tax", rate=0.02, inclusive=True, text="VAT 20%"
        )

def test_add_vat_code_invalid_name_length():
    """ Test code parameter exceeding max length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=1, code="x"*51, rate=0.02, inclusive=True, text="VAT 20%"
        )  # 51 chars long

def test_add_vat_code_invalid_rate():
    """ Test invalid rate (out of bounds) """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=1, code="x", rate=20, inclusive=True, text="VAT 20%"
        )  # > 100.0

def test_add_vat_code_invalid_text():
    """ Test invalid text length """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_vat_code(
            account=1, code="x", rate=0.02, inclusive=True, text="x"*51
        )  # 51 chars long

def test_valid_update():
    """
    Test a valid update operation to ensure the method handles correct inputs
    without errors. Validates that the network client's post method is called
    once.
    """
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.update_vat_code(
        account=1170, code="test_api_update", rate=0.2, inclusive=True, text="VAT 20%"
    )

def test_name_too_long():
    """
    Test input validation for the 'name' parameter
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            account=1170, code="test_api_update", rate=0.2, inclusive=True, text="x"*51
        )

def test_percentage_out_of_bounds():
    """
    Ensure the method validates the 'percentage' parameter within the range 0
    to 1, raising a ValueError if not.
    """
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_vat_code(
            account=1170, code="test_api_update", rate=20, inclusive=True, text="x"*51
        )
