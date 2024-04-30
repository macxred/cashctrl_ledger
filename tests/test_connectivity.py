"""
Unit tests for basic connectivity in CashCtrlLedger.
These tests check that essential data is present and not empty.
"""

import pytest  # Using pytest framework for testing
from cashctrl_ledger import CashCtrlLedger

@pytest.fixture
def cashctrl_ledger():
    """
    Fixture to create an instance of CashCtrlLedger for use in tests.
    """
    return CashCtrlLedger()

def test_data_is_not_empty(cashctrl_ledger):
    """
    Test that the 'person/list.json' endpoint returns a response with non-empty data.
    """
    # Act
    response = cashctrl_ledger._client.get("person/list.json")
    
    # Assert
    assert "data" in response and len(response["data"]) > 0, "Expected 'data' field to be non-empty"
