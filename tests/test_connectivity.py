"""
Unit tests for basic connectivity approvement.
"""

from cashctrl_ledger import CashCtrlLedger

def test_data_not_empty():
    # Arrange
    cc_ledger = CashCtrlLedger()
    # Act
    response = cc_ledger._client.get("person/list.json")
    is_not_empty = "data" in response and len(response['data']) > 0
    # Assert
    assert is_not_empty, "Expect data to be not empty"
    
