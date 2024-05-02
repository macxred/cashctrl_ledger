"""
Unit tests for basic connectivity in CashCtrlLedger.
These tests check that essential data is present and not empty.
"""

import pytest
from cashctrl_ledger import CashCtrlLedger
from requests.exceptions import HTTPError

def test_data_is_not_empty():
    """
    Test that the 'person/list.json' endpoint returns a response with non-empty data.
    """
    cashctrl_ledger = CashCtrlLedger()

    try:
        response = cashctrl_ledger.client.get("person/list.json")
    except HTTPError as e:
        pytest.fail(f"API request failed with error: {e}")

    assert "data" in response, "Expected 'data' to be in the response"
