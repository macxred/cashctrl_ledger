"""Unit tests for account_multipliers() method."""

import pytest
from cashctrl_ledger import CashCtrlLedger


@pytest.mark.parametrize("accounts, expected", [
    # Basic positive and negative mix
    (
        {"add": [1000, 2000, 1000], "subtract": [2000, 3000]},
        {1000: 2, 2000: 0, 3000: -1}
    ),
    # Only add
    (
        {"add": [4000, 4000, 4000], "subtract": []},
        {4000: 3}
    ),
    # Only subtract
    (
        {"add": [], "subtract": [5000, 5000]},
        {5000: -2}
    ),
    # Empty input
    (
        {"add": [], "subtract": []},
        {}
    ),
    # Overlapping accounts cancel out
    (
        {"add": [6000, 6000], "subtract": [6000, 6000]},
        {6000: 0}
    ),
    # Mixed additive and subtractive with zero-sum edge
    (
        {"add": [7000, 8000, 8000], "subtract": [7000, 8000]},
        {7000: 0, 8000: 1}
    )
])
def test_account_multipliers(accounts, expected):
    result = CashCtrlLedger.account_multipliers(accounts)
    assert result == expected
