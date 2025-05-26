import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from cashctrl_ledger import CashCtrlLedger


@pytest.mark.parametrize("accounts, expected", [
    # Basic positive and negative mix
    (
        {"add": [1000, 2000, 1000], "subtract": [2000, 3000]},
        pd.DataFrame({"account": [1000, 2000, 3000], "multiplier": [2, 0, -1]})
    ),
    # Only add
    (
        {"add": [4000, 4000, 4000], "subtract": []},
        pd.DataFrame({"account": [4000], "multiplier": [3]})
    ),
    # Only subtract
    (
        {"add": [], "subtract": [5000, 5000]},
        pd.DataFrame({"account": [5000], "multiplier": [-2]})
    ),
    # Empty input
    (
        {"add": [], "subtract": []},
        pd.DataFrame(columns=["account", "multiplier"])
    ),
    # Overlapping accounts cancel out
    (
        {"add": [6000, 6000], "subtract": [6000, 6000]},
        pd.DataFrame({"account": [6000], "multiplier": [0]})
    ),
    # Mixed additive and subtractive with zero-sum edge
    (
        {"add": [7000, 8000, 8000], "subtract": [7000, 8000]},
        pd.DataFrame({"account": [7000, 8000], "multiplier": [0, 1]})
    )
])
def test_account_multipliers(accounts, expected):
    result = CashCtrlLedger.account_multipliers(accounts)
    result_sorted = result.sort_values(by="account").reset_index(drop=True)
    expected_sorted = expected.sort_values(by="account").reset_index(drop=True)
    assert_frame_equal(result_sorted, expected_sorted)
