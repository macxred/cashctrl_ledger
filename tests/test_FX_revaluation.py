"""Unit tests for FX revaluation"""

from io import StringIO
from cashctrl_ledger import CashCtrlLedger
import pandas as pd
import pytest

ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10001,      EUR,         , Test EUR Bank Account
    /Assets, 10002,      USD,         , Test USD Bank Account
    /Assets, 10003,      CHF,         , Test CHF Bank Account
"""

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10001,           10003,      EUR,     100.00,                     ,              , pytest single transaction 1,
    2,  2024-05-24, 10002,           10003,      USD,     100.00,                     ,              , pytest single transaction 2,
"""

# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


@pytest.fixture(scope="module")
def set_up_ledger_and_account():
    cashctrl = CashCtrlLedger()
    cashctrl.transitory_account = 1000

    # Fetch original state
    initial_account_chart = cashctrl.account_chart().reset_index()
    initial_ledger = cashctrl.ledger()

    # Create test accounts and Ledger
    cashctrl.mirror_account_chart(TEST_ACCOUNTS, delete=False)
    cashctrl.mirror_ledger(LEDGER_ENTRIES, delete=False)

    yield

    # Restore initial state
    cashctrl.mirror_ledger(initial_ledger, delete=True)
    cashctrl.mirror_account_chart(initial_account_chart, delete=True)


def test_FX_revaluation(set_up_ledger_and_account):
    cashctrl = CashCtrlLedger()
    accounts = pd.DataFrame({
        "foreign_currency_account": [10001, 10002],
        "fx_gain_loss_account": [10003, 10003],
        "exchange_rate": [0.75, 0.5],
    })
    eur_account_id = cashctrl._client.account_to_id(10001)
    usd_account_id = cashctrl._client.account_to_id(10002)

    cashctrl.FX_revaluation(accounts=accounts)
    ex_diff = cashctrl._client.get("fiscalperiod/exchangediff.json")["data"]
    eur_account = next((item for item in ex_diff if item['accountId'] == eur_account_id), None)
    usd_account = next((item for item in ex_diff if item['accountId'] == usd_account_id), None)
    assert eur_account is not None, "EUR account not found in exchange differences"
    assert usd_account is not None, "USD account not found in exchange differences"
    assert eur_account['dcBalance'] == 75.0, (
        f"EUR account dcBalance is {eur_account['dcBalance']}, expected 50.0")
    assert usd_account['dcBalance'] == 50.0, (
        f"USD account dcBalance is {usd_account['dcBalance']}, expected 50.0")
