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
    /Assets, 10004,      USD,         , Test USD Bank Account 2
"""

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10001,           10003,      EUR,     100.00,                     ,              , pytest single transaction 1,
    2,  2024-05-24, 10002,           10003,      USD,     100.00,                     ,              , pytest single transaction 2,
    3,  2024-05-24, 10004,           10003,      USD,     100.00,                     ,              , pytest single transaction 3,
"""

# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


@pytest.fixture(scope="session")
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
        "foreign_currency_account": [10001, 10002, 10004],
        "fx_gain_loss_account": [10003, 10003, 10003],
        "exchange_rate": [0.75, 0.5, None],
    })
    cashctrl.FX_revaluation(accounts=accounts)
    fx_rates = cashctrl.FX_valuation()

    # TODO: Refactor this part when get_exchange_rate() will be implemented
    from_currency = cashctrl._client.account_to_currency(10004)
    params = {"from": from_currency, "to": cashctrl.base_currency, "date": None}
    response = cashctrl._client.request("GET", "currency/exchangerate", params=params)
    usd_na_account_fx_rate = response.json()
    usd_na_account_balance = round(100 * usd_na_account_fx_rate, 2)

    assert fx_rates.query("account == 10001")["dcBalance"].item() == 75.0, (
        "EUR account dcBalance doesn't match expected"
    )
    assert fx_rates.query("account == 10002")["dcBalance"].item() == 50.0, (
        "USD account dcBalance doesn't match expected"
    )
    assert fx_rates.query("account == 10004")["dcBalance"].item() == usd_na_account_balance, (
        "USD account with NA exchange rate dcBalance doesn't match expected"
    )


@pytest.mark.parametrize("account_id", [10001, 10002, 10004])
def test_FX_revaluation_account_none(set_up_ledger_and_account, account_id):
    cashctrl = CashCtrlLedger()
    cashctrl.FX_revaluation(accounts=None)

    ex_diff = cashctrl._client.get("fiscalperiod/exchangediff.json")["data"]
    mapped_account_id = cashctrl._client.account_to_id(account_id)
    account = next((item for item in ex_diff if item['accountId'] == mapped_account_id), None)

    from_currency = cashctrl._client.account_to_currency(account_id)
    params = {"from": from_currency, "to": cashctrl.base_currency, "date": None}
    response = cashctrl._client.request("GET", "currency/exchangerate", params=params)
    account_fx_rate = response.json()

    initial_balance = 100.0  # Assuming the initial balance is 100.0 for simplicity
    expected_dcBalance = round(initial_balance * account_fx_rate, 2)

    assert account is not None, f"Account {account} with fx rate not found in exchange differences"
    assert account['dcBalance'] == expected_dcBalance, (
        f"Account {account} dcBalance is {account['dcBalance']}, expected {expected_dcBalance}"
    )
