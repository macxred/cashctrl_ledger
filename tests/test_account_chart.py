"""Unit tests for account chart accessor and mutator methods."""

from io import StringIO
from cashctrl_ledger import CashCtrlLedger
import pandas as pd
import pytest
import requests


ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10022,      USD,         , Test USD Bank Account
    /Assets, 10023,      CHF,         , Test CHF Bank Account
    /Assets, 19992,      USD,         , Transitory Account USD
    /Assets, 19993,      CHF,         , Transitory Account CHF
"""

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount, text,                        document
    1,  2024-01-21, 10023,           19993,      CHF,     100.00,                     , pytest transaction 1,
    2,  2024-02-22, 10022,           19992,      USD,     100.00,                88.88, pytest transaction 2,
    3,  2024-03-23, 10022,                ,      USD,     100.00,                85.55, pytest transaction 3,
    3,  2024-03-23,      ,           19993,      CHF,      85.55,                     , pytest transaction 3,
    4,  2024-04-24, 19992,           10022,      USD,     100.00,                77.77, pytest transaction 4,
    5,  2024-05-25, 10023,           19993,      CHF,      10.00,                     , pytest transaction 5,
    6,  2024-06-26, 19993,                ,      CHF,      95.55,                     , pytest transaction 6,
    6,  2024-06-26,      ,           10022,      USD,     100.00,                95.55, pytest transaction 6,
"""

# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


@pytest.fixture(scope="module")
def set_up_vat_account_and_ledger():
    cashctrl = CashCtrlLedger()
    cashctrl.transitory_account = 19993

    initial_account_chart = cashctrl.account_chart().reset_index()
    initial_ledger = cashctrl.ledger()

    cashctrl.mirror_account_chart(TEST_ACCOUNTS, delete=False)
    cashctrl.mirror_ledger(LEDGER_ENTRIES, delete=False)

    yield

    cashctrl.mirror_ledger(initial_ledger, delete=True)
    cashctrl.mirror_account_chart(initial_account_chart, delete=True)


@pytest.fixture(scope="module")
def add_and_delete_vat_code():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        code="TestCodeAccounts",
        text="VAT 2%",
        account=2200,
        rate=0.02,
        inclusive=True,
    )

    yield

    cashctrl_ledger.delete_vat_code(code="TestCodeAccounts")


@pytest.mark.parametrize(
    "account, date, expected",
    [
        (10022, "2024-02-21", {"USD": 0.00, "base_currency": 0.00}),
        (10022, "2024-02-22", {"USD": 100.00, "base_currency": 88.88}),
        (10022, "2024-03-23", {"USD": 200.00, "base_currency": 174.43}),
        (10022, "2024-04-24", {"USD": 100.00, "base_currency": 96.66}),
        (10022, "2024-06-26", {"USD": 0.00, "base_currency": 1.11}),
        (10022, None, {"USD": 0.00, "base_currency": 1.11}),
        (10023, "2024-01-20", {"CHF": 0.00, "base_currency": 0.00}),
        (10023, "2024-01-21", {"CHF": 100.00, "base_currency": 100.00}),
        (10023, "2024-05-25", {"CHF": 110.00, "base_currency": 110.00}),
        (10023, None, {"CHF": 110.00, "base_currency": 110.00}),
        (19992, "2024-02-21", {"USD": 0.00, "base_currency": 0.00}),
        (19992, "2024-02-22", {"USD": -100.00, "base_currency": -88.88}),
        (19992, "2024-04-24", {"USD": 0.00, "base_currency": -11.11}),
        (19992, None, {"USD": 0.00, "base_currency": -11.11}),
        (19993, "2024-01-20", {"CHF": 0.00, "base_currency": 0.00}),
        (19993, "2024-01-21", {"CHF": -100.00, "base_currency": -100.00}),
        (19993, "2024-03-23", {"CHF": -185.55, "base_currency": -185.55}),
        (19993, "2024-05-25", {"CHF": -195.55, "base_currency": -195.55}),
        (19993, "2024-06-26", {"CHF": -100.00, "base_currency": -100.00}),
        (19993, None, {"CHF": -100.00, "base_currency": -100.00}),
    ],
)
def test_account_single_balance(set_up_vat_account_and_ledger, account, date, expected):
    cashctrl_ledger = CashCtrlLedger()
    balance = cashctrl_ledger._single_account_balance(account=account, date=date)
    assert balance == expected


def test_account_mutators(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()

    cashctrl_ledger.delete_account(1145, allow_missing=True)
    cashctrl_ledger.delete_account(1146, allow_missing=True)
    account_chart = cashctrl_ledger.account_chart()
    assert 1145 not in account_chart.index
    assert 1146 not in account_chart.index

    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        "account": 1145,
        "currency": "CHF",
        "text": "test create account",
        "vat_code": "TestCodeAccounts",
        "group": "/Assets/Anlagevermögen",
    }
    cashctrl_ledger.add_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
    created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["text"].item() == new_account["text"]
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["currency"].item() == new_account["currency"]
    assert created_accounts["vat_code"].item() == "TestCodeAccounts"
    assert created_accounts["group"].item() == new_account["group"]

    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        "account": 1146,
        "currency": "CHF",
        "text": "test create account",
        "vat_code": None,
        "group": "/Assets/Anlagevermögen",
    }
    cashctrl_ledger.add_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
    created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["text"].item() == new_account["text"]
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["currency"].item() == new_account["currency"]
    assert pd.isna(created_accounts["vat_code"].item())
    assert created_accounts["group"].item() == new_account["group"]

    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        "account": 1146,
        "currency": "CHF",
        "text": "test update account",
        "vat_code": "TestCodeAccounts",
        "group": "/Assets/Anlagevermögen",
    }
    cashctrl_ledger.update_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
    modified_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

    assert len(modified_accounts) == 1, "Expected exactly one updated row"
    assert modified_accounts["account"].item() == new_account["account"]
    assert modified_accounts["text"].item() == new_account["text"]
    assert modified_accounts["account"].item() == new_account["account"]
    assert modified_accounts["currency"].item() == new_account["currency"]
    assert modified_accounts["vat_code"].item() == "TestCodeAccounts"
    assert modified_accounts["group"].item() == new_account["group"]

    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    new_account = {
        "account": 1145,
        "currency": "USD",
        "text": "test update account without VAT",
        "vat_code": None,
        "group": "/Assets/Anlagevermögen",
    }
    cashctrl_ledger.update_account(**new_account)
    updated_accounts = cashctrl_ledger.account_chart().reset_index()
    outer_join = pd.merge(initial_accounts, updated_accounts, how="outer", indicator=True)
    created_accounts = outer_join[outer_join["_merge"] == "right_only"].drop("_merge", axis=1)

    assert len(created_accounts) == 1, "Expected exactly one row to be added"
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["text"].item() == new_account["text"]
    assert created_accounts["account"].item() == new_account["account"]
    assert created_accounts["currency"].item() == new_account["currency"]
    assert pd.isna(created_accounts["vat_code"].item())
    assert created_accounts["group"].item() == new_account["group"]

    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(account=1145)
    cashctrl_ledger.delete_account(account=1146)
    updated_accounts = cashctrl_ledger.account_chart()
    assert 1145 not in updated_accounts.index
    assert 1146 not in updated_accounts.index


def test_delete_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.delete_account(1141, allow_missing=True)
    assert 1141 not in cashctrl_ledger.account_chart().index
    with pytest.raises(ValueError):
        cashctrl_ledger.delete_account(1141)


def test_add_pre_existing_account_raise_error(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(requests.exceptions.RequestException):
        cashctrl_ledger.add_account(
            account=1200,
            currency="EUR",
            text="test account",
            vat_code="TestCodeAccounts",
            group="/Assets/Anlagevermögen",
        )


def test_add_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(
            account=1142,
            currency="",
            text="test account",
            vat_code=None,
            group="/Assets/Anlagevermögen",
        )


def test_add_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(
            account=1143,
            currency="USD",
            text="test account",
            vat_code="Non-Existing Tax Code",
            group="/Assets/Anlagevermögen",
        )


def test_add_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.add_account(
            account=999999,
            currency="USD",
            text="test account",
            vat_code="MwSt. 2.6%",
            group="/Assets/Anlagevermögen/ABC",
        )


def test_update_non_existing_account_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(
            account=1147,
            currency="CHF",
            text="test account",
            vat_code="MwSt. 2.6%",
            group="/Assets/Anlagevermögen",
        )


def test_update_account_with_invalid_currency_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(
            account=1148,
            currency="not-existing-currency",
            text="test account",
            vat_code=None,
            group="/Assets/Anlagevermögen",
        )


def test_update_account_with_invalid_vat_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(
            account=1149,
            currency="USD",
            text="test create account",
            vat_code="Non-Existing Tax Code",
            group="/Assets/Anlagevermögen",
        )


def test_update_account_with_invalid_group_raise_error():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(ValueError):
        cashctrl_ledger.update_account(
            account=1149,
            currency="USD",
            text="test create account",
            vat_code="MwSt. 2.6%",
            group="/ABC",
        )


def test_mirror_accounts(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()

    account = pd.DataFrame(
        {
            "account": [1, 2],
            "currency": ["CHF", "EUR"],
            "text": ["Test Account 1", "Test Account 2"],
            "vat_code": ["TestCodeAccounts", None],
            "group": ["/Assets", "/Assets/Anlagevermögen/xyz"],
        }
    )
    target_df = pd.concat([account, initial_accounts])

    cashctrl_ledger.mirror_account_chart(target_df, delete=False)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how="left", indicator=True)
    assert (m["_merge"] == "both").all(), "Mirroring error: Some target accounts were not mirrored"

    cashctrl_ledger.mirror_account_chart(target_df, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how="outer", indicator=True)
    assert (m["_merge"] == "both").all(), "Mirroring error: Some target accounts were not mirrored"

    target_df = target_df.sample(frac=1).reset_index(drop=True)

    target_df.loc[target_df["account"] == 2, "text"] = "New_Test_Text"
    cashctrl_ledger.mirror_account_chart(target_df, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = target_df.merge(mirrored_df, how="outer", indicator=True)
    assert (m["_merge"] == "both").all(), "Mirroring error: Some target accounts were not mirrored"

    cashctrl_ledger.mirror_account_chart(initial_accounts, delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    m = initial_accounts.merge(mirrored_df, how="outer", indicator=True)
    assert (m["_merge"] == "both").all(), "Mirroring error: Some target accounts were not mirrored"


def test_mirror_accounts_with_root_category(add_and_delete_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_accounts = cashctrl_ledger.account_chart().reset_index()
    expected = initial_accounts[~initial_accounts["group"].str.startswith("/Balance")]
    initial_categories = cashctrl_ledger._client.list_categories("account", include_system=True)
    categories_dict = initial_categories.set_index("path")["number"].to_dict()

    assert not initial_accounts[initial_accounts["group"].str.startswith("/Balance")].empty, (
        "There are no remote accounts placed in /Balance node"
    )

    cashctrl_ledger.mirror_account_chart(expected.copy(), delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    updated_categories = cashctrl_ledger._client.list_categories("account", include_system=True)
    updated_categories_dict = updated_categories.set_index("path")["number"].to_dict()
    difference = set(categories_dict.keys()) - set(updated_categories_dict.keys())
    initial_sub_nodes = [
        key for key in difference if key.startswith("/Balance") and key != "/Balance"
    ]

    assert mirrored_df[mirrored_df["group"].str.startswith("/Balance")].empty, (
        "Accounts placed in /Balance node were not deleted"
    )
    assert len(initial_sub_nodes) > 0, "Sub-nodes were not deleted"
    assert updated_categories_dict["/Balance"] == categories_dict["/Balance"], (
        "Root node /Balance was deleted"
    )

    cashctrl_ledger.mirror_account_chart(initial_accounts.copy(), delete=True)
    mirrored_df = cashctrl_ledger.account_chart().reset_index()
    updated_categories = cashctrl_ledger._client.list_categories("account", include_system=True)
    updated_categories_dict = initial_categories.set_index("path")["number"].to_dict()
    pd.testing.assert_frame_equal(initial_accounts, mirrored_df)
    assert updated_categories_dict == categories_dict, "Some categories were not restored"
