"""Unit tests for accounts accessor and mutator methods."""

from io import StringIO
import pandas as pd
import pytest
from pyledger.tests.base_accounts import BaseTestAccounts
# flake8: noqa: F401
from base_test import initial_ledger
from requests.exceptions import RequestException


ACCOUNT_CSV = """
    group,         account, currency, tax_code, description
    /Balance,         9990,      EUR,         , Test EUR Bank Account
    /Balance/Node,    9993,      EUR,         , Transitory Account EUR
"""

LEDGER_CSV = """
    id,     date, account, contra, currency,     amount, report_amount, description
    1,  2024-01-21,  9992,   9995,      CHF,     100.00,              , transaction 1
    2,  2024-02-22,  9991,   9994,      USD,     100.00,         88.88, transaction 2
    3,  2024-03-23,  9991,       ,      USD,     100.00,         85.55, transaction 3
    3,  2024-03-23,      ,   9995,      CHF,      85.55,              , transaction 3
    4,  2024-04-24,  9994,   9991,      USD,     100.00,         77.77, transaction 4
    5,  2024-05-25,  9992,   9995,      CHF,      10.00,              , transaction 5
    6,  2024-06-26,  9995,       ,      CHF,      95.55,              , transaction 6
    6,  2024-06-26,      ,   9991,      USD,     100.00,         95.55, transaction 6
"""
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


class TestAccounts(BaseTestAccounts):
    @pytest.fixture()
    def ledger(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger

    @pytest.fixture(scope="class")
    def ledger_with_balance(self, initial_ledger):
        initial_ledger.restore(accounts=self.ACCOUNTS, ledger=LEDGER_ENTRIES, settings=self.SETTINGS)
        return initial_ledger

    # TODO: Move this test to the PyLedger package when storing Price history implemented
    @pytest.mark.parametrize(
        "account, date, expected",
        [
            (9991, "2024-02-21", {"USD": 0.00, "reporting_currency": 0.00}),
            (9991, "2024-02-22", {"USD": 100.00, "reporting_currency": 88.88}),
            (9991, "2024-03-23", {"USD": 200.00, "reporting_currency": 174.43}),
            (9991, "2024-04-24", {"USD": 100.00, "reporting_currency": 96.66}),
            (9991, "2024-06-26", {"USD": 0.00, "reporting_currency": 1.11}),
            (9991, None, {"USD": 0.00, "reporting_currency": 1.11}),
            (9992, "2024-01-20", {"CHF": 0.00, "reporting_currency": 0.00}),
            (9992, "2024-01-21", {"CHF": 100.00, "reporting_currency": 100.00}),
            (9992, "2024-05-25", {"CHF": 110.00, "reporting_currency": 110.00}),
            (9992, None, {"CHF": 110.00, "reporting_currency": 110.00}),
            (9994, "2024-02-21", {"USD": 0.00, "reporting_currency": 0.00}),
            (9994, "2024-02-22", {"USD": -100.00, "reporting_currency": -88.88}),
            (9994, "2024-04-24", {"USD": 0.00, "reporting_currency": -11.11}),
            (9994, None, {"USD": 0.00, "reporting_currency": -11.11}),
            (9995, "2024-01-20", {"CHF": 0.00, "reporting_currency": 0.00}),
            (9995, "2024-01-21", {"CHF": -100.00, "reporting_currency": -100.00}),
            (9995, "2024-03-23", {"CHF": -185.55, "reporting_currency": -185.55}),
            (9995, "2024-05-25", {"CHF": -195.55, "reporting_currency": -195.55}),
            (9995, "2024-06-26", {"CHF": -100.00, "reporting_currency": -100.00}),
            (9995, None, {"CHF": -100.00, "reporting_currency": -100.00}),
        ],
    )
    def test_account_single_balance(self, ledger_with_balance, account, date, expected):
        balance = ledger_with_balance._single_account_balance(account=account, date=date)
        assert balance == expected

    def test_add_already_existed_raise_error(self, ledger):
        super().test_add_already_existed_raise_error(
            ledger, error_class=RequestException, error_message="This number is already used"
        )

    def test_modify_non_existed_raise_error(self, ledger):
        super().test_modify_non_existed_raise_error(
            ledger, error_class=ValueError, error_message="No id found for account"
        )
    def test_delete_non_existing_account_raise_error(self, ledger):
        ledger.delete_accounts([1141], allow_missing=True)
        assert 1141 not in ledger.accounts()["account"].values
        with pytest.raises(ValueError):
            ledger.delete_accounts([1141])

    def test_add_account_with_invalid_currency_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.add_account(
                account=1142,
                currency="",
                description="test account",
                tax_code=None,
                group="/Assets/Anlagevermögen",
            )

    def test_add_account_with_invalid_tax_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.modify_account(
                account=1143,
                currency="USD",
                description="test account",
                tax_code="Non-Existing Tax Code",
                group="/Assets/Anlagevermögen",
            )

    def test_add_account_with_invalid_group_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.add_account(
                account=999999,
                currency="USD",
                description="test account",
                tax_code="MwSt. 2.6%",
                group="/Assets/Anlagevermögen/ABC",
            )

    def test_update_non_existing_account_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.modify_account(
                account=1147,
                currency="CHF",
                description="test account",
                tax_code="MwSt. 2.6%",
                group="/Assets/Anlagevermögen",
            )

    def test_modify_account_with_invalid_currency_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.modify_account(
                account=1148,
                currency="not-existing-currency",
                description="test account",
                tax_code=None,
                group="/Assets/Anlagevermögen",
            )

    def test_modify_account_with_invalid_tax_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.modify_account(
                account=1149,
                currency="USD",
                description="test create account",
                tax_code="Non-Existing Tax Code",
                group="/Assets/Anlagevermögen",
            )

    def test_modify_account_with_invalid_group_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.modify_account(
                account=1149,
                currency="USD",
                description="test create account",
                tax_code="MwSt. 2.6%",
                group="/ABC",
            )

    def test_mirror_accounts_with_root_category(self, ledger):
        """This test ensures that root categories remain untouched, and all new accounts categories
        expected to be created are done so before any existing accounts are mirrored.
        """
        ledger.restore(accounts=ACCOUNTS, settings=self.SETTINGS)
        initial_accounts = ledger.accounts()
        expected = initial_accounts[~initial_accounts["group"].str.startswith("/Balance")]
        initial_categories = ledger._client.list_categories("account", include_system=True)
        categories_dict = initial_categories.set_index("path")["number"].to_dict()

        assert not initial_accounts[initial_accounts["group"].str.startswith("/Balance")].empty, (
            "There are no remote accounts placed in /Balance node"
        )

        ledger.mirror_accounts(expected.copy(), delete=True)
        mirrored_df = ledger.accounts()
        updated_categories = ledger._client.list_categories("account", include_system=True)
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

        ledger.mirror_accounts(initial_accounts.copy(), delete=True)
        mirrored_df = ledger.accounts()
        updated_categories = ledger._client.list_categories("account", include_system=True)
        updated_categories_dict = initial_categories.set_index("path")["number"].to_dict()
        pd.testing.assert_frame_equal(initial_accounts, mirrored_df)
        assert updated_categories_dict == categories_dict, "Some categories were not restored"
