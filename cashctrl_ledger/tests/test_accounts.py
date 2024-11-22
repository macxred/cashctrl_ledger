"""Unit tests for accounts accessor and mutator methods."""

from io import StringIO
import pandas as pd
import pytest
from pyledger.tests import BaseTestAccounts
# flake8: noqa: F401
from base_test import initial_engine
from requests.exceptions import RequestException


ACCOUNT_CSV = """
    group,         account, currency, tax_code, description
    /Balance,         9990,      EUR,         , Test EUR Bank Account
    /Balance/Node,    9993,      EUR,         , Transitory Account EUR
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


class TestAccounts(BaseTestAccounts):
    ACCOUNTS = BaseTestAccounts.ACCOUNTS.copy()
    # Set the default root node for CashCtrl. In CashCtrl it is not possible to create root nodes
    ACCOUNTS.loc[:, "group"] = "/Assets"
    # TODO: Remove when Assets will be implemented
    ACCOUNTS.loc[ACCOUNTS["currency"] == "JPY", "currency"] = "USD"
    # TODO: Remove when error with .ne() will be fixed
    ACCOUNTS.loc[:, "tax_code"] = pd.NA

    TAX_CODES = BaseTestAccounts.TAX_CODES.copy()
    # In CashCtrl it is not possible to create TAX CODE without specified account
    account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = account

    @pytest.fixture()
    def engine(self, initial_engine):
        initial_engine.clear()
        return initial_engine

    def test_account_accessor_mutators(self, engine):
        super().test_account_accessor_mutators(engine, ignore_row_order=True)

    def test_add_existing_account_raise_error(self, engine):
        account = {
            "account": 77777, "currency": "CHF", "description": "test account", "group": "/Assets"
        }
        engine.accounts.add([account])
        with pytest.raises(RequestException):
            engine.accounts.add([account])

    def test_modify_nonexistent_account_raise_error(self, engine):
        super().test_modify_nonexistent_account_raise_error(
            engine, error_class=ValueError, error_message="No id found for account"
        )
    def test_delete_account_allow_missing(self, engine):
        super().test_delete_account_allow_missing(
            engine, error_class=ValueError, error_message="No id found for account"
        )

    def test_add_account_with_invalid_currency_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.add({
                "account": 1142,
                "currency": "",
                "description": "test account",
                "tax_code": None,
                "group": "/Assets/Anlagevermögen",
            })

    def test_add_account_with_invalid_tax_raise_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.add({
                "account": 1143,
                "currency": "USD",
                "description": "test account",
                "tax_code": "Non-Existing Tax Code",
                "group": "/Assets/Anlagevermögen",
            })

    def test_add_account_with_invalid_group_raise_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.add({
                "account": 999999,
                "currency": "USD",
                "description": "test account",
                "tax_code": "MwSt. 2.6%",
                "group": "/Assets/Anlagevermögen/ABC",
            })

    def test_update_non_existing_account_raise_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1147,
                "currency": "CHF",
                "description": "test account",
                "tax_code": "MwSt. 2.6%",
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_currency_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1148,
                "currency": "not-existing-currency",
                "description": "test account",
                "tax_code": None,
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_tax_raise_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1149,
                "currency": "USD",
                "description": "test create account",
                "tax_code": "Non-Existing Tax Code",
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_group_raise_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1149,
                "currency": "USD",
                "description": "test create account",
                "tax_code": "MwSt. 2.6%",
                "group": "/ABC",
            })

    def test_mirror_accounts_with_root_category(self, engine):
        """This test ensures that root categories remain untouched, and all new accounts categories
        expected to be created are done so before any existing accounts are mirrored.
        """
        engine.restore(accounts=ACCOUNTS, settings=self.SETTINGS)
        initial_accounts = engine.accounts.list()
        expected = initial_accounts[~initial_accounts["group"].str.startswith("/Balance")]
        initial_categories = engine._client.list_categories("account", include_system=True)
        categories_dict = initial_categories.set_index("path")["number"].to_dict()

        assert not initial_accounts[initial_accounts["group"].str.startswith("/Balance")].empty, (
            "There are no remote accounts placed in /Balance node"
        )

        engine.accounts.mirror(expected.copy(), delete=True)
        mirrored_df = engine.accounts.list()
        updated_categories = engine._client.list_categories("account", include_system=True)
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

        engine.accounts.mirror(initial_accounts.copy(), delete=True)
        mirrored_df = engine.accounts.list()
        updated_categories = engine._client.list_categories("account", include_system=True)
        updated_categories_dict = initial_categories.set_index("path")["number"].to_dict()
        pd.testing.assert_frame_equal(initial_accounts, mirrored_df)
        assert updated_categories_dict == categories_dict, "Some categories were not restored"

    @pytest.mark.skip(reason="Need to implement all entities to run this tests")
    def test_account_balance(self):
        pass
