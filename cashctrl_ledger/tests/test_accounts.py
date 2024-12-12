"""Unit tests for accounts accessor and mutator methods."""

from io import StringIO
import pandas as pd
import pytest
from pyledger.tests import BaseTestAccounts
# flake8: noqa: F401
from base_test import initial_engine
from requests.exceptions import RequestException
from consistent_df import assert_frame_equal
from cashctrl_ledger.constants import DEFAULT_ACCOUNT_GROUPS


class TestAccounts(BaseTestAccounts):
    """Test suite for the Account accessor and mutator methods."""

    TAX_CODES = BaseTestAccounts.TAX_CODES.copy()
    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    default_account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = default_account

    @pytest.fixture()
    def engine(self, initial_engine):
        self.ACCOUNTS = initial_engine.sanitize_accounts(self.ACCOUNTS)
        initial_engine.clear()
        return initial_engine

    def test_account_accessor_mutators(self, restored_engine):
        # TODO: some accounts can not be created via add() method since they
        # are contain groups that do not exist.
        # This functionality only works when use mirror() method - then categories are created
        # Hack: Keep only root nodes
        self.ACCOUNTS['group'] = self.ACCOUNTS['group'].str.split('/').str[1]
        self.ACCOUNTS = restored_engine.sanitize_accounts(self.ACCOUNTS)
        super().test_account_accessor_mutators(restored_engine, ignore_row_order=True)

    def test_add_existing_account_raise_error(self, engine):
        """Override base test to include `group` field, required for CashCtrl."""
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

    def test_add_account_with_invalid_currency_raises_error(self, engine):
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

    def test_add_account_with_invalid_tax_raises_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.add({
                "account": 999999,
                "currency": "USD",
                "description": "test account",
                "tax_code": "MwSt. 2.6%",
                "group": "/Assets/Anlagevermögen/ABC",
            })

    def test_update_nonexistent_account_raises_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1147,
                "currency": "CHF",
                "description": "test account",
                "tax_code": "MwSt. 2.6%",
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_currency_raises_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1148,
                "currency": "not-existing-currency",
                "description": "test account",
                "tax_code": None,
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_tax_raises_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1149,
                "currency": "USD",
                "description": "test create account",
                "tax_code": "Non-Existing Tax Code",
                "group": "/Assets/Anlagevermögen",
            })

    def test_modify_account_with_invalid_group_raises_error(self, engine):
        with pytest.raises(ValueError):
            engine.accounts.modify({
                "account": 1149,
                "currency": "USD",
                "description": "test create account",
                "tax_code": "MwSt. 2.6%",
                "group": "/ABC",
            })

    def test_mirror_accounts_updates_category_tree(self, engine):
        """
        Ensures that new categories are created and orphaned categories,
        except root nodes, are deleted when mirroring accounts.
        """
        ACCOUNT_CSV = """
            group,                 account, currency, tax_code, description
            /Balance,                 9990,      EUR,         , Test EUR Bank Account
            /Balance/Node,            9993,      EUR,         , Transitory Account EUR
            /Balance/Node/Subnode,    9994,      CHF,         , Transitory Account CHF
        """
        ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

        initial_accounts = engine.accounts.list()
        initial_categories = engine._client.list_categories("account", include_system=True)
        initial_categories = initial_categories["path"].to_list()
        expected_categories = ACCOUNTS["group"].to_list()
        assert not set(expected_categories).issubset(initial_categories), (
            "Expected categories already exists"
        )

        # Ensure target categories not present on remote are created when mirroring
        engine.accounts.mirror(ACCOUNTS, delete=True)
        accounts = engine.accounts.list()
        categories = engine._client.list_categories("account", include_system=True)
        categories = categories["path"].to_list()
        assert not accounts[accounts["group"].str.startswith("/Balance")].empty, (
            "Accounts with '/Balance' root category were not created"
        )
        expected = pd.concat(
            [ACCOUNTS, accounts.query("account not in @ACCOUNTS['account']")], ignore_index=True
        )
        assert_frame_equal(expected, accounts, ignore_row_order=True, check_like=True)

        # Ensure orphaned categories except root nodes are deleted when mirroring
        engine.accounts.mirror(initial_accounts, delete=True)
        accounts = engine.accounts.list()
        categories = engine._client.list_categories("account", include_system=True)
        categories = categories["path"].to_list()
        assert_frame_equal(initial_accounts, accounts)
        assert set(initial_categories) == set(categories), (
            "Some orphaned categories were not deleted"
        )
        assert "/Balance" in set(categories), (
            "Mirroring initial accounts should not delete root '/Balance' category"
        )

        # Ensure orphaned categories except default root nodes are deleted when mirroring
        engine.accounts.mirror(pd.DataFrame({}), delete=True)
        accounts = engine.accounts.list()
        categories = engine._client.list_categories("account", include_system=True)
        categories = categories["path"].to_list()
        assert accounts.empty, "Mirror empty accounts should erase all of them"
        root_categories = DEFAULT_ACCOUNT_GROUPS
        assert set(root_categories) == set(categories), (
            "Mirroring empty state should leave only root categories"
        )

    def test_mirror_accounts_new_root_category_raises_error(self, engine):
        """CashCtrl does not allow to add new root categories."""
        ACCOUNT = pd.DataFrame([{
            "group": "/NewRoot",
            "account": 9995,
            "currency": "USD",
            "description": "Account with custom root category"
        }])
        with pytest.raises(ValueError, match="Cannot create new root nodes"):
            engine.accounts.mirror(ACCOUNT, delete=True)

    @pytest.mark.skip(reason="Need to implement all entities to run this test")
    def test_account_balance(self):
        pass

    def test_sanitize_accounts(self, engine):
        ACCOUNT_CSV = """
            group,             account, currency,  description
                 ,                1000,      USD,  No group
            Assets,               1001,      USD,  No leading /
            /Assets,              1002,      USD,  Already valid group
            Liability,            1003,      USD,  Needs replacement
            /NonStandardGroup,    1004,      USD,  Non-standard group
        """
        EXPECTED_CSV = """
            group,             account, currency,  description
                 ,                1000,      USD,  No group
            /Assets,              1001,      USD,  No leading /
            /Assets,              1002,      USD,  Already valid group
            /Liabilities,         1003,      USD,  Needs replacement
            /Assets,              1004,      USD,  Non-standard group
        """

        # Load input and expected data
        accounts = engine.accounts.standardize(
            pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
        )
        expected_accounts = engine.accounts.standardize(
            pd.read_csv(StringIO(EXPECTED_CSV), skipinitialspace=True)
        )

        sanitized_df = engine.sanitize_accounts(accounts)
        assert_frame_equal(sanitized_df, expected_accounts)
