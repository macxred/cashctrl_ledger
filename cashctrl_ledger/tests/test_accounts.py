"""Unit tests for accounts accessor and mutator methods."""

from io import StringIO
import pandas as pd
import pytest
from pyledger.tests import BaseTestAccounts
from pyledger.constants import ACCOUNT_BALANCE_SCHEMA, AGGREGATED_BALANCE_SCHEMA
from base_test import BaseTestCashCtrl
from requests.exceptions import RequestException
from consistent_df import assert_frame_equal, enforce_schema
from cashctrl_ledger.constants import ACCOUNT_ROOT_CATEGORIES
from cashctrl_ledger import CashCtrlLedger


class TestAccounts(BaseTestCashCtrl, BaseTestAccounts):
    """Test suite for the Account accessor and mutator methods."""

    @pytest.fixture()
    def engine(self, initial_engine):
        initial_engine.clear()
        return initial_engine

    def test_account_accessor_mutators(self, restored_engine):
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
        root_categories = ["/" + category for category in ACCOUNT_ROOT_CATEGORIES]
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

    def test_account_balance(self, engine):
        """This method overrides base implementation since revaluations
        in the CashCtrlLedger package can not be restored and should be manually booked.
        """
        engine.transitory_account = 9999
        accounts = pd.concat(
            [self.ACCOUNTS, engine.accounts.list()], ignore_index=True
        ).drop_duplicates(["account"])
        engine.restore(
            accounts=accounts, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
        )
        engine.book_revaluations(self.REVALUATIONS)

        EXPECTED_BALANCE_NO_PROFIT_CENTERS = self.EXPECTED_BALANCE.query("profit_center.isna()")
        for _, row in EXPECTED_BALANCE_NO_PROFIT_CENTERS.iterrows():
            period = row['period']
            account = row['account']
            expected = row['balance']
            actual = engine._account_balance(period=period, account=row['account'])
            assert expected == actual, (
                f"Account balance for {account} on {period} of {actual} differs from {expected}."
            )

    def test_account_balances(self, engine):
        """This method overrides base implementation since revaluations
        in the CashCtrlLedger package cannot be restored and should be manually booked.
        Additionally, account groups in the expected data should be sanitized
        to comply with CashCtrl requirements.
        """
        engine.transitory_account = 9999
        accounts = pd.concat(
            [self.ACCOUNTS, engine.accounts.list()], ignore_index=True
        ).drop_duplicates(["account"])
        engine.restore(
            accounts=accounts, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
        )
        engine.book_revaluations(self.REVALUATIONS)

        # Extract unique test cases
        df = self.EXPECTED_BALANCES.copy()
        argument_cols = ["period", "accounts", "profit_center"]
        df[argument_cols] = df[argument_cols].ffill()
        cases = df.drop_duplicates(subset=argument_cols).sort_values("period")

        # Test account balances without specified profit centers
        cases_without_profit_centers = cases.query("profit_center.isna()")[argument_cols]
        for period, accounts, _ in cases_without_profit_centers.itertuples(index=False):
            expected = df.query(
                "period == @period and accounts == @accounts and profit_center.isna()"
            ).drop(columns=argument_cols)
            expected = enforce_schema(expected, ACCOUNT_BALANCE_SCHEMA)
            expected["group"] = engine.sanitize_account_groups(expected["group"])
            actual = engine.individual_account_balances(period=period, accounts=accounts)
            assert_frame_equal(expected, actual, ignore_index=True)

    def test_aggregate_account_balances(self, engine):
        """This method overrides base implementation since revaluations
        in the CashCtrlLedger package cannot be restored and should be manually booked.
        Additionally, account groups in the expected data should be sanitized
        to comply with CashCtrl requirements.
        """
        engine.transitory_account = 9999
        accounts = pd.concat(
            [self.ACCOUNTS, engine.accounts.list()], ignore_index=True
        ).drop_duplicates(["account"])
        engine.restore(
            accounts=accounts, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
        )
        engine.book_revaluations(self.REVALUATIONS)

        account_balances = engine.individual_account_balances(period="2024", accounts="1000:9999")
        actual = engine.aggregate_account_balances(account_balances, n=2)
        actual = actual.query("description != 'Transitory account'")
        expected = enforce_schema(self.EXPECTED_AGGREGATED_BALANCES, AGGREGATED_BALANCE_SCHEMA)
        expected["group"] = engine.sanitize_account_groups(expected["group"])
        assert_frame_equal(actual, expected, ignore_index=True)

    @pytest.mark.parametrize(
        "input_groups, expected_groups",
        [
            # Basic cases
            (["Assets/Subgroup1/Subgroup2"], ["/Assets/Subgroup1/Subgroup2"]),
            (["/Assets/Subgroup1/Subgroup2"], ["/Assets/Subgroup1/Subgroup2"]),
            (["Revenue/Subgroup"], ["/Revenue/Subgroup"]),
            # Close matches
            (["Revenues/Subgroup"], ["/Revenue/Subgroup"]),
            (["Expens/Subgroup"], ["/Expense/Subgroup"]),
            # Top-level groups
            (["Balance"], ["/Balance"]),
            (["/Liabilities & Equity"], ["/Liabilities & Equity"]),
            ([], []),  # Empty input
            (["Assets"], ["/Assets"]),  # Single valid group
            (["Invalid"], ["/Balance"]),  # Single invalid group
            (["/RandomGroup/Subgroup"], ["/Revenue/Subgroup"]),  # No match
        ]
    )
    def test_sanitize_account_groups(self, input_groups, expected_groups):
        engine = CashCtrlLedger()
        input_series = pd.Series(input_groups, dtype="string[python]")
        expected_series = pd.Series(expected_groups, dtype="string[python]")
        output_series = engine.sanitize_account_groups(input_series)
        assert output_series.equals(expected_series), (
            f"Expected {expected_series.tolist()} but got {output_series.tolist()}"
        )

    # TODO: adapt this test
    @pytest.mark.skip(reason="This test need to be adapted to work in this package")
    def test_account_history(self):
        pass
