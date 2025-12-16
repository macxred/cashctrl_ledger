"""Unit tests for journal accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from requests import RequestException
# flake8: noqa: F401
from base_test import BaseTestCashCtrl
from pyledger.tests import BaseTestJournal
from consistent_df import assert_frame_equal
from io import StringIO
from pyledger.tests import BaseTest


class TestJournal(BaseTestCashCtrl, BaseTestJournal):

    @pytest.fixture()
    def engine(self, initial_engine):
        # Hack: when updating reporting currency transitory account currency should be updated
        initial_engine.restore(configuration=self.CONFIGURATION)
        initial_engine.transitory_account = 9999
        return initial_engine

    @pytest.fixture()
    def restore_fiscal_periods(self, engine):
        initial_ids = engine._client.list_fiscal_periods()["id"]
        initial_ledger = engine.journal.list()

        yield

        # Delete any created journal
        engine.journal.mirror(initial_ledger, delete=True)

        # Delete any created fiscal period
        new_ids = engine._client.list_fiscal_periods()["id"]
        created_ids = set(new_ids) - set(initial_ids)
        if len(created_ids):
            ids = ",".join([str(id) for id in created_ids])
            engine._client.post("fiscalperiod/delete.json", params={"ids": ids})
            engine._client.list_fiscal_periods.cache_clear()

    @pytest.fixture()
    def restore_transitory_account(self, engine):
        """Save and restore transitory account after test."""
        original_transitory = engine.transitory_account
        yield
        engine.transitory_account = original_transitory

    def test_journal_accessor_mutators(self, restored_engine):
        # Journal entries need to be sanitized before adding to the CashCtrl
        self.JOURNAL = restored_engine.sanitize_journal(self.JOURNAL)
        super().test_journal_accessor_mutators(restored_engine, ignore_row_order=True)

    @pytest.mark.skip(reason="CashCtrl allows to create same entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_add_journal_with_illegal_attributes(self, restored_engine):
        journal_entry = self.JOURNAL.query("id == '2'")

        # Add with non existent Tax code should raise an error
        target = journal_entry.copy()
        target["tax_code"] = "Test_Non_Existent_TAX_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            restored_engine.journal.add(target)

        # Add with non existent account should raise an error
        target = journal_entry.copy()
        target["account"] = 33333
        with pytest.raises(ValueError, match="No id found for account"):
            restored_engine.journal.add(target)

        # Add with non existent currency should raise an error
        target = journal_entry.copy()
        target["currency"] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            restored_engine.journal.add(target)

    def test_modify_non_existed_raises_error(self, restored_engine):
        super().test_modify_non_existed_raises_error(
            restored_engine, error_class=RequestException, error_message="entry does not exist"
        )

    def test_update_journal_entry_with_illegal_attributes(self, restored_engine):
        journal_entry = self.JOURNAL.query("id == '2'")
        id = restored_engine.journal.add(journal_entry)[0]

        # Updating a journal entry with non existent tax code should raise an error
        target = journal_entry.copy()
        target["id"] = id
        target["tax_code"] = "Test_Non_Existent_TAX_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            restored_engine.journal.modify(target)

        # Updating a journal entry with non existent account code should raise an error
        target = journal_entry.copy()
        target["id"] = id
        target["account"] = 333333
        with pytest.raises(ValueError, match="No id found for account"):
            restored_engine.journal.modify(target)

        # Updating a journal entry with non existent currency code should raise an error
        target = journal_entry.copy()
        target["id"] = id
        target["currency"] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            restored_engine.journal.modify(target)

        # Delete the journal entry created above
        restored_engine.journal.delete([{"id": id}])

    @pytest.mark.skip(reason="We don't have a mechanism to allow missing ids.")
    def test_delete_entry_allow_missing(self):
        pass

    def test_delete_nonexistent_entry_raise_error(self, restored_engine):
        with pytest.raises(RequestException, match="API call failed. ID missing."):
            restored_engine.journal.delete({"id": ["FAKE_ID"]})

    def test_adding_transaction_with_two_non_reporting_currencies_fails(self, restored_engine):
        expected = (
            "CashCtrl allows only the reporting currency plus a single foreign currency"
        )
        entry = BaseTestJournal.JOURNAL.query("id == '23'")
        with pytest.raises(ValueError, match=expected):
            restored_engine.journal.add(entry)

    def test_split_multi_currency_transactions(self, engine):
        transitory_account = 9999
        txn = engine.journal.standardize(BaseTestJournal.JOURNAL.query("id == '10'"))
        spit_txn = engine.split_multi_currency_transactions(
            txn, transitory_account=transitory_account
        )
        is_reporting_currency = spit_txn["currency"] == engine.reporting_currency
        spit_txn.loc[is_reporting_currency, "report_amount"] = spit_txn.loc[
            is_reporting_currency, "amount"
        ]
        assert len(spit_txn) == len(txn) + 2, "Expecting two new lines when transaction is split"
        assert sum(spit_txn["account"] == transitory_account) == 2, (
            "Expecting two transactions on transitory account"
        )
        assert all(spit_txn.groupby("id")["report_amount"].sum() == 0), (
            "Expecting split transactions to be balanced"
        )
        assert spit_txn.query("account == @transitory_account")["report_amount"].sum() == 0, (
            "Expecting transitory account to be balanced"
        )

    def test_split_several_multi_currency_transactions(self, engine):
        transitory_account = 9999
        txn = engine.journal.standardize(BaseTestJournal.JOURNAL.query("id in ['10', '23']"))
        spit_txn = engine.split_multi_currency_transactions(
            txn, transitory_account=transitory_account
        )
        is_reporting_currency = spit_txn["currency"] == engine.reporting_currency
        spit_txn.loc[is_reporting_currency, "report_amount"] = spit_txn.loc[
            is_reporting_currency, "amount"
        ]
        id_currency_pairs = (txn["id"] + txn["currency"]).nunique()
        assert len(spit_txn) == len(txn) + id_currency_pairs, (
            "Expecting one new line per currency and transaction"
        )
        assert sum(spit_txn["account"] == transitory_account) == id_currency_pairs, (
            "Expecting one transaction on transitory account per id and currency"
        )
        assert all(spit_txn.groupby("id")["report_amount"].sum() == 0), (
            "Expecting split transactions to be balanced"
        )
        assert spit_txn.query("account == @transitory_account")["report_amount"].sum() == 0, (
            "Expecting transitory account to be balanced"
        )

    @pytest.mark.skip(reason="Temporary skipping to fix later.")
    def test_list_journal_with_fiscal_periods(self, restored_engine, restore_fiscal_periods):
        JOURNAL_CSV = """
            id,       date, account, contra, currency,      amount, tax_code,  description
            1, 2023-01-24,    1000,   4000,      USD,     1000.00,         ,  Sell cakes
            2, 2024-01-24,    1000,   4000,      USD,     2000.00,         ,  Sell donuts
            3, 2025-01-24,    1000,   4000,      USD,     3000.00,         ,  Sell candies
            4, 2026-01-24,    1000,   4000,      USD,     4000.00,         ,  Sell chocolate
        """
        journal_df = restored_engine.sanitize_journal(
            pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)
        )
        restored_engine.journal.mirror(target=journal_df, delete=True)

        # Test listing journal entries for specific fiscal periods
        for year in journal_df["date"].dt.year.unique():
            assert_frame_equal(
                restored_engine.journal.list(fiscal_period=str(year)),
                journal_df.query("date.dt.year == @year"),
                ignore_columns=["id"], check_like=True, ignore_index=True
            )

        # Test retrieving all journal entries
        assert_frame_equal(restored_engine.journal.list(), journal_df,
            ignore_columns=["id"], check_like=True, ignore_index=True
        )

        # Test journal entries retrieval for current fiscal period
        fiscal_periods = restored_engine.fiscal_period_list()
        new_fiscal_period = fiscal_periods.query("isCurrent == False").iloc[0]
        restored_engine._client.post(
            "fiscalperiod/switch.json", data={"id": new_fiscal_period["id"].item()}
        )
        restored_engine._client.list_fiscal_periods.cache_clear()
        new_fiscal_period_name = int(new_fiscal_period["name"]) # noqa: F841
        expected_df = journal_df.query("date.dt.year == @new_fiscal_period_name")
        assert_frame_equal(restored_engine.journal.list("current"), expected_df,
            ignore_columns=["id"], check_like=True, ignore_index=True
        )

        # Test journal entries retrieval for invalid fiscal period raise an error
        with pytest.raises(ValueError, match="No id found for fiscal period"):
            restored_engine.journal.list("test_fiscal_period")

    def test_multi_currency_journal_transitory_balance(self, engine, restore_transitory_account):
        """Test that multi-currency journal entries can be mirrored without imbalance errors.

        Motivation:
            CashCtrl has an 8-digit precision limit for FX rates and restricts collective
            journal entries to a single foreign currency plus the reporting currency.
            When processing multi-currency transactions (e.g., USD and CAD entries in a
            CHF-reporting ledger), ExtendedCashCtrlLedger splits them into separate
            transactions and uses a transitory account to balance residual amounts.

            The original problem: CashCtrl was rejecting these journal entries with
            "Total debit and total credit must be equal" errors due to 0.01 rounding
            differences. This occurred because CashCtrl recalculates report_amount as
            `amount * fx_rate`, and the 8-digit FX precision causes small discrepancies
            that accumulate across transaction legs.

            This test verifies that the sanitize_journal and rounding compensation
            mechanisms correctly handle these FX precision issues, allowing the
            entries to be successfully mirrored to CashCtrl with a balanced
            transitory account.

        Test scenario:
            - Three multi-currency journal entries representing interest income with
              withholding tax deductions (real-world Swiss bank transaction pattern)
            - Each entry involves the reporting currency (CHF) plus one foreign currency
              (USD or CAD)
            - The test verifies that entries can be mirrored without CashCtrl rejecting
              them, and that the transitory account (1999) has a zero balance.
        """
        ACCOUNTS_CSV = """
        group,    account, currency, description
        /Assets,     1176,      CHF, Accounts Receivable VAT Cleared
        /Assets,     1903,      USD, Transitory account - USD
        /Assets,     1904,      CAD, Transitory account - CAD
        /Assets,     1999,      CHF, Transitory Account for CashCtrl rounding precision
        /Revenue,    6953,      USD, Interest Income USD
        /Revenue,    6954,      CAD, Interest Income CAD
        """
        ASSETS_CSV = """
        ticker, increment
           CAD,      0.01
           USD,      0.01
           CHF,      0.01
        """
        PRICE_CSV = """
        ticker,       date, currency,    price
           CAD, 2024-01-01,      CHF,   0.6523
           CAD, 2024-02-01,      CHF,   0.6458
           CAD, 2024-03-01,      CHF,   0.6545
           CAD, 2024-04-01,      CHF,   0.6594
           CAD, 2024-05-01,      CHF,   0.6714
           CAD, 2024-06-01,      CHF,   0.6725
           CAD, 2024-07-01,      CHF,   0.6617
           CAD, 2024-08-01,      CHF,   0.6617
           CAD, 2024-09-01,      CHF,   0.6369
           CAD, 2024-10-01,      CHF,   0.6326
           CAD, 2024-11-01,      CHF,   0.6335
           CAD, 2024-12-01,      CHF,   0.6346
           USD, 2024-01-01,      CHF,   0.8808
           USD, 2024-02-01,      CHF,   0.8639
           USD, 2024-03-01,      CHF,   0.8817
           USD, 2024-04-01,      CHF,   0.8918
           USD, 2024-05-01,      CHF,   0.9165
           USD, 2024-06-01,      CHF,   0.9197
           USD, 2024-07-01,      CHF,   0.9061
           USD, 2024-08-01,      CHF,   0.9049
           USD, 2024-09-01,      CHF,   0.8754
           USD, 2024-10-01,      CHF,   0.8564
           USD, 2024-11-01,      CHF,    0.865
           USD, 2024-12-01,      CHF,   0.8846
        """
        JOURNAL_CSV = """
              date, account, contra, currency,     amount, report_amount, description
        2024-08-26,        ,   6953,      USD,    2408.10,       2060.85, Bruttozins Festgeldanlage 1
                  ,    1176,       ,      CHF,     721.30,              , Verrechnungssteuer 35%
                  ,    1903,       ,      USD,    1565.27,       1339.55, Nettozins Festgeldanlage 1
        2024-09-06,        ,   6954,      CAD,    1618.17,       1015.30, Bruttozins Festgeldanlage 2
                  ,    1176,       ,      CHF,     355.35,              , Verrechnungssteuer 35%
                  ,    1904,       ,      CAD,    1051.81,        659.95, Nettozins Festgeldanlage 2
        2024-10-11,        ,   6954,      CAD,    1522.11,        955.50, Bruttozins Festgeldanlage 3
                  ,    1176,       ,      CHF,     334.40,              , Verrechnungssteuer 35%
                  ,    1904,       ,      CAD,     989.37,        621.10, Nettozins Festgeldanlage 3
        """

        accounts = pd.read_csv(StringIO(ACCOUNTS_CSV), skipinitialspace=True)
        assets = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)
        price_history = pd.read_csv(StringIO(PRICE_CSV), skipinitialspace=True)
        journal = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)

        engine.reporting_currency = "CHF"
        engine.transitory_account = 1999
        engine.restore(
            accounts=accounts,
            assets=assets,
            price_history=price_history,
            journal=journal
        )

        # Check that transitory account (1999) balances to zero
        balance = engine.individual_account_balances(accounts=1999, period="2024")
        assert balance.query("account == 1999")["report_balance"].iloc[0] == 0.0
