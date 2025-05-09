"""Unit tests for journal accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from requests import RequestException
# flake8: noqa: F401
from base_test import BaseTestCashCtrl
from pyledger.tests import BaseTestJournal
from consistent_df import assert_frame_equal
from io import StringIO


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

    def test_txn_in_non_reporting_currency_assigned_to_account_in_reporting_currency(
        self, restored_engine
    ):
        """Ensure that when a transaction is recorded in a non-reporting currency
        but assigned to an account in the reporting currency, the original row-level
        currency is preserved through a creation–read round-trip."""
        # flake8: noqa: E501
        JOURNAL_CSV = """
        id,       date, account, contra, currency,    amount, report_amount, tax_code,  description, document
         1, 2024-07-01,    4001,       ,      EUR,    166.67,        178.47,         ,     test txn,
         1, 2024-07-01,    4001,       ,      EUR,     23.81,         25.50,         ,     test txn,
         1, 2024-07-01,    2200,       ,      EUR,   -166.67,       -178.47,         ,     test txn,
         1, 2024-07-01,    2200,       ,      EUR,    -23.81,        -25.50,         ,     test txn,
        """
        # flake8: enable
        JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)
        JOURNAL = restored_engine.journal.standardize(JOURNAL)
        restored_engine.journal.mirror(JOURNAL, delete=True)
        remote = restored_engine.journal.list()
        assert_frame_equal(remote, JOURNAL, ignore_index=True, ignore_columns=["id"])
