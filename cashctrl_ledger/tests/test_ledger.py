"""Unit tests for ledger accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from requests import RequestException
# flake8: noqa: F401
from base_test import BaseTestCashCtrl
from pyledger.tests import BaseTestLedger
from io import StringIO


class TestLedger(BaseTestCashCtrl, BaseTestLedger):

    @pytest.fixture()
    def engine(self, initial_engine):
        # Hack: when updating reporting currency transitory account currency should be updated
        initial_engine.restore(settings=self.SETTINGS)
        initial_engine.transitory_account = 9999
        return initial_engine

    def test_ledger_accessor_mutators(self, restored_engine):
        # Ledger entries need to be sanitized before adding to the CashCtrl
        self.LEDGER_ENTRIES = restored_engine.sanitize_ledger(self.LEDGER_ENTRIES)
        super().test_ledger_accessor_mutators(restored_engine, ignore_row_order=True)

    @pytest.mark.skip(reason="CashCtrl allows to create same entries.")
    def test_add_already_existed_raise_error(self):
        pass

    def test_add_ledger_with_illegal_attributes(self, restored_engine):
        ledger_entry = self.LEDGER_ENTRIES.query("id == '2'")

        # Add with non existent Tax code should raise an error
        target = ledger_entry.copy()
        target["tax_code"] = "Test_Non_Existent_TAX_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            restored_engine.ledger.add(target)

        # Add with non existent account should raise an error
        target = ledger_entry.copy()
        target["account"] = 33333
        with pytest.raises(ValueError, match="No id found for account"):
            restored_engine.ledger.add(target)

        # Add with non existent currency should raise an error
        target = ledger_entry.copy()
        target["currency"] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            restored_engine.ledger.add(target)

    def test_modify_non_existed_raises_error(self, restored_engine):
        super().test_modify_non_existed_raises_error(
            restored_engine, error_class=RequestException, error_message="entry does not exist"
        )

    def test_update_ledger_with_illegal_attributes(self, restored_engine):
        ledger_entry = self.LEDGER_ENTRIES.query("id == '2'")
        id = restored_engine.ledger.add(ledger_entry)[0]

        # Updating a ledger with non existent tax code should raise an error
        target = ledger_entry.copy()
        target["id"] = id
        target["tax_code"] = "Test_Non_Existent_TAX_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            restored_engine.ledger.modify(target)

        # Updating a ledger with non existent account code should raise an error
        target = ledger_entry.copy()
        target["id"] = id
        target["account"] = 333333
        with pytest.raises(ValueError, match="No id found for account"):
            restored_engine.ledger.modify(target)

        # Updating a ledger with non existent currency code should raise an error
        target = ledger_entry.copy()
        target["id"] = id
        target["currency"] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            restored_engine.ledger.modify(target)

        # Delete the ledger entry created above
        restored_engine.ledger.delete([{"id": id}])

    @pytest.mark.skip(reason="We don't have a mechanism to allow missing ids.")
    def test_delete_entry_allow_missing(self):
        pass

    def test_delete_nonexistent_entry_raise_error(self, restored_engine):
        with pytest.raises(RequestException, match="API call failed. ID missing."):
            restored_engine.ledger.delete({"id": ["FAKE_ID"]})

    def test_adding_transaction_with_two_non_reporting_currencies_fails(self, restored_engine):
        expected = (
            "CashCtrl allows only the reporting currency plus a single foreign currency"
        )
        entry = BaseTestLedger.LEDGER_ENTRIES.query("id == '23'")
        with pytest.raises(ValueError, match=expected):
            restored_engine.ledger.add(entry)

    def test_split_multi_currency_transactions(self, engine):
        transitory_account = 9999
        txn = engine.ledger.standardize(BaseTestLedger.LEDGER_ENTRIES.query("id == '10'"))
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
        txn = engine.ledger.standardize(BaseTestLedger.LEDGER_ENTRIES.query("id in ['10', '23']"))
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
