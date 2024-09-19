"""Unit tests for ledger accessors, mutators and mirroring."""

import pytest
import pandas as pd
from requests.exceptions import RequestException
from pyledger import BaseTestLedger
from consistent_df import assert_frame_equal
# flake8: noqa: F401
from base_test import initial_ledger


class TestAccounts(BaseTestLedger):
    @pytest.fixture(scope="module")
    def ledger(self, initial_ledger):
        initial_ledger.restore(accounts=self.ACCOUNTS, vat_codes=self.VAT_CODES)
        return initial_ledger

    @pytest.mark.parametrize(
        "ledger_id", set(BaseTestLedger.LEDGER_ENTRIES["id"].unique()).difference([15, 16, 17, 18])
    )
    def test_add_ledger_entry(self, ledger, ledger_id):
        ledger.restore(accounts=self.ACCOUNTS, vat_codes=self.VAT_CODES)
        target = self.LEDGER_ENTRIES.query("id == @ledger_id")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(
            created, expected, ignore_index=True, ignore_columns=["id"], check_exact=True
        )

    def test_add_ledger_with_non_existing_vat(self, ledger):
        # Adding a ledger entry with non existing VAT code should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["vat_code"].iat[0] = "Test_Non_Existent_VAT_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            ledger.add_ledger_entry(target)

    def test_add_ledger_with_non_existing_account(self, ledger):
        # Adding a ledger entry with non existing account should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["account"] = 33333
        with pytest.raises(ValueError, match="No id found for account"):
            ledger.add_ledger_entry(target)

    def test_add_ledger_with_non_existing_currency(self, ledger):
        # Adding a ledger entry with non existing currency code should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["currency"] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            ledger.add_ledger_entry(target)

    @pytest.mark.parametrize("id", [15, 16])
    def test_adding_transaction_with_two_non_base_currencies_fails(self, ledger, id):
        target = self.LEDGER_ENTRIES[self.LEDGER_ENTRIES["id"] == id]
        expected = (
            "CashCtrl allows only the base currency plus a single foreign currency"
        )
        with pytest.raises(ValueError, match=expected):
            ledger.add_ledger_entry(target)

    def test_modify_non_existed_raises_error(self, ledger):
        super().test_modify_non_existed_raises_error(
            ledger, error_class=RequestException, error_message="entry does not exist"
        )

    def test_update_ledger_with_illegal_attributes(self, ledger):
        id = ledger.add_ledger_entry(self.LEDGER_ENTRIES.query("id == 1"))

        # Updating a ledger with non existent VAT code should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = id
        target["vat_code"] = "Test_Non_Existent_VAT_code"
        with pytest.raises(ValueError, match="No id found for tax code"):
            ledger.modify_ledger_entry(target)

        # Updating a ledger with non existent account code should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = id
        target["account"].iat[0] = 333333
        with pytest.raises(ValueError, match="No id found for account"):
            ledger.modify_ledger_entry(target)

        # Updating a ledger with non existent currency code should raise an error
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = id
        target["currency"].iat[0] = "Non_Existent_Currency"
        with pytest.raises(ValueError, match="No id found for currency"):
            ledger.modify_ledger_entry(target)

        # Delete the ledger entry created above
        ledger.delete_ledger_entry(id)

    def test_update_non_existent_ledger(self, ledger):
        target = self.LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = 999999
        with pytest.raises(RequestException):
            ledger.modify_ledger_entry(target)

    def test_delete_non_existent_ledger(self, ledger):
        with pytest.raises(RequestException):
            ledger.delete_ledger_entry(ids="non-existent")

    def test_split_multi_currency_transactions(self, ledger):
        transitory_account = 9995
        txn = ledger.standardize_ledger(self.LEDGER_ENTRIES.query("id == 15"))
        spit_txn = ledger.split_multi_currency_transactions(
            txn, transitory_account=transitory_account
        )
        is_base_currency = spit_txn["currency"] == ledger.base_currency
        spit_txn.loc[is_base_currency, "base_currency_amount"] = spit_txn.loc[
            is_base_currency, "amount"
        ]
        assert len(spit_txn) == len(txn) + 2, "Expecting two new lines when transaction is split"
        assert sum(spit_txn["account"] == transitory_account) == 2, (
            "Expecting two transactions on transitory account"
        )
        assert all(spit_txn.groupby("id")["base_currency_amount"].sum() == 0), (
            "Expecting split transactions to be balanced"
        )
        assert spit_txn.query("account == @transitory_account")["base_currency_amount"].sum() == 0, (
            "Expecting transitory account to be balanced"
        )

    def test_split_several_multi_currency_transactions(self, ledger):
        transitory_account = 9995
        txn = ledger.standardize_ledger(self.LEDGER_ENTRIES.query("id.isin([15, 16])"))
        spit_txn = ledger.split_multi_currency_transactions(
            txn, transitory_account=transitory_account
        )
        is_base_currency = spit_txn["currency"] == ledger.base_currency
        spit_txn.loc[is_base_currency, "base_currency_amount"] = spit_txn.loc[
            is_base_currency, "amount"
        ]
        id_currency_pairs = (txn["id"] + txn["currency"]).nunique()
        assert len(spit_txn) == len(txn) + id_currency_pairs, (
            "Expecting one new line per currency and transaction"
        )
        assert sum(spit_txn["account"] == transitory_account) == id_currency_pairs, (
            "Expecting one transaction on transitory account per id and currency"
        )
        assert all(spit_txn.groupby("id")["base_currency_amount"].sum() == 0), (
            "Expecting split transactions to be balanced"
        )
        assert spit_txn.query("account == @transitory_account")["base_currency_amount"].sum() == 0, (
            "Expecting transitory account to be balanced"
        )
