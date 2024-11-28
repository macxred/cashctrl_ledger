"""Unit tests for ledger accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
# flake8: noqa: F401
from base_test import initial_engine
from pyledger.tests import BaseTestLedger
from io import StringIO


class TestLedger(BaseTestLedger):
    ACCOUNTS = BaseTestLedger.ACCOUNTS.copy()
    # Set the default root node, as CashCtrl does not allow the creation of root nodes
    ACCOUNTS.loc[:, "group"] = "/Assets"
    # TODO: Remove when Assets will be implemented
    ACCOUNTS.loc[ACCOUNTS["currency"] == "JPY", "currency"] = "USD"

    TAX_CODES = BaseTestLedger.TAX_CODES.copy()
    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    default_account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = default_account

    LEDGER_ENTRIES = BaseTestLedger.LEDGER_ENTRIES.copy()
    exclude_ids = ["1", "23", "10", "3", "9", "18"]
    # flake8: noqa: E501
    # "23": Transaction with CHF currency is correctly sanitized, but when reading - CHF is converted to USD and amount is recalculated to USD
    # LEDGER_ENTRIES = LEDGER_ENTRIES.query("id == '23'")
    # "1": Same as above - JPY converted to the USD
    # "10": Same as above - EUR converted to the USD
    # LEDGER_ENTRIES = LEDGER_ENTRIES.query("id == '1'")

    # "3": API call failed. Total debit (20 000.00) and total credit (40 000.00) must be equal. - Broken on our side (in code)
    # Probably broken in standardize method in this part '# Split collective transaction line items with both debit and credit into two items with a single account each'
    # Before standardize:
    # (Pdb) ledger
    #     id       date  account  contra currency    amount  report_amount tax_code         description                               document
    #     5  3 2024-04-12     <NA>    1000      USD  21288.24           <NA>     <NA>  Convert USD to EUR  2024/transfers/2024-04-12_USD-EUR.pdf
    #     6  3 2024-04-12     1010    1000      EUR   20000.0       21288.24     <NA>  Convert USD to EUR  2024/transfers/2024-04-12_USD-EUR.pdf
    #     (Pdb) cont
    # After:
    # (Pdb) ledger
    #     id       date  account  contra currency    amount  report_amount tax_code         description                               document
    #     5  3 2024-04-12     1000    <NA>      USD -21288.24           <NA>     <NA>  Convert USD to EUR  2024/transfers/2024-04-12_USD-EUR.pdf
    #     6  3 2024-04-12     1010    <NA>      EUR   20000.0       21288.24     <NA>  Convert USD to EUR  2024/transfers/2024-04-12_USD-EUR.pdf
    #     6  3 2024-04-12     1000    <NA>      EUR  -20000.0      -21288.24     <NA>  Convert USD to EUR  2024/transfers/2024-04-12_USD-EUR.pdf
    # LEDGER_ENTRIES = LEDGER_ENTRIES.query("id == '3'")

    # "9": requests.exceptions.RequestException: API call failed. Total debit (2 500.00) and total credit (500.00) must be equal.
    # LEDGER_ENTRIES = LEDGER_ENTRIES.query("id == '9'")

    # "18": Broken transaction amounts - API call failed. Total debit (0.00) and total credit (5.55) must be equal.

    LEDGER_ENTRIES = LEDGER_ENTRIES.query("id not in @exclude_ids")

    @pytest.fixture()
    def engine(self, initial_engine):
        initial_engine.restore(settings=self.SETTINGS)
        # Hack: when updating reporting currency transitory account currency should be updated
        initial_engine.transitory_account = 9999
        return initial_engine

    def test_ledger_accessor_mutators(self, restored_engine):
        self.LEDGER_ENTRIES = restored_engine.sanitize_ledger(self.LEDGER_ENTRIES)
        super().test_ledger_accessor_mutators(restored_engine, ignore_row_order=True)

    MULTI_CURRENCY_ENTRIES_CSV = """
        id,     date,  account, contra, currency,     amount, report_amount, tax_code,   description,                     document
        1, 2024-06-26,       ,   9991,      USD,  100000.00,      90000.00,         ,   Convert 100k USD to EUR @ 0.9375,
        1, 2024-06-26,   9990,       ,      EUR,   93750.00,      90000.00,         ,   Convert 100k USD to EUR @ 0.9375,
        2, 2024-06-26,       ,   9991,      USD,  200000.00,     180000.00,         ,   Convert 200k USD to EUR and CHF,
        2, 2024-06-26,   9990,       ,      EUR,   93750.00,      90000.00,         ,   Convert 200k USD to EUR and CHF,
        2, 2024-06-26,   9992,       ,      CHF,   90000.00,      90000.00,         ,   Convert 200k USD to EUR and CHF,
    """
    MULTI_CURRENCY_ENTRIES = pd.read_csv(StringIO(MULTI_CURRENCY_ENTRIES_CSV), skipinitialspace=True)

    def test_split_multi_currency_transactions(self, engine):
        engine.reporting_currency = "CHF"
        transitory_account = 9995
        txn = engine.ledger.standardize(self.MULTI_CURRENCY_ENTRIES.query("id == 1"))
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
        engine.reporting_currency = "CHF"
        transitory_account = 9995
        txn = engine.ledger.standardize(self.MULTI_CURRENCY_ENTRIES.query("id.isin([1, 2])"))
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
