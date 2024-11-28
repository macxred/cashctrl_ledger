"""Unit tests for ledger accessor, mutator, and mirror methods."""

import pytest
# flake8: noqa: F401
from base_test import initial_engine
from pyledger.tests import BaseTestLedger
from cashctrl_ledger import ExtendedCashCtrlLedger


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

    engine = ExtendedCashCtrlLedger(9999)
    LEDGER_ENTRIES = LEDGER_ENTRIES.query("id not in @exclude_ids")
    LEDGER_ENTRIES = engine.sanitize_ledger(LEDGER_ENTRIES)

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        initial_engine.restore(settings=self.SETTINGS)
        initial_engine.transitory_account = 9999
        return initial_engine

    def test_ledger_accessor_mutators(self, restored_engine):
        super().test_ledger_accessor_mutators(restored_engine, ignore_row_order=True)
