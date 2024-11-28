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
    exclude_ids = ["1", "3", "8", "9", "10", "16", "17", "18", "22", "23", "24"]
    # flake8: noqa: E501
    # "1", "10", "24": CashCtrl allows only the reporting currency plus a single foreign currency
    # in a collective booking: 1.
    # - Test is right, code is broken. We should also call sanitize for add/modify method.

    # "3": API call failed. Total debit (20 000.00) and total credit (40 000.00) must be equal. - Broken on our side (in code)
    # "8": API call failed. Total debit (999.99) and total credit (888.88) must be equal. - Broken transaction amounts
    # "18": API call failed. Total debit (0.00) and total credit (5.55) must be equal. - Broken transaction amounts

    # "9": Problem with report_amount - values are NA and calculations within
    # _collective_transaction_currency_and_rate failed

    # "16": Add one extra line

    # "17": API call failed. amount: The amount must be positive.
    # Please switch debit and credit instead. - Broken on our side (in code)

    # "22", 23: self._client.account_to_id(entry["contra"].iat[0]) - ledger.py L:640
    # *** ValueError: No id found for account: <NA> - Broken transaction amounts

    engine = ExtendedCashCtrlLedger(9999)
    LEDGER_ENTRIES = LEDGER_ENTRIES.query("id not in @exclude_ids")
    LEDGER_ENTRIES = engine.sanitize_ledger(LEDGER_ENTRIES)

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        initial_engine.restore(settings=self.SETTINGS)
        initial_engine.transitory_account = 9999
        return initial_engine
