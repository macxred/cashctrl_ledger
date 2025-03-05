"""Unit tests for testing dump, restore, and clear logic."""

import pandas as pd
import pytest
import zipfile
import json
from pyledger.tests import BaseTestDumpRestoreClear
from base_test import BaseTestCashCtrl
from consistent_df import assert_frame_equal


CONFIGURATION = {
    "CASH_CTRL": {
        "DEFAULT_OPENING_ACCOUNT_ID": 1000,
        "DEFAULT_INPUT_TAX_ADJUSTMENT_ACCOUNT_ID": 1005,
        "DEFAULT_INVENTORY_ASSET_REVENUE_ACCOUNT_ID": 1010,
        "DEFAULT_INVENTORY_DEPRECIATION_ACCOUNT_ID": 1015,
        "DEFAULT_PROFIT_ALLOCATION_ACCOUNT_ID": 1020,
        "DEFAULT_SALES_TAX_ADJUSTMENT_ACCOUNT_ID": 1025,
        "DEFAULT_INVENTORY_ARTICLE_REVENUE_ACCOUNT_ID": 1300,
        "DEFAULT_INVENTORY_ARTICLE_EXPENSE_ACCOUNT_ID": 2000,
        "DEFAULT_DEBTOR_ACCOUNT_ID": 2010,
        "DEFAULT_INVENTORY_DISPOSAL_ACCOUNT_ID": 2200,
        "DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID": 3000,
        "DEFAULT_CREDITOR_ACCOUNT_ID": 4000
    },
    "REPORTING_CURRENCY": "CHF",
    "ROUNDING": [
        {
            "account": 4001,
            "name": "<values><de>Auf 0.05 runden</de><en>Round to 0.05</en></values>",
            "rounding": 0.05,
            "mode": "HALF_UP",
            "value": None,
        },
        {
            "account": 4001,
            "name": "<values><de>Auf 1.00 runden</de><en>Round to 1.00</en></values>",
            "rounding": 1.0,
            "mode": "HALF_UP",
            "value": None,
        }
    ]
}


# Revaluations are not implemented in CashCtrl.
# A placeholder class is used to fulfill the test interface.
class Revaluations:
    def list(self):
        return pd.DataFrame({})

    def mirror(self, target):
        pass


class TestDumpRestoreClear(BaseTestCashCtrl, BaseTestDumpRestoreClear):
    JOURNAL = BaseTestCashCtrl.JOURNAL.query("id.isin(['2', '5', '6', '7'])")
    # CashCtrl doesn't support revaluations, use an empty DataFrame
    REVALUATIONS = pd.DataFrame({})

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        initial_engine._revaluations = Revaluations()
        # Temporarily set the transitory account to the first listed account for simpler testing
        initial_transitory_account = initial_engine.transitory_account
        initial_engine.transitory_account = self.ACCOUNTS.iloc[0]["account"].item()

        yield initial_engine

        # Restore initial transitory account
        initial_engine.transitory_account = initial_transitory_account

    def test_restore_configuration(self, engine, tmp_path):
        self.ACCOUNTS = engine.sanitize_accounts(self.ACCOUNTS)
        engine.restore(
            accounts=self.ACCOUNTS, tax_codes=self.TAX_CODES, configuration=CONFIGURATION
        )
        engine.dump_to_zip(tmp_path / "system.zip")
        with zipfile.ZipFile(tmp_path / "system.zip", 'r') as archive:
            configuration = json.loads(archive.open('configuration.json').read().decode('utf-8'))
            default_roundings = pd.DataFrame(CONFIGURATION["ROUNDING"])
            roundings = pd.DataFrame(configuration.get("ROUNDING", None))
            columns = roundings.columns.intersection(default_roundings.columns)
            roundings = roundings[columns]
            system_configuration = configuration.get("CASH_CTRL", None)
            reporting_currency = configuration.get("REPORTING_CURRENCY", None)

            assert_frame_equal(default_roundings, roundings, check_like=True)
            assert reporting_currency == CONFIGURATION["REPORTING_CURRENCY"]
            assert system_configuration == CONFIGURATION["CASH_CTRL"]
