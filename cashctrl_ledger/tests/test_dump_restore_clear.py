"""Unit tests for testing dump, restore, and clear logic."""

import polars as pl
import pytest
import zipfile
import json
from pyledger.tests import BaseTestDumpRestoreClear, assert_frame_equal
from base_test import BaseTestCashCtrl


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


class TestDumpRestoreClear(BaseTestCashCtrl, BaseTestDumpRestoreClear):
    JOURNAL = BaseTestCashCtrl.JOURNAL.filter(pl.col("id").is_in(["2", "5", "6", "7"]))

    @pytest.fixture(scope="class")
    @classmethod
    def engine(cls, initial_engine):
        # Temporarily set the transitory account to the first listed account for simpler testing
        initial_transitory_account = initial_engine.transitory_account
        initial_engine.transitory_account = cls.ACCOUNTS[0, "account"]

        yield initial_engine

        # Restore initial transitory account
        initial_engine.transitory_account = initial_transitory_account

    def test_restore_configuration(self, engine, tmp_path):
        self.ACCOUNTS = engine.sanitize_accounts(self.ACCOUNTS, pandas=False)
        engine.restore(
            accounts=self.ACCOUNTS, tax_codes=self.TAX_CODES, configuration=CONFIGURATION
        )
        engine.dump_to_zip(tmp_path / "system.zip")
        with zipfile.ZipFile(tmp_path / "system.zip", 'r') as archive:
            configuration = json.loads(archive.open('configuration.json').read().decode('utf-8'))
            default_roundings = pl.DataFrame(CONFIGURATION["ROUNDING"])
            roundings = pl.DataFrame(configuration.get("ROUNDING", None))
            columns = [c for c in roundings.columns if c in default_roundings.columns]
            roundings = roundings.select(columns)
            system_configuration = configuration.get("CASH_CTRL", None)
            reporting_currency = configuration.get("REPORTING_CURRENCY", None)

            assert_frame_equal(default_roundings, roundings, check_like=True)
            assert reporting_currency == CONFIGURATION["REPORTING_CURRENCY"]
            assert system_configuration == CONFIGURATION["CASH_CTRL"]
