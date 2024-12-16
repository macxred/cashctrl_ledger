"""Unit tests for testing dump, restore, and clear logic."""

import pandas as pd
import pytest
import zipfile
import json
from io import StringIO
from pyledger.tests import BaseTestDumpRestoreClear
# flake8: noqa: F401
from base_test import initial_engine
from consistent_df import assert_frame_equal


ACCOUNT_CSV = """
      group,  account, currency, tax_code, description
    /Assets,     9100,      CHF,         , Opening Account
    /Assets,     1172,      CHF,         , Input Tax Adjustment Account
    /Assets,     7900,      CHF,         , Inventory Asset Revenue Account
    /Assets,     6800,      CHF,         , Inventory Depreciation Account
    /Assets,     9200,      CHF,         , Profit Allocation Account
    /Assets,     2202,      CHF,         , Sales Tax Adjustment Account
    /Assets,     3200,      CHF,         , Inventory Article Revenue Account
    /Assets,     4200,      CHF,         , Inventory Article Expense Account
    /Assets,     1100,      CHF,         , Debtor Account
    /Assets,     6801,      CHF,         , Inventory Disposal Account
    /Assets,     6960,      CHF,         , Exchange Difference Account
    /Assets,     2000,      CHF,         , Creditor Account
    /Assets,     6961,      CHF,         , Round Account
    /Assets,     1000,      CHF,         , Transitory Account
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
SETTINGS = {
    "CASH_CTRL": {
        "DEFAULT_OPENING_ACCOUNT_ID": 9100,
        "DEFAULT_INPUT_TAX_ADJUSTMENT_ACCOUNT_ID": 1172,
        "DEFAULT_INVENTORY_ASSET_REVENUE_ACCOUNT_ID": 7900,
        "DEFAULT_INVENTORY_DEPRECIATION_ACCOUNT_ID": 6800,
        "DEFAULT_PROFIT_ALLOCATION_ACCOUNT_ID": 9200,
        "DEFAULT_SALES_TAX_ADJUSTMENT_ACCOUNT_ID": 2202,
        "DEFAULT_INVENTORY_ARTICLE_REVENUE_ACCOUNT_ID": 3200,
        "DEFAULT_INVENTORY_ARTICLE_EXPENSE_ACCOUNT_ID": 4200,
        "DEFAULT_DEBTOR_ACCOUNT_ID": 1100,
        "DEFAULT_INVENTORY_DISPOSAL_ACCOUNT_ID": 6801,
        "DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID": 6960,
        "DEFAULT_CREDITOR_ACCOUNT_ID": 2000
    },
    "REPORTING_CURRENCY": "CHF",
    "ROUNDING":[
        {
            "account": 6961,
            "name": "<values><de>Auf 0.05 runden</de><en>Round to 0.05</en></values>",
            "rounding": 0.05,
            "mode": "HALF_UP",
            "value": None,
            "referenced": False
        },
        {
            "account": 6961,
            "name": "<values><de>Auf 1.00 runden</de><en>Round to 1.00</en></values>",
            "rounding": 1.0,
            "mode": "HALF_UP",
            "value": None,
            "referenced": False
        }
    ]
}


# Revaluations can not be implemented in CashCtrl
# Defining a placeholder class to satisfy test interface
class Revaluations:
    def list(self):
        return pd.DataFrame({})
    def mirror(self, target):
        pass


class TestDumpRestoreClear(BaseTestDumpRestoreClear):
    LEDGER_ENTRIES = BaseTestDumpRestoreClear.LEDGER_ENTRIES.query("id.isin(['2', '5', '6', '7'])")
    TAX_CODES = BaseTestDumpRestoreClear.TAX_CODES.copy()
    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    default_account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = default_account

    # Revaluations can not be dumped or restored in CashCtrl, using empty DataFrame
    REVALUATIONS = pd.DataFrame({})

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        self.ACCOUNTS = initial_engine.sanitize_accounts(self.ACCOUNTS)
        initial_engine._revaluations = Revaluations()
        # Set transitory account as first from constants to simplify the test logic
        initial_transitory_account = initial_engine.transitory_account
        initial_engine.transitory_account = self.ACCOUNTS.iloc[0]["account"].item()

        yield initial_engine

        initial_engine.transitory_account = initial_transitory_account

    def test_restore_settings(self, engine, tmp_path):
        engine.restore(ledger=pd.DataFrame({}), accounts=ACCOUNTS, settings=SETTINGS)
        engine.dump_to_zip(tmp_path / "system.zip")
        with zipfile.ZipFile(tmp_path / "system.zip", 'r') as archive:
            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            default_roundings = pd.DataFrame(SETTINGS["ROUNDING"])
            roundings = pd.DataFrame(settings.get("ROUNDING", None))
            columns = roundings.columns.intersection(default_roundings.columns)
            roundings = roundings[columns]
            system_settings = settings.get("CASH_CTRL", None)
            reporting_currency = settings.get("REPORTING_CURRENCY", None)

            assert_frame_equal(default_roundings, roundings, check_like=True)
            assert reporting_currency == SETTINGS["REPORTING_CURRENCY"]
            assert system_settings == SETTINGS["CASH_CTRL"]
