"""Unit tests for testing dump, restore, and clear logic."""

import pandas as pd
import pytest
import zipfile
import json
from io import StringIO
from pyledger import BaseTestDumpRestoreClear
# flake8: noqa: F401
from base_test import initial_ledger
from cashctrl_ledger.constants import SETTINGS_KEYS
from consistent_df import assert_frame_equal


ACCOUNT_CSV = """
      group,  account, currency, vat_code, text
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
    /Assets,     9999,      CHF,         , Transitory Account
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
SETTINGS = {
    "DEFAULT_SETTINGS": {
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
    "BASE_CURRENCY": "CHF",
    "DEFAULT_ROUNDINGS":[
        {
            "accountId": 6961,
            "name": "<values><de>Auf 0.05 runden</de><en>Round to 0.05</en></values>",
            "rounding": 0.05,
            "mode": "HALF_UP",
            "text": None,
            "value": None,
            "referenced": False
        },
        {
            "accountId": 6961,
            "name": "<values><de>Auf 1.00 runden</de><en>Round to 1.00</en></values>",
            "rounding": 1.0,
            "mode": "HALF_UP",
            "text": None,
            "value": None,
            "referenced": False
        }
    ]
}


class TestDumpRestoreClear(BaseTestDumpRestoreClear):
    @pytest.fixture()
    def ledger(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger

    def test_restore_settings(self, ledger, tmp_path):
        ledger.restore(ledger=pd.DataFrame({}), accounts=ACCOUNTS, settings=SETTINGS)
        ledger.dump_to_zip(tmp_path / "system.zip")
        with zipfile.ZipFile(tmp_path / "system.zip", 'r') as archive:
            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            roundings = settings.get("DEFAULT_ROUNDINGS", None)
            base_currency = settings.get("BASE_CURRENCY", None)
            system_settings = settings.get("DEFAULT_SETTINGS", None)
            settings["DEFAULT_SETTINGS"]

            for key in SETTINGS_KEYS:
                if system_settings.get(key, None) is not None:
                    system_settings[key] = ledger._client.account_to_id(system_settings[key])
            if roundings is not None:
                for rounding in roundings:
                    rounding["accountId"] = ledger._client.account_to_id(rounding["accountId"])

            roundings = pd.DataFrame(roundings)
            default_roundings = pd.DataFrame(SETTINGS["DEFAULT_ROUNDINGS"])
            columns = roundings.columns.intersection(default_roundings.columns)
            roundings = roundings[columns]

            assert_frame_equal(default_roundings, roundings)
            assert base_currency == SETTINGS["BASE_CURRENCY"]
            assert system_settings == SETTINGS["DEFAULT_SETTINGS"]
