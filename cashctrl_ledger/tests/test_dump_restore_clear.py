"""Unit tests for testing dump, restore, and clear logic."""

import pandas as pd
import pytest
import zipfile
import json
from pyledger.tests import BaseTestDumpRestoreClear
# flake8: noqa: F401
from base_test import initial_engine
from cashctrl_ledger.constants import SETTINGS_KEYS
from consistent_df import assert_frame_equal


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
    "REPORTING_CURRENCY": "CHF",
    "DEFAULT_ROUNDINGS":[
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


class TestDumpRestoreClear(BaseTestDumpRestoreClear):

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        initial_engine.clear()
        return initial_engine

    @pytest.mark.skip(reason="We don't have implemented functionality for this yet.")
    def test_restore(self):
        pass

    @pytest.mark.skip(reason="We don't have implemented functionality for this yet.")
    def test_dump_and_restore_zip(self):
        pass

    @pytest.mark.skip(reason="We don't have implemented functionality for this yet.")
    def test_clear(self):
        pass

    def test_restore_settings(self, engine, tmp_path):
        engine.restore(settings=SETTINGS)
        engine.dump_to_zip(tmp_path / "system.zip")
        with zipfile.ZipFile(tmp_path / "system.zip", 'r') as archive:
            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            roundings = settings.get("DEFAULT_ROUNDINGS", None)
            reporting_currency = settings.get("REPORTING_CURRENCY", None)
            system_settings = settings.get("DEFAULT_SETTINGS", None)
            settings["DEFAULT_SETTINGS"]

            for key in SETTINGS_KEYS:
                if system_settings.get(key, None) is not None:
                    system_settings[key] = engine._client.account_to_id(system_settings[key])
            if roundings is not None:
                for rounding in roundings:
                    rounding["accountId"] = engine._client.account_to_id(rounding["account"])

            roundings = pd.DataFrame(roundings)
            default_roundings = pd.DataFrame(SETTINGS["DEFAULT_ROUNDINGS"])
            columns = roundings.columns.intersection(default_roundings.columns)
            roundings = roundings[columns]

            assert_frame_equal(default_roundings, roundings, check_like=True)
            assert reporting_currency == SETTINGS["REPORTING_CURRENCY"]
            assert system_settings == SETTINGS["DEFAULT_SETTINGS"]
