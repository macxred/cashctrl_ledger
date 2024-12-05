"""This module contains constants used throughout the application."""

import pandas as pd
from io import StringIO


JOURNAL_ITEM_COLUMNS = {
    "accountId": "int",
    "description": "string[python]",
    "debit": "float64",
    "credit": "float64",
    "taxName": "string[python]",
}


SETTINGS_KEYS = [
    "DEFAULT_OPENING_ACCOUNT_ID",
    "DEFAULT_INPUT_TAX_ADJUSTMENT_ACCOUNT_ID",
    "DEFAULT_INVENTORY_ASSET_REVENUE_ACCOUNT_ID",
    "DEFAULT_INVENTORY_DEPRECIATION_ACCOUNT_ID",
    "DEFAULT_PROFIT_ALLOCATION_ACCOUNT_ID",
    "DEFAULT_SALES_TAX_ADJUSTMENT_ACCOUNT_ID",
    "DEFAULT_INVENTORY_ARTICLE_REVENUE_ACCOUNT_ID",
    "DEFAULT_INVENTORY_ARTICLE_EXPENSE_ACCOUNT_ID",
    "DEFAULT_DEBTOR_ACCOUNT_ID",
    "DEFAULT_INVENTORY_DISPOSAL_ACCOUNT_ID",
    "DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID",
    "DEFAULT_CREDITOR_ACCOUNT_ID"
]


FX_REVALUATION_SCHEMA_CSV = """
    column,                     dtype,  mandatory,   id
    foreign_currency_account,     int,       True,   False
    fx_gain_loss_account,       Int64,       True,   False
    exchange_rate,            Float64,      False,   False
"""
FX_REVALUATION_SCHEMA = pd.read_csv(StringIO(FX_REVALUATION_SCHEMA_CSV), skipinitialspace=True)
