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


CONFIGURATION_KEYS = [
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


FISCAL_PERIOD_SCHEMA_CSV = """
       column,              dtype,   mandatory,     id
           id,              Int64,        True,     True
        start,     datetime64[ns],        True,     False
          end,     datetime64[ns],        True,     False
         name,     string[python],        True,     False
    isCurrent,               bool,        True,     False
"""
FISCAL_PERIOD_SCHEMA = pd.read_csv(StringIO(FISCAL_PERIOD_SCHEMA_CSV), skipinitialspace=True)


ACCOUNT_ROOT_CATEGORIES = ["Assets", "Balance", "Expense", "Liabilities & Equity", "Revenue"]
ACCOUNT_CATEGORIES_NEED_TO_NEGATE = ["/Liabilities & Equity", "/Revenue"]
