"""This module contains constants used throughout the application."""

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

FX_REVALUATION_ACCOUNT_COLUMNS = {
    "foreign_currency_account": "int",
    "fx_gain_loss_account": "int",
    "exchange_rate": "float64",
}
