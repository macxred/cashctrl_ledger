"""This module contains constants used throughout the application."""

JOURNAL_ITEM_COLUMNS = {
    "accountId": "int",
    "description": "string[python]",
    "debit": "float64",
    "credit": "float64",
    "taxName": "string[python]",
}

FX_REVALUATION_ACCOUNT_COLUMNS = {
    "foreign_currency_account": "int",
    "fx_gain_loss_account": "int",
    "exchange_rate": "float64",
}
