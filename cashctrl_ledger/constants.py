"""This module contains constants used throughout the application."""

JOURNAL_ITEM_COLUMNS = {
    "accountId": "int",
    "description": "string[python]",
    "debit": "float64",
    "credit": "float64",
    "taxName": "string[python]",
}

FX_REVALUATION_ACCOUNT_REQUIRED_COLUMNS = {
    "foreign_currency_account": "int",
    "fx_gain_loss_account": "Int64",
}

FX_REVALUATION_ACCOUNT_OPTIONAL_COLUMNS = {
    "exchange_rate": "Float64",
}
