"""This module contains constants used throughout the application."""

from pyledger.schema import read_schema as _read_schema_pl


JOURNAL_ITEM_COLUMNS = [
    "accountId", "description", "debit", "credit",
    "taxCode", "associateName",
]


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
    isCurrent,            boolean,        True,     False
"""
FISCAL_PERIOD_SCHEMA = _read_schema_pl(FISCAL_PERIOD_SCHEMA_CSV)


REPORT_ELEMENT_SCHEMA_CSV = """
       column,              dtype,   mandatory,     id
   endAmount2,            Float64,       False,     False
 dcEndAmount2,            Float64,       False,     False
    accountId,              Int64,       False,     True
 currencyCode,     string[python],       False,     False
         path,     string[python],       False,     False
"""
REPORT_ELEMENT = _read_schema_pl(REPORT_ELEMENT_SCHEMA_CSV)


ACCOUNT_ROOT_CATEGORIES = ["Assets", "Balance", "Expense", "Liabilities & Equity", "Revenue"]
ACCOUNT_CATEGORIES_NEED_TO_NEGATE = ["/Liabilities & Equity", "/Revenue"]

REPORTING_CURRENCY_TAG = "Reporting currency"
