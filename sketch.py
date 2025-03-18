"""
Execute:
    CC_API_ORGANISATION=<ORG> CC_API_KEY=<API_KEY> python sketch.py
"""

from io import StringIO
import pandas as pd
from cashctrl_ledger import ExtendedCashCtrlLedger
from consistent_df import assert_frame_equal

ACCOUNT_CSV = """
    group,                         account, currency, tax_code, description
    /Assets/Current Assets,           1999,      USD,         , Transitory Account for CashCtrl rounding precision
    /Liabilities/Current Liabilites,  2200,      USD,         , VAT Payable (Output VAT)
    /Revenue/Sales,                   4001,      EUR,         , Sales Revenue - EUR
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

JOURNAL_CSV = """
                id,       date, account, contra, currency,    amount, report_amount, tax_code,                   description,                   document
default.csv:11:tax, 2024-07-01,    4001,       ,      EUR,    166.67,        178.47,         , TAX: Sale at mixed VAT rate,  /invoices/invoice_002.pdf
default.csv:11:tax, 2024-07-01,    4001,       ,      EUR,     23.81,         25.50,         , TAX: Sale at mixed VAT rate,  /invoices/invoice_002.pdf
default.csv:11:tax, 2024-07-01,    2200,       ,      EUR,   -166.67,       -178.47,         , TAX: Sale at mixed VAT rate,  /invoices/invoice_002.pdf
default.csv:11:tax, 2024-07-01,    2200,       ,      EUR,    -23.81,        -25.50,         , TAX: Sale at mixed VAT rate,  /invoices/invoice_002.pdf
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)

cashctrl = ExtendedCashCtrlLedger(transitory_account=1999)
cashctrl.restore(configuration={"reporting_currency": "USD"}, accounts=ACCOUNTS)

JOURNAL = cashctrl.journal.standardize(JOURNAL)
cashctrl.journal.mirror(JOURNAL, delete=True)
remote = cashctrl.journal.list()
print(remote)
#     id       date  account  contra currency  amount  report_amount tax_code                  description                   document profit_center
# 0  843 2024-07-01     4001    <NA>      EUR  166.67         178.47     <NA>  TAX: Sale at mixed VAT rate  /invoices/invoice_002.pdf          <NA>
# 1  843 2024-07-01     4001    <NA>      EUR   23.81           25.5     <NA>  TAX: Sale at mixed VAT rate  /invoices/invoice_002.pdf          <NA>
# 2  843 2024-07-01     2200    <NA>      USD -178.47           <NA>     <NA>  TAX: Sale at mixed VAT rate  /invoices/invoice_002.pdf          <NA>
# 3  843 2024-07-01     2200    <NA>      USD   -25.5           <NA>     <NA>  TAX: Sale at mixed VAT rate  /invoices/invoice_002.pdf          <NA>

assert_frame_equal(remote, JOURNAL, ignore_index=True, ignore_columns=["id"])
# AssertionError: DataFrame.iloc[:, 3] (column name="currency") are different
# DataFrame.iloc[:, 3] (column name="currency") values are different (50.0 %)
# [index]: [0, 1, 2, 3]
# [left]:  [EUR, EUR, USD, USD]
# [right]: [EUR, EUR, EUR, EUR]
# At positional index 2, first diff: USD != EUR
