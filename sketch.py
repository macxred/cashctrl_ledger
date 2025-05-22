"""
Execute:
    CC_API_ORGANISATION=<ORG> CC_API_KEY=<API_KEY> python sketch.py
"""

from datetime import date
from io import StringIO
import pandas as pd
from cashctrl_ledger import ExtendedCashCtrlLedger
from pyledger import MemoryLedger

ACCOUNT_CSV = """
    group,                         account, currency, tax_code, description
    /Assets/Current Assets,           1999,      USD,         , Transitory Account for CashCtrl rounding precision
    /Revenue/Sales,                   2200,      USD,         , VAT Payable (Output VAT)
    /Revenue/Sales,                   4001,      EUR,         , Sales Revenue - EUR
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
PRICES_CSV = """
          date, ticker,  price, currency
    2023-12-29,    EUR, 1.1068, USD
"""
PRICES = pd.read_csv(StringIO(PRICES_CSV), skipinitialspace=True)

JOURNAL_CSV = """
id,       date, account, contra, currency,    amount, report_amount, tax_code, description,
 1, 2024-07-01,    4001,       ,      EUR,       100,              ,         ,    2024 txn,
 1, 2024-07-01,        ,   2200,      EUR,       100,              ,         ,    2024 txn,
 2, 2025-07-01,    4001,       ,      EUR,       200,              ,         ,    2025 txn,
 2, 2025-07-01,        ,   2200,      EUR,       200,              ,         ,    2025 txn,
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)

source = MemoryLedger()
source.restore(configuration={"REPORTING_CURRENCY": "USD"}, accounts=ACCOUNTS, price_history=PRICES, journal=JOURNAL)
remote = ExtendedCashCtrlLedger(transitory_account=1999)
remote.restore(configuration={"REPORTING_CURRENCY": "USD"}, accounts=ACCOUNTS, price_history=PRICES, journal=JOURNAL)

# Define the test date ranges
fiscal_2024_start = date(2024, 1, 12)
fiscal_2025_end = date(2025, 12, 31)
account = 4001

source_balance = source.account_balance(account, "2025-12-31")
remote_balance = remote.account_balance(account, "2025-12-31")

print(source_balance)
print(remote_balance)
# {'reporting_currency': 332.04, 'EUR': 300.0}
# {'reporting_currency': 332.04, 'EUR': 300.0}

assert source_balance == remote_balance