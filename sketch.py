"""
Execute:
    python -i sketch.py
"""

from io import StringIO
import pandas as pd
from pyledger import TextLedger
from cashctrl_ledger import ExtendedCashCtrlLedger
from consistent_df import assert_frame_equal

source = TextLedger()
engine = ExtendedCashCtrlLedger(1999)

ACCOUNT_CSV = """
group,    account, currency, description
/Assets,     1000,      USD, Cash in Bank USD
/Assets,     1010,      EUR, Cash in Bank EUR
/Assets,     1020,      JPY, Cash in Bank JPY
/Assets,     1999,      USD, Transitory Account for CashCtrl rounding precision
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

ASSETS_CSV = """
ticker, increment
   USD,      0.01
   EUR,      0.01
   JPY,       1.0
"""
ASSETS = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)

PRICE_CSV = """
ticker,       date, currency,  price
   CHF, 2023-12-29,      USD,  0.007
   EUR, 2023-12-29,      USD, 1.1068
   EUR, 2024-03-29,      USD, 1.0794
   EUR, 2024-06-28,      USD, 1.0708
   EUR, 2024-09-30,      USD,  1.117
   JPY, 2023-12-29,      USD, 0.0071
   JPY, 2024-03-29,      USD, 0.0066
   JPY, 2024-06-28,      USD, 0.0062
   JPY, 2024-09-30,      USD,  0.007

"""
PRICE = pd.read_csv(StringIO(PRICE_CSV), skipinitialspace=True)


JOURNAL_CSV = """
      date, account, contra, currency,      amount, report_amount, description
2024-07-04,    1020,       ,      JPY, 12345678.00,      76386.36, Convert JPY to EUR
          ,    1010,       ,      EUR,   -70791.78,     -76386.36, Convert JPY to EUR
2024-12-04,        ,   1000,      USD,  9500000.00,              , Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places)
          ,    1010,       ,      EUR,  9978888.88,    9500000.00, Convert 9.5 Mio USD at EUR @1.050409356 (9 decimal places)
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)


engine.reporting_currency = "USD"
engine.restore(
    accounts=ACCOUNTS,
    assets=ASSETS,
    price_history=PRICE,
)
initial = engine.sanitize_journal(JOURNAL)
engine.journal.mirror(initial)
balance = engine.individual_account_balances(accounts=1999, period="2024")

# Rounding precision compensation
assert balance.query("account == 1999")["report_balance"].iloc[0] == 0.0
assert balance.query("account == 1999")["balance"].iloc[0]["USD"] == 0.0

# Currencies mismatch
assert_frame_equal(engine.journal.list(), initial, check_like=True, ignore_row_order=True, ignore_columns=["id"])
