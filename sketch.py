"""
Execute:
    python -i sketch.py
"""

from io import StringIO
import numpy as np
import pandas as pd
from pyledger import TextLedger
from cashctrl_ledger import ExtendedCashCtrlLedger

source = TextLedger()
engine = ExtendedCashCtrlLedger(1999)

ACCOUNT_CSV = """
group,    account, currency, description
/Assets,     1176,      CHF, Accounts Receivable VAT Cleared
/Assets,     1903,      USD, Transitory account - USD
/Assets,     1904,      CAD, Transitory account - CAD
/Assets,     1999,      CHF, Transitory Account for CashCtrl rounding precision
/Revenue,    6953,      USD, Interest Income CAUSDD
/Revenue,    6954,      CAD, Interest Income CAD
"""
ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)

ASSETS_CSV = """
ticker, increment
   CAD,      0.01
   USD,      0.01
   CHF,      0.01
"""
ASSETS = pd.read_csv(StringIO(ASSETS_CSV), skipinitialspace=True)

PRICE_CSV = """
ticker,       date, currency,    price
   CAD, 2024-01-01,      CHF,   0.6523
   CAD, 2024-02-01,      CHF,   0.6458
   CAD, 2024-03-01,      CHF,   0.6545
   CAD, 2024-04-01,      CHF,   0.6594
   CAD, 2024-05-01,      CHF,   0.6714
   CAD, 2024-06-01,      CHF,   0.6725
   CAD, 2024-07-01,      CHF,   0.6617
   CAD, 2024-08-01,      CHF,   0.6617
   CAD, 2024-09-01,      CHF,   0.6369
   CAD, 2024-10-01,      CHF,   0.6326
   CAD, 2024-11-01,      CHF,   0.6335
   CAD, 2024-12-01,      CHF,   0.6346
   USD, 2024-01-01,      CHF,   0.8808
   USD, 2024-02-01,      CHF,   0.8639
   USD, 2024-03-01,      CHF,   0.8817
   USD, 2024-04-01,      CHF,   0.8918
   USD, 2024-05-01,      CHF,   0.9165
   USD, 2024-06-01,      CHF,   0.9197
   USD, 2024-07-01,      CHF,   0.9061
   USD, 2024-08-01,      CHF,   0.9049
   USD, 2024-09-01,      CHF,   0.8754
   USD, 2024-10-01,      CHF,   0.8564
   USD, 2024-11-01,      CHF,    0.865
   USD, 2024-12-01,      CHF,   0.8846
"""
PRICE = pd.read_csv(StringIO(PRICE_CSV), skipinitialspace=True)


JOURNAL_CSV = """
      date, account, contra, currency,     amount, report_amount, description
2024-08-26,        ,   6953,      USD,    2408.10,       2060.85, Bruttozins Festgeldanlage 1252851-3G-6
          ,    1176,       ,      CHF,     721.30,              , Verrechnungssteuer 35% von CHF 2060.85
          ,    1903,       ,      USD,    1565.27,       1339.55, Nettozins Festgeldanlage 1252851-3G-6
2024-09-06,        ,   6954,      CAD,    1618.17,       1015.30, Bruttozins Festgeldanlage 1252851-3G-8
          ,    1176,       ,      CHF,     355.35,              , Verrechnungssteuer 35% von CHF 1015.30
          ,    1904,       ,      CAD,    1051.81,        659.95, Nettozins Festgeldanlage 1252851-3G-8
2024-10-11,        ,   6954,      CAD,    1522.11,        955.50, Bruttozins Festgeldanlage 1252851-3G-8
          ,    1176,       ,      CHF,     334.40,              , Verrechnungssteuer 35% von CHF 955.50
          ,    1904,       ,      CAD,     989.37,        621.10, Nettozins Festgeldanlage 1252851-3G-8
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)


engine.reporting_currency = "CHF"
engine.restore(
    accounts=ACCOUNTS,
    assets=ASSETS,
    price_history=PRICE,
)

engine.journal.mirror(JOURNAL)

