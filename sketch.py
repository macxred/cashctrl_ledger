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

memoryLedger = MemoryLedger()
memoryLedger.restore(configuration={"REPORTING_CURRENCY": "USD"}, accounts=ACCOUNTS, price_history=PRICES, journal=JOURNAL)
cashctrl = ExtendedCashCtrlLedger(transitory_account=1999)
cashctrl.restore(configuration={"REPORTING_CURRENCY": "USD"}, accounts=ACCOUNTS, price_history=PRICES, journal=JOURNAL)

def extract_nodes(data, parent_path="", level=0):
    """
    Recursively extracts node information from a nested structure of financial categories and accounts.

    Args:
        data (list): The list of dictionaries representing the hierarchy.
        parent_path (list): The trail of parent node 'text' attributes.
        level (int): Current depth level for clarity (optional).

    Returns:
        list of dicts: Flattened list of extracted node data.
    """
    result = []
    for item in data:
        path = f"{parent_path}/{item.get('text', '')}"
        if item.get('leaf', False):
            result.append(item | {"path": path})
        elif 'data' in item and isinstance(item['data'], list):
            result.extend(extract_nodes(item['data'], parent_path=path, level=level+1))
        else:
            raise ValueError(f"Unexpected data at {path}.")
    return result

source = memoryLedger
remote = ExtendedCashCtrlLedger(transitory_account=1999)
out = remote.assets.mirror(source.assets.list(), delete=True)



# Define the test date ranges
fiscal_2024_start = date(2024, 1, 12)
fiscal_2025_end = date(2025, 12, 31)
account = 4001

print(source.account_balance(account, "2025-12-31"))
print(remote.account_balance(account, "2025-12-31"))
response = remote._client.json_request("GET", "report/element/data.json", params={"elementId": 2, "startDate": fiscal_2024_start, "endDate": fiscal_2025_end})
pnl = pd.DataFrame(extract_nodes(response["data"]))
print(pnl[["endAmount", "text"]])