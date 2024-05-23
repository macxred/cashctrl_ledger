import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

def test_ledger_mutators():
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index().drop(columns=['id'])

    # Test adding a ledger entry
    new = pd.DataFrame({
        'account': [2270],
        'counter_account': [2210],
        'amount': [50],
        'currency': ['USD'],
        'text': ['pytest added ledger'],
        'vat_code': ['MwSt. 2.6%'],
        'document': [''],
    })

    cashctrl_ledger.add_ledger_entry(date='2024-01-01', target=new)
    updated_ledger = cashctrl_ledger.ledger().reset_index().drop(columns=['id'])
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created_ledger = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1)

    updated_ledger.iloc[1]['date'] == initial_ledger.iloc[0]['date']

    breakpoint()
