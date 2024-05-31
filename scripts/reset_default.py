#!/usr/bin/env python

import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

def main():
    cashctrl_ledger = CashCtrlLedger()

    # Delete journals
    journals = cashctrl_ledger._client.list_journal_entries()
    ids = ','.join(journals['id'].astype(str).tolist())
    if len(ids):
        cashctrl_ledger._client.post("journal/delete.json", {'ids': ids})

    # Restore default VAT with delete=False
    initial_vat = pd.read_csv('scripts/initial_vat.csv')
    cashctrl_ledger.mirror_vat_codes(target_state=initial_vat, delete=False)

    # Restore default accounts
    initial_accounts = pd.read_csv('scripts/initial_accounts.csv', skipinitialspace=True)
    cashctrl_ledger.mirror_account_chart(target=initial_accounts)

    # Restore default VAT with delete=True
    cashctrl_ledger.mirror_vat_codes(target_state=initial_vat)

if __name__ == "__main__":
    main()