from cashctrl_ledger import CashCtrlLedger
self = cashctrl_ledger = CashCtrlLedger()

x = self.ledger()

self.add_ledger_entry(
    date="2024-07-25",
    account=1480,
    counter_account=2200,
    amount=7,
    currency="USD",
    text="Test code added",
    vat_code="MwSt. 2.6%",
    document=4,
)