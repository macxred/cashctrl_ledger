from cashctrl_ledger import CashCtrlLedger

cc_ledger = CashCtrlLedger()
print(cc_ledger._client.get("person/list.json"))