"""
This cashctrl_ledger package is a Python package that implements the `pyledger.LedgerEngine` 
interface, enabling seamless integration with the CashCtrl accounting service. With this 
package, users can perform various accounting operations programmatically, directly from Python.

Modules:
- ledger: Contains the CashCtrlLedger class to sync ledger system onto CashCtrl.
"""

from .ledger import CashCtrlLedger