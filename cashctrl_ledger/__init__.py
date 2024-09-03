# flake8: noqa: F401

"""This cashctrl_ledger package is a Python package that implements the `pyledger.LedgerEngine`
interface, enabling seamless integration with the CashCtrl accounting service. With this
package, users can perform various accounting operations programmatically, directly from Python.

Modules:
- ledger: Contains the CashCtrlLedger Class that Implements the pyledger interface by connecting
          to the CashCtrl online accounting software.
- extended_ledger: Contains the ExtendedCashCtrlLedger class that helps to splits transactions
				   that can not be represented in CashCtrl into multiple representable transactions.
"""

from .ledger import CashCtrlLedger
from .extended_ledger import ExtendedCashCtrlLedger
from .constants import *
