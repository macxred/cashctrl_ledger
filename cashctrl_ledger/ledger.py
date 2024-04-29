"""
Module to sync ledger system onto CashCtrl.
"""

from pyledger import LedgerEngine
from cashctrl_api import CashCtrlClient

class CashCtrlLedger(LedgerEngine):
    """
    Class that give you an ability to sync ledger system onto CashCtrl

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    def __init__(self, client: CashCtrlClient | None = None):
        super().__init__()
        self._client = CashCtrlClient() if client is None else client

    def _single_account_balance():
        """
        Pass realization
        """
        pass

    def account_chart():
        """
        Pass realization
        """
        pass

    def add_account():
        """
        Pass realization
        """
        pass

    def add_ledger_entry():
        """
        Pass realization
        """
        pass

    def add_price():
        """
        Pass realization
        """
        pass  

    def add_vat_code():
        """
        Pass realization
        """
        pass  

    def base_currency():
        """
        Pass realization
        """
        pass

    def delete_account():
        """
        Pass realization
        """
        pass  

    def delete_ledger_entry():
        """
        Pass realization
        """
        pass  

    def delete_price():
        """
        Pass realization
        """
        pass

    def delete_vat_code():
        """
        Pass realization
        """
        pass  

    def ledger():
        """
        Pass realization
        """
        pass

    def ledger_entry():
        """
        Pass realization
        """
        pass

    def modify_account():
        """
        Pass realization
        """
        pass  

    def modify_ledger_entry():
        """
        Pass realization
        """
        pass  

    def precision():
        """
        Pass realization
        """
        pass  

    def price():
        """
        Pass realization
        """
        pass

    def price_history():
        """
        Pass realization
        """
        pass  

    def vat_codes():
        """
        Pass realization
        """
        pass  