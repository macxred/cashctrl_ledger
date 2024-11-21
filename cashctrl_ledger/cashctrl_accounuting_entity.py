"""Provides a base class for storing tabular accounting entities in CashCtrl."""

from pyledger import AccountingEntity
from cashctrl_api import CashCtrlClient


class CashCtrlAccountingEntity(AccountingEntity):
    """Abstract base class for storing tabular accounting entities in CashCtrl."""

    _client: CashCtrlClient = None

    def __init__(self, client: CashCtrlClient, *args, **kwargs):
        """
        Initialize the CashCtrlAccountingEntity.

        Args:
            client (CashCtrlClient): The client instance used to interact with the CashCtrl API.
            *args, **kwargs: Additional arguments passed to the superclass.
        """
        self._client = client
        super().__init__(*args, **kwargs)
