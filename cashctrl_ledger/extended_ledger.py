"""This module extends CashCtrlLedger to handle transactions that can not be
directly represented in CahCtrl.
"""

from .ledger import CashCtrlLedger


class ExtendedCashCtrlLedger(CashCtrlLedger):
    """
    Extends `CashCtrlLedger` to handle transactions that cannot be directly
    represented due to CashCtrl's limitations.

    CashCtrl's data model imposes constraints, such as restricting FX rates
    to eight-digit precision and limiting collective ledger entries to a single
    currency beyond the reporting currency. This class ensures that transactions
    conform to CashCtrl's standards by splitting unrepresentable transactions
    into multiple simpler ones that can be accommodated, while preserving the
    overall financial result.

    To use this class, a special `transitory_account` must be defined in the
    chart of accounts. Residual amounts arising from split transactions are
    recorded in this account. The account is balanced for any group of split
    transactions that together represent a single original transaction.
    """

    _transitory_account = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, transitory_account: int):
        super().__init__()
        self.transitory_account = transitory_account

    # ----------------------------------------------------------------------
    # Accounts

    @property
    def transitory_account(self) -> int:
        """Returns the transitory account used to book residual amounts when complex
        transactions are broken into simpler ones for compatibility with CashCtrl.

        Raises:
            ValueError: If the transitory account is not set, does not exist, or is
            denominated in a different currency than the reporting currency.

        Returns:
            int: The transitory account number.
        """
        if self._transitory_account is None:
            raise ValueError("transitory_account is not set.")
        if self._transitory_account not in set(
            self._client.list_accounts()["number"]
        ):
            raise ValueError(
                f"The transitory account {self._transitory_account} does not exist."
            )
        account_currency = self._client.account_to_currency(self._transitory_account)
        if account_currency != self.reporting_currency:
            raise ValueError(
                f"The transitory account {self._transitory_account} must be "
                f"denominated in {self.reporting_currency} reporting currency, not "
                f"{account_currency}."
            )
        return self._transitory_account

    @transitory_account.setter
    def transitory_account(self, value: int):
        self._transitory_account = value
