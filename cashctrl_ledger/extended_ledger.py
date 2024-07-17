"""This module contains "hackish" code that violates the limitations of the CashCtrl."""

from typing import Union
import numpy as np
import pandas as pd
from .ledger import CashCtrlLedger


class CashCtrlLedgerExtended(CashCtrlLedger):
    """Class that inherits the CashCtrlLedger class and violates
    the limitations of the CashCtrl.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
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
        """Transitory account for balancing entries.

        Some complex transactions can not be mapped to CashCtrl. We split such transactions
        into multiple simpler transactions. The balance of each simple transaction is booked
        onto the transitory account, where the combination of all postings originating from
        the same complex transactions should sum up to zero.

        Raises:
            ValueError: If transitory_account is not set or the account does not exist.

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
        if account_currency != self.base_currency:
            raise ValueError(
                f"The transitory account {self._transitory_account} must be "
                f"denominated in {self.base_currency} base currency, not "
                f"{account_currency}."
            )
        return self._transitory_account

    @transitory_account.setter
    def transitory_account(self, value: int):
        self._transitory_account = value

    # ----------------------------------------------------------------------
    # Ledger

    def sanitize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Sanitizes the ledger DataFrame by splitting multi-currency transactions
        and ensuring FX adjustments.

        Args:
            ledger (pd.DataFrame): The ledger DataFrame to be sanitized.

        Returns:
            pd.DataFrame: The sanitized ledger DataFrame.
        """
        # Number of currencies other than base currency
        base_currency = self.base_currency
        n_currency = ledger[["id", "currency"]][ledger["currency"] != base_currency]
        n_currency = n_currency.groupby("id")["currency"].nunique()

        # Split entries with multiple currencies into separate entries for each currency
        ids = n_currency.index[n_currency > 1]
        if len(ids) > 0:
            multi_currency = self.standardize_ledger(ledger[ledger["id"].isin(ids)])
            multi_currency = self.split_multi_currency_transactions(multi_currency)
            others = ledger[~ledger["id"].isin(ids)]
            df = pd.concat([others, multi_currency], ignore_index=True)
        else:
            df = ledger

        # Ensure foreign currencies can be mapped, correct with FX adjustments otherwise
        transitory_account = self.transitory_account
        result = []
        for _, txn in df.groupby("id"):
            new_txn = self._add_fx_adjustment(
                txn, transitory_account=transitory_account, base_currency=base_currency
            )
            result.append(new_txn)
        if len(result) > 0:
            result = pd.concat(result)
        else:
            result = df

        # Invoke parent class method
        return super().sanitize_ledger(result)

    def split_multi_currency_transactions(
        self, ledger: pd.DataFrame, transitory_account: Union[int, None] = None
    ) -> pd.DataFrame:
        """Splits multi-currency transactions into separate transactions for each currency.

        CashCtrl restricts collective transactions to base currency plus a single foreign currency.
        This method splits multi-currency transactions into several separate transactions with
        a single currency and base currency compatible with CashCtrl. A residual balance in any
        currency is booked to the `transitory_account`, the aggregate amount booked to the
        transitory account across all currencies is zero.

        Args:
            ledger (pd.DataFrame): DataFrame with ledger transactions to split.
            transitory_account (int, optional): The number of the account used for balancing
                                                transitory entries.

        Returns:
            pd.DataFrame: A DataFrame with the split transactions
                          and any necessary balancing entries.
        """
        base_currency = self.base_currency
        is_base_currency = ledger["currency"] == base_currency
        ledger.loc[is_base_currency, "base_currency_amount"] = ledger.loc[
            is_base_currency, "amount"
        ]

        if any(ledger["base_currency_amount"].isna()):
            raise ValueError("Base currency amount missing for some items.")
        if transitory_account is None:
            transitory_account = self.transitory_account

        result = []
        for (id, currency), group in ledger.groupby(["id", "currency"]):
            sub_id = f"{id}:{currency}"
            result.append(group.assign(id=sub_id))
            balance = round(group["base_currency_amount"].sum(), 2)
            if balance != 0:
                clearing_txn = pd.DataFrame(
                    {
                        "id": [sub_id],
                        "text": [
                            "Split multi-currency transaction "
                            "into multiple transactions compatible with CashCtrl."
                        ],
                        "amount": [-1 * balance],
                        "base_currency_amount": [-1 * balance],
                        "currency": [base_currency],
                        "account": [transitory_account],
                    }
                )
                result.append(clearing_txn)

        result = pd.concat(result, ignore_index=True)
        return self.standardize_ledger(result)

    def _add_fx_adjustment(
        self, entry: pd.DataFrame, transitory_account: int, base_currency: str
    ) -> pd.DataFrame:
        """Ensure amounts conform to CashCtrl's eight-digit FX rate precision.

        Adjusts the base currency amounts of a ledger entry to match CashCtrl's
        eight-digit precision for exchange rates. Adds balancing ledger entries
        if adjusted amounts differ from the original, ensuring the sum of all
        entries remains consistent with the original entry.

        Args:
            entry (pd.DataFrame): Ledger entry data.
            transitory_account (int): Account for balancing transactions.
            base_currency (str): Base currency for adjustments.

        Returns:
            pd.DataFrame: Adjusted ledger entries with FX adjustments.
        """
        if len(entry) == 1:
            # Individual transaction: one row in the ledger data frame
            if (
                entry["amount"].item() == 0
                or entry["currency"].item() == base_currency
            ):
                return entry
            else:
                amount = round(entry["amount"].item(), 2)
                base_amount = round(entry["base_currency_amount"].item(), 2)
                fx_rate = round(base_amount / amount, 8)
                balance = base_amount - round(amount * fx_rate, 2)
                if balance == 0.0:
                    return entry
                else:
                    balancing_txn = entry.copy()
                    balancing_txn["id"] = balancing_txn["id"] + ":fx"
                    balancing_txn["currency"] = base_currency
                    balancing_txn["amount"] = balance
                    balancing_txn["base_currency_amount"] = pd.NA
                    entry["base_currency_amount"] = (
                        entry["base_currency_amount"] - balance
                    )
                    result = pd.concat(
                        [
                            self.standardize_ledger_columns(entry),
                            self.standardize_ledger_columns(balancing_txn),
                        ]
                    )
                    result["amount"] = result["amount"].round(2)
                    result["base_currency_amount"] = result[
                        "base_currency_amount"
                    ].round(2)
                    return self.standardize_ledger(result)

        elif len(entry) > 1:
            # Collective transaction: multiple row in the ledger data frame
            currency, fx_rate = self._collective_transaction_currency_and_rate(
                entry, suppress_error=True
            )
            fx_rate = round(fx_rate, 8)
            if currency == base_currency:
                return entry
            else:
                amount = entry["amount"].round(2)
                base_amount = entry["base_currency_amount"].round(2)
                balance = np.where(
                    entry["currency"] == base_currency,
                    amount - ((amount / fx_rate).round(2) * fx_rate).round(2),
                    base_amount - (amount * fx_rate).round(2),
                )
                if all(balance == 0.0):
                    return entry
                else:
                    is_base_currency = entry["currency"] == base_currency
                    balancing_txn = entry.head(1).copy()
                    balancing_txn["currency"] = base_currency
                    balancing_txn["amount"] = balance.sum()
                    balancing_txn["account"] = transitory_account
                    balancing_txn["base_currency_amount"] = pd.NA
                    balancing_txn[
                        "text"
                    ] = "Currency adjustments to match CashCtrl FX rate precision"
                    entry["amount"] = entry["amount"] - np.where(
                        is_base_currency, balance, 0
                    )
                    entry["base_currency_amount"] = (
                        entry["base_currency_amount"] - balance
                    )
                    entry = pd.concat(
                        [
                            self.standardize_ledger_columns(entry),
                            self.standardize_ledger_columns(balancing_txn),
                        ]
                    )
                    balance = np.append(balance, -1 * balance.sum())
                    fx_adjust = entry.copy()
                    is_base_currency = fx_adjust["currency"] == base_currency
                    fx_adjust["amount"] = np.where(is_base_currency, balance, 0.0)
                    fx_adjust["base_currency_amount"] = np.where(
                        is_base_currency, pd.NA, balance
                    )
                    fx_adjust["id"] = fx_adjust["id"] + ":fx"
                    fx_adjust["text"] = "Currency adjustments: " + fx_adjust["text"]
                    fx_adjust = fx_adjust[balance != 0]
                    result = pd.concat([entry, fx_adjust])
                    result["amount"] = result["amount"].astype(pd.Float64Dtype()).round(2)
                    result["base_currency_amount"] = result["base_currency_amount"].astype(
                        pd.Float64Dtype()
                    ).round(2)
                    return self.standardize_ledger(result)

        else:
            raise ValueError("Expecting at least one `entry` row.")
