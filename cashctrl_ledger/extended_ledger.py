"""This module extends CashCtrlLedger to handle transactions that can not be
directly represented in CahCtrl.
"""

import numpy as np
import pandas as pd
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
    accounts. Residual amounts arising from split transactions are
    recorded in this account. The account is balanced for any group of split
    transactions that together represent a single original transaction.
    """

    _transitory_account = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, transitory_account: int):
        super().__init__()
        self.transitory_account = transitory_account

    def clear(self):
        transitory_account = self._client.account_to_id(
            self._transitory_account, allow_missing=True
        )
        if transitory_account is None:
            payload = {
                "account": self._transitory_account,
                "currency": self.base_currency,
                "text": "temp transitory account",
                "tax_code": None,
                "group": "/Assets",
            }
            self.add_account(**payload)
        super().clear()

    # ----------------------------------------------------------------------
    # Accounts

    @property
    def transitory_account(self) -> int:
        """Returns the transitory account used to book residual amounts when complex
        transactions are broken into simpler ones for compatibility with CashCtrl.

        Raises:
            ValueError: If the transitory account is not set, does not exist, or is
            denominated in a different currency than the base currency.

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
        if value not in set(self._client.list_accounts()["number"]):
            self.add_account(
                account=value,
                tax_code=None,
                group="/Assets",
                text="Transitory account",
                currency=self.base_currency
            )
        self._transitory_account = value

    # ----------------------------------------------------------------------
    # Ledger

    def sanitize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Modify ledger entries to ensure coherence and compatibility with CashCtrl.

        Extends the base `sanitize_ledger` to address CashCtrl-specific constraints:
        - Splits collective ledger entries with multiple currencies (other than
          the reporting currency) into separate entries for each currency that can
          be represented in CashCtrl.
        - Ensures transactions conform to CashCtrl's eight-digit precision limit.
        - Creates additional compensating transactions to balance residual amounts
          using the designated `transitory_account`.

        Args:
            ledger (pd.DataFrame): The ledger DataFrame with transactions to be processed.

        Returns:
            pd.DataFrame: The modified ledger DataFrame.
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

    def split_multi_currency_transactions(self, ledger: pd.DataFrame,
                                          transitory_account: int | None = None) -> pd.DataFrame:
        """
        Splits multi-currency transactions into individual transactions for each currency.

        CashCtrl restricts collective transactions to a single currency beyond the reporting
        currency. This method splits multi-currency transactions into several transactions
        compatible with CashCtrl, each involving a single currency and possibly
        the reporting currency. If there is a residual balance in any currency, it is
        recorded in the `transitory_account`. The total of all entries in the transitory
        account across these transactions will balance to zero.

        Args:
            ledger (pd.DataFrame): The ledger DataFrame containing transactions to be split.
            transitory_account (int, optional): The account number for recording balancing
                entries. If not provided, the instance's `transitory_account` will be used.

        Returns:
            pd.DataFrame: Modified ledger entries with split transactions and any necessary
                balancing entries.
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
        """
        Adjusts ledger entries to conform with CashCtrl's eight-digit FX rate precision.

        This method ensures that the base currency amounts in a ledger entry match
        CashCtrl's precision limit for foreign exchange rates of eight digits after
        the decimal point. If an adjustment is needed due to rounding differences,
        it adds a balancing ledger entry using the `transitory_account`, so the
        financial result remains consistent.

        Args:
            entry (pd.DataFrame): The ledger entry or entries to be adjusted.
            transitory_account (int): The account number for recording balancing transactions.
            base_currency (str): The base currency against which adjustments are made.

        Returns:
            pd.DataFrame: The adjusted ledger entries and any necessary balancing entries.
        """
        if len(entry) == 1:
            # Individual transaction: one row in the ledger data frame
            if (
                entry["amount"].item() == 0
                or entry["currency"].item() == base_currency
            ):
                return entry
            else:
                amount = self.round_to_precision(entry["amount"].item(), entry["currency"].item())
                base_amount = self.round_to_precision(
                    entry["base_currency_amount"].item(), base_currency
                )
                fx_rate = round(base_amount / amount, 8)
                balance = base_amount - self.round_to_precision(amount * fx_rate, base_currency)
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
                    result["amount"] = self.round_to_precision(
                        result["amount"], result["currency"]
                    )
                    result["base_currency_amount"] = self.round_to_precision(
                        result["base_currency_amount"], base_currency
                    )
                    return self.standardize_ledger(result)

        elif len(entry) > 1:
            # Collective transaction: multiple rows in the ledger data frame
            currency, fx_rate = self._collective_transaction_currency_and_rate(
                entry, suppress_error=True
            )
            fx_rate = round(fx_rate, 8)
            if currency == base_currency:
                return entry
            else:
                entry["amount"] = self.round_to_precision(entry["amount"], entry["currency"])
                entry["base_currency_amount"] = self.round_to_precision(
                    entry["base_currency_amount"], base_currency,
                )
                balance = np.where(
                    entry["currency"] == base_currency,
                    entry["amount"] - np.array(
                        self.round_to_precision(entry["amount"] / fx_rate, currency)
                    ) * fx_rate,
                    entry["base_currency_amount"] - np.array(
                        self.round_to_precision(entry["amount"] * fx_rate, base_currency)
                    ),
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
                    result["amount"] = self.round_to_precision(
                        result["amount"], result["currency"]
                    )
                    result["base_currency_amount"] = self.round_to_precision(
                        result["base_currency_amount"], base_currency
                    )
                    return self.standardize_ledger(result)

        else:
            raise ValueError("Expecting at least one `entry` row.")
