"""Module to sync ledger system onto CashCtrl."""

from datetime import datetime
from typing import Dict, List, Tuple, Union
from cashctrl_api import CachedCashCtrlClient, enforce_dtypes
import numpy as np
import pandas as pd
from pyledger import LedgerEngine, StandaloneLedger
from .constants import JOURNAL_ITEM_COLUMNS
from .ledger_utils import df_to_consistent_str
from .nesting import nest, unnest


class CashCtrlLedger(LedgerEngine):
    """Class that give you an ability to sync ledger system onto CashCtrl.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    _transitory_account = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, client: Union[CachedCashCtrlClient, None] = None):
        super().__init__()
        self._client = CachedCashCtrlClient() if client is None else client

    # ----------------------------------------------------------------------
    # VAT codes

    def vat_codes(self) -> pd.DataFrame:
        """Retrieves VAT codes from the remote CashCtrl account and converts to standard
        pyledger format.

        Returns:
            pd.DataFrame: A DataFrame with pyledger.VAT_CODE column schema.
        """
        tax_rates = self._client.list_tax_rates()
        accounts = self._client.list_accounts()
        account_map = accounts.set_index("id")["number"].to_dict()
        if not tax_rates["accountId"].isin(account_map).all():
            raise ValueError("Unknown 'accountId' in CashCtrl tax rates.")
        result = pd.DataFrame(
            {
                "id": tax_rates["name"],
                "text": tax_rates["documentName"],
                "account": tax_rates["accountId"].map(account_map),
                "rate": tax_rates["percentage"] / 100,
                "inclusive": ~tax_rates["isGrossCalcType"],
            }
        )

        duplicates = set(result.loc[result["id"].duplicated(), "id"])
        if duplicates:
            raise ValueError(
                f"Duplicated VAT codes in the remote system: '{', '.join(map(str, duplicates))}'"
            )
        return StandaloneLedger.standardize_vat_codes(result)

    def add_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ):
        """Adds a new VAT code to the CashCtrl account.

        Args:
            code (str): The VAT code to be added.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool, optional): Determines whether the VAT is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            text (str, optional): Additional text or description associated with the VAT code.
                                  Defaults to "".
        """
        payload = {
            "name": code,
            "percentage": rate * 100,
            "accountId": self._client.account_to_id(account),
            "calcType": "NET" if inclusive else "GROSS",
            "documentName": text,
        }
        self._client.post("tax/create.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def update_vat_code(
        self,
        code: str,
        rate: float,
        account: str,
        inclusive: bool = True,
        text: str = "",
    ):
        """Updates an existing VAT code in the CashCtrl account with new parameters.

        Args:
            code (str): The VAT code to be updated.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool, optional): Determines whether the VAT is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            text (str, optional): Additional text or description associated with the VAT code.
                                  Defaults to "".
        """
        payload = {
            "id": self._client.tax_code_to_id(code),
            "percentage": rate * 100,
            "accountId": self._client.account_to_id(account),
            "calcType": "NET" if inclusive else "GROSS",
            "name": code,
            "documentName": text,
        }
        self._client.post("tax/update.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def delete_vat_code(self, code: str, allow_missing: bool = False):
        """Deletes a VAT code from the remote CashCtrl account.

        Args:
            code (str): The VAT code name to be deleted.
            allow_missing (bool, optional): If True, no error is raised if the VAT code is not
                                            found; if False, raises ValueError. Defaults to False.
        """
        delete_id = self._client.tax_code_to_id(code, allow_missing=allow_missing)
        if delete_id:
            self._client.post("tax/delete.json", {"ids": delete_id})
            self._client.invalidate_tax_rates_cache()

    def mirror_vat_codes(self, target: pd.DataFrame, delete: bool = False):
        """Aligns VAT rates on the remote CashCtrl account with
        the desired state provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame containing VAT rates in
                                         the pyledger.vat_codes format.
            delete (bool, optional): If True, deletes VAT codes on the remote account
                                     that are not present in target_state.
        """
        target_df = StandaloneLedger.standardize_vat_codes(target).reset_index()
        current_state = self.vat_codes().reset_index()

        # Delete superfluous VAT codes on remote
        if delete:
            for idx in set(current_state["id"]).difference(set(target_df["id"])):
                self.delete_vat_code(code=idx)

        # Create new VAT codes on remote
        ids = set(target_df["id"]).difference(set(current_state["id"]))
        to_add = target_df.loc[target_df["id"].isin(ids)]
        for row in to_add.to_dict("records"):
            self.add_vat_code(
                code=row["id"],
                text=row["text"],
                account=row["account"],
                rate=row["rate"],
                inclusive=row["inclusive"],
            )

        # Update modified VAT cods on remote
        both = set(target_df["id"]).intersection(set(current_state["id"]))
        left = target_df.loc[target_df["id"].isin(both)]
        right = current_state.loc[current_state["id"].isin(both)]
        merged = pd.merge(left, right, how="outer", indicator=True)
        to_update = merged[merged["_merge"] == "left_only"]
        for row in to_update.to_dict("records"):
            self.update_vat_code(
                code=row["id"],
                text=row["text"],
                account=row["account"],
                rate=row["rate"],
                inclusive=row["inclusive"],
            )

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

    def account_chart(self) -> pd.DataFrame:
        """Retrieves the account chart from a remote CashCtrl instance,
        formatted to the pyledger schema.

        Returns:
            pd.DataFrame: A DataFrame with the account chart in pyledger format.
        """
        accounts = self._client.list_accounts()
        result = pd.DataFrame(
            {
                "account": accounts["number"],
                "currency": accounts["currencyCode"],
                "text": accounts["name"],
                "vat_code": accounts["taxName"],
                "group": accounts["path"],
            }
        )
        return StandaloneLedger.standardize_account_chart(result)

    def add_account(
        self,
        account: str,
        currency: str,
        text: str,
        group: str,
        vat_code: Union[str, None] = None,
    ):
        """Adds a new account to the remote CashCtrl instance.

        Args:
            account (str): The account number or identifier to be added.
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """
        payload = {
            "number": account,
            "currencyId": self._client.currency_to_id(currency),
            "name": text,
            "taxId": None
            if pd.isna(vat_code)
            else self._client.tax_code_to_id(vat_code),
            "categoryId": self._client.account_category_to_id(group),
        }
        self._client.post("account/create.json", data=payload)
        self._client.invalidate_accounts_cache()

    def modify_account(
        self,
        account: str,
        currency: str,
        text: str,
        group: str,
        vat_code: Union[str, None] = None,
    ):
        """Updates an existing account in the remote CashCtrl instance.

        Args:
            account (str): The account number or identifier to be added.
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """
        payload = {
            "id": self._client.account_to_id(account),
            "number": account,
            "currencyId": self._client.currency_to_id(currency),
            "name": text,
            "taxId": None
            if pd.isna(vat_code)
            else self._client.tax_code_to_id(vat_code),
            "categoryId": self._client.account_category_to_id(group),
        }
        self._client.post("account/update.json", data=payload)
        self._client.invalidate_accounts_cache()

    def delete_account(self, account: str, allow_missing: bool = False):
        """Deletes an account from the remote CashCtrl instance.

        Args:
            account (str): The account number to be deleted.
            allow_missing (bool, optional): If True, do not raise an error if the
                                            account is missing. Defaults to False.
        """
        delete_id = self._client.account_to_id(account, allow_missing=allow_missing)
        if delete_id:
            self._client.post("account/delete.json", {"ids": delete_id})
            self._client.invalidate_accounts_cache()

    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = False):
        """Synchronizes remote CashCtrl accounts with a desired target state
        provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes accounts on the remote that are not
                                     present in the target DataFrame.
        """
        target_df = StandaloneLedger.standardize_account_chart(target).reset_index()
        current_state = self.account_chart().reset_index()

        # Delete superfluous accounts on remote
        if delete:
            for account in set(current_state["account"]).difference(
                set(target_df["account"])
            ):
                self.delete_account(account=account)

        # Create new accounts on remote
        accounts = set(target_df["account"]).difference(
            set(current_state["account"])
        )
        to_add = target_df.loc[target_df["account"].isin(accounts)]

        # Update account categories
        def get_nodes_list(path: str) -> List[str]:
            parts = path.strip("/").split("/")
            return ["/" + "/".join(parts[:i]) for i in range(1, len(parts) + 1)]

        def account_groups(df: pd.DataFrame) -> Dict[str, str]:
            df["nodes"] = [
                pd.DataFrame({"items": get_nodes_list(path)}) for path in df["group"]
            ]
            df = unnest(df, key="nodes")
            return df.groupby("items")["account"].agg("min").to_dict()

        self._client.update_categories(
            resource="account",
            target=account_groups(target),
            delete=delete,
            ignore_account_root_nodes=True,
        )

        for row in to_add.to_dict("records"):
            self.add_account(
                account=row["account"],
                currency=row["currency"],
                text=row["text"],
                vat_code=row["vat_code"],
                group=row["group"],
            )

        # Update modified accounts on remote
        both = set(target_df["account"]).intersection(set(current_state["account"]))
        left = target_df.loc[target_df["account"].isin(both)]
        right = current_state.loc[current_state["account"].isin(both)]
        merged = pd.merge(left, right, how="outer", indicator=True)
        to_update = merged[merged["_merge"] == "left_only"]

        for row in to_update.to_dict("records"):
            self.modify_account(
                account=row["account"],
                currency=row["currency"],
                text=row["text"],
                vat_code=row["vat_code"],
                group=row["group"],
            )

    def _single_account_balance(
        self, account: int, date: Union[datetime.date, None] = None
    ) -> dict:
        """Calculate the balance of a single account in both account currency and base currency.

        Args:
            account (int): The account number.
            date (datetime.date, optional): The date for the balance. Defaults to None,
                in which case the balance on the last day of the current fiscal period is returned.

        Returns:
            dict: A dictionary with the balance in the account currency and the base currency.
        """
        account_id = self._client.account_to_id(account)
        params = {"id": account_id, "date": date}
        response = self._client.request("GET", "account/balance", params=params)
        balance = float(response.text)

        account_currency = self._client.account_to_currency(account)
        if self.base_currency == account_currency:
            base_currency_balance = balance
        else:
            response = self._client.get(
                "fiscalperiod/exchangediff.json", params={"date": date}
            )
            exchange_diff = pd.DataFrame(response["data"])
            base_currency_balance = exchange_diff.loc[
                exchange_diff["accountId"] == account_id, "dcBalance"
            ].item()

        return {account_currency: balance, "base_currency": base_currency_balance}

    # ----------------------------------------------------------------------
    # Ledger

    def ledger(self) -> pd.DataFrame:
        """Retrieves ledger entries from the remote CashCtrl account and converts
        the entries to standard pyledger format.

        Returns:
            pd.DataFrame: A DataFrame with LedgerEngine.ledger() column schema.
        """
        ledger = self._client.list_journal_entries()

        # Individual ledger entries represent a single transaction and
        # map to a single row in the resulting data frame.
        individual = ledger[ledger["type"] != "COLLECTIVE"]

        # Map to credit and debit account number and account currency
        cols = {"id": "creditId", "currencyCode": "credit_currency", "number": "credit_account"}
        account_map = self._client.list_accounts()[cols.keys()].rename(columns=cols)
        individual = pd.merge(individual, account_map, "left", on="creditId", validate="m:1")
        cols = {"id": "debitId", "currencyCode": "debit_currency", "number": "debit_account"}
        account_map = self._client.list_accounts()[cols.keys()].rename(columns=cols)
        individual = pd.merge(individual, account_map, "left", on="debitId", validate="m:1")

        # Identify foreign currency adjustment transactions
        currency = individual["currencyCode"]
        base_currency = self.base_currency
        is_fx_adjustment = (
            (currency == base_currency)
            & (
                (currency != individual["credit_currency"])
                | (currency != individual["debit_currency"])
            )
        )

        result = pd.DataFrame(
            {
                "id": individual["id"],
                "date": individual["dateAdded"].dt.date,
                "account": individual["debit_account"],
                "counter_account": individual["credit_account"],
                "amount": individual["amount"],
                "currency": individual["currencyCode"],
                "text": individual["title"],
                "vat_code": individual["taxName"],
                # TODO: Once precision() is implemented, use `round_to_precision()`
                # instead of hard-coded rounding
                "base_currency_amount": np.where(
                    is_fx_adjustment,
                    pd.NA,
                    round(individual["amount"] * individual["currencyRate"], 2),
                ),
                "document": individual["reference"],
            }
        )

        # Collective ledger entries represent a group of transactions and
        # map to multiple rows in the resulting data frame with same id.
        collective_ids = ledger.loc[ledger["type"] == "COLLECTIVE", "id"]
        if len(collective_ids) > 0:

            # Fetch individual legs (line 'items') of collective transaction
            def fetch_journal(id: int) -> pd.DataFrame:
                res = self._client.get("journal/read.json", params={"id": id})["data"]
                return pd.DataFrame(
                    {
                        "id": [res["id"]],
                        "document": res["reference"],
                        "date": [pd.to_datetime(res["dateAdded"]).date()],
                        "currency": [res["currencyCode"]],
                        "rate": [res["currencyRate"]],
                        "items": [enforce_dtypes(pd.DataFrame(res["items"]), JOURNAL_ITEM_COLUMNS)],
                        "fx_rate": [res["currencyRate"]],
                    }
                )
            dfs = pd.concat([fetch_journal(id) for id in collective_ids])
            collective = unnest(dfs, "items")

            # Map to account number and account currency
            cols = {"id": "accountId", "currencyCode": "account_currency", "number": "account"}
            account_map = self._client.list_accounts()[cols.keys()].rename(columns=cols)
            collective = pd.merge(collective, account_map, "left", on="accountId", validate="m:1")

            # Identify base currency or foreign currency adjustment transactions
            base_currency = self.base_currency
            is_fx_adjustment = (collective["account_currency"] != base_currency) & (
                collective["currency"].isna() | (collective["currency"] == base_currency)
            )

            amount = collective["debit"].fillna(0) - collective["credit"].fillna(0)
            currency = collective["account_currency"]
            base_amount = np.where(
                currency == base_currency,
                pd.NA,
                np.where(is_fx_adjustment, amount, amount * collective["fx_rate"]),
            )
            foreign_amount = np.where(
                currency == base_currency,
                amount * collective["fx_rate"],
                np.where(is_fx_adjustment, 0, amount),
            )
            mapped_collective = pd.DataFrame({
                "id": collective["id"],
                "date": collective["date"],
                "currency": currency,
                "account": collective["account"],
                "text": collective["description"],
                # TODO: Once precision() is implemented, use `round_to_precision()`
                # instead of hard-coded rounding
                "amount": pd.Series(foreign_amount).astype(pd.Float64Dtype()).round(2),
                "base_currency_amount": pd.Series(base_amount).astype(pd.Float64Dtype()).round(2),
                "vat_code": collective["taxName"],
                "document": collective["document"]
            })
            result = pd.concat([
                self.standardize_ledger_columns(result),
                self.standardize_ledger_columns(mapped_collective),
            ])

        return self.standardize_ledger(result)

    def ledger_entry(self):
        """Not implemented yet."""
        raise NotImplementedError

    def add_ledger_entry(self, entry: pd.DataFrame) -> int:
        """Adds a new ledger entry to the remote CashCtrl instance.

        Args:
            entry (pd.DataFrame): DataFrame with ledger entry in pyledger schema.

        Returns:
            int: The Id of created ledger entry.
        """
        payload = self._map_ledger_entry(entry)
        res = self._client.post("journal/create.json", data=payload)
        self._client.invalidate_journal_cache()
        return res["insertId"]

    def modify_ledger_entry(self, entry: pd.DataFrame):
        """Adds a new ledger entry to the remote CashCtrl instance.

        Args:
            entry (pd.DataFrame): DataFrame with ledger entry in pyledger schema.
        """
        payload = self._map_ledger_entry(entry)
        if entry["id"].nunique() != 1:
            raise ValueError("Id needs to be unique in all rows of a collective booking.")
        payload["id"] = entry["id"].iat[0]
        self._client.post("journal/update.json", data=payload)
        self._client.invalidate_journal_cache()

    def delete_ledger_entry(self, ids: Union[str, List[str]]):
        """Deletes a ledger entry from the remote CashCtrl instance.

        Args:
            ids (Union[str, List[str]]): The Id(s) of the ledger entry(ies) to be deleted.
        """
        if isinstance(ids, list):
            ids = ",".join(ids)
        self._client.post("journal/delete.json", {"ids": ids})
        self._client.invalidate_journal_cache()

    def mirror_ledger(self, target: pd.DataFrame, delete: bool = False):
        """Mirrors the ledger data to the remote CashCtrl instance.

        Args:
            target (pd.DataFrame): DataFrame containing ledger data in pyledger format.
            delete (bool, optional): If True, deletes ledger entries on the remote that are
                                     not present in the target DataFrame.
        """
        # Standardize data frame schema, discard incoherent entries with a warning
        target = self.standardize_ledger(target)
        target = self.sanitize_ledger(target)

        # Nest to create one row per transaction, add unique string identifier
        def process_ledger(df: pd.DataFrame) -> pd.DataFrame:
            df = nest(
                df,
                columns=[col for col in df.columns if col not in ["id", "date"]],
                key="txn",
            )
            df["txn_str"] = [
                f"{str(date)},{df_to_consistent_str(txn)}"
                for date, txn in zip(df["date"], df["txn"])
            ]
            return df

        remote = process_ledger(self.ledger())
        target = process_ledger(target)
        if target["id"].duplicated().any():
            # We expect nesting to combine all rows with the same
            raise ValueError("Non-unique dates in `target` transactions.")

        # Count occurrences of each unique transaction in target and remote,
        # find number of additions and deletions for each unique transaction
        count = pd.DataFrame({
            "remote": remote["txn_str"].value_counts(),
            "target": target["txn_str"].value_counts(),
        })
        count = count.fillna(0).reset_index(names="txn_str")
        count["n_add"] = (count["target"] - count["remote"]).clip(lower=0).astype(int)
        count["n_delete"] = (count["remote"] - count["target"]).clip(lower=0).astype(int)

        # Delete unneeded transactions on remote
        if delete and any(count["n_delete"] > 0):
            ids = [
                id
                for txn_str, n in zip(count["txn_str"], count["n_delete"])
                if n > 0
                for id in remote.loc[remote["txn_str"] == txn_str, "id"]
                .tail(n=n)
                .values
            ]
            self.delete_ledger_entry(ids=",".join(ids))

        # Add missing transactions to remote
        for txn_str, n in zip(count["txn_str"], count["n_add"]):
            if n > 0:
                txn = unnest(
                    target.loc[target["txn_str"] == txn_str, :].head(1), "txn"
                )
                if txn["id"].dropna().nunique() > 0:
                    id = txn["id"].dropna().unique()[0]
                else:
                    id = txn["text"].iat[0]
                for _ in range(n):
                    try:
                        self.add_ledger_entry(txn)
                    except Exception as e:
                        raise Exception(
                            f"Error while adding ledger entry {id}: {e}"
                        ) from e

        # return number of elements found, targeted, changed:
        stats = {
            "pre-existing": int(count["remote"].sum()),
            "targeted": int(count["target"].sum()),
            "added": count["n_add"].sum(),
            "deleted": count["n_delete"].sum() if delete else 0,
        }
        return stats

    def standardize_ledger(self, ledger: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the ledger DataFrame to conform to CashCtrl format.

        Args:
            ledger (pd.DataFrame): The ledger DataFrame to be standardized.

        Returns:
            pd.DataFrame: The standardized ledger DataFrame.
        """
        df = super().standardize_ledger(ledger)
        # In CashCtrl, attachments are stored at the transaction level rather than
        # for each individual line item within collective transactions. To ensure
        # consistency between equivalent transactions, we fill any missing (NA)
        # document paths with non-missing paths from other line items in the same
        # transaction.
        df["document"] = df.groupby("id")["document"].ffill()
        df["document"] = df.groupby("id")["document"].bfill()

        # Split collective transaction line items with both debit and credit into
        # two items with a single account each
        is_collective = df["id"].duplicated(keep=False)
        items_to_split = (
            is_collective & df["account"].notna() & df["counter_account"].notna()
        )
        if items_to_split.any():
            new = df.loc[items_to_split].copy()
            new["account"] = new["counter_account"]
            new.loc[:, "counter_account"] = pd.NA
            for col in ["amount", "base_currency_amount"]:
                new[col] = np.where(
                    new[col].isna() | (new[col] == 0), new[col], -1 * new[col]
                )
            df.loc[items_to_split, "counter_account"] = pd.NA
            df = pd.concat([df, new])

        # TODO: move this code block to parent class
        # Swap accounts if a counter_account but no account is provided,
        # or if individual transaction amount is negative
        swap_accounts = df["counter_account"].notna() & (
            (df["amount"] < 0) | df["account"].isna()
        )
        if swap_accounts.any():
            initial_account = df.loc[swap_accounts, "account"]
            df.loc[swap_accounts, "account"] = df.loc[
                swap_accounts, "counter_account"
            ]
            df.loc[swap_accounts, "counter_account"] = initial_account
            df.loc[swap_accounts, "amount"] = -1 * df.loc[swap_accounts, "amount"]
            df.loc[swap_accounts, "base_currency_amount"] = (
                -1 * df.loc[swap_accounts, "base_currency_amount"]
            )

        return df

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

    def attach_ledger_files(self, detach: bool = False):
        """Updates the attachments of all ledger entries based on the file paths specified
        in the 'reference' field of each journal entry. If a file with the specified path
        exists in the remote CashCtrl account, it will be attached to the corresponding
        ledger entry.

        Note: The 'reference' field in CashCtrl corresponds to the 'document' column in pyledger.

        Args:
            detach (bool, optional): If True, any files currently attached to ledger entries that do
                        not have a valid reference path or whose reference path does not
                        match an actual file will be detached. Defaults to False.
        """
        # Map ledger entries to their actual and targeted attachments
        attachments = self._get_ledger_attachments()
        ledger = self._client.list_journal_entries()
        ledger["reference"] = "/" + ledger["reference"]
        files = self._client.list_files()
        df = pd.DataFrame(
            {
                "ledger_id": ledger["id"],
                "target_attachment": np.where(
                    ledger["reference"].isin(files["path"]), ledger["reference"], pd.NA
                ),
                "actual_attachments": [
                    attachments.get(id, []) for id in ledger["id"]
                ],
            }
        )

        # Update attachments to align with the target attachments
        for id, target, actual in zip(
            df["ledger_id"], df["target_attachment"], df["actual_attachments"]
        ):
            if pd.isna(target):
                if actual and detach:
                    self._client.post(
                        "journal/update_attachments.json", data={"id": id, "fileIds": ""}
                    )
            elif (len(actual) != 1) or (actual[0] != target):
                file_id = self._client.file_path_to_id(target)
                self._client.post(
                    "journal/update_attachments.json",
                    data={"id": id, "fileIds": file_id},
                )
        self._client.invalidate_journal_cache()

    def _get_ledger_attachments(self, allow_missing=True) -> Dict[str, List[str]]:
        """Retrieves paths of files attached to CashCtrl ledger entries.

        Args:
            allow_missing (bool, optional): If True, return None if the file has no path,
                e.g. for files in the recylce bin. Otherwise raise a ValueError. Defaults to True.

        Returns:
            Dict[str, List[str]]: A Dict that contains ledger ids with attached
            files as keys and a list of file paths as values.
        """
        ledger = self._client.list_journal_entries()
        result = {}
        for id in ledger.loc[ledger["attachmentCount"] > 0, "id"]:
            res = self._client.get("journal/read.json", params={"id": id})["data"]
            paths = [
                self._client.file_id_to_path(
                    attachment["fileId"], allow_missing=allow_missing
                )
                for attachment in res["attachments"]
            ]
            if len(paths):
                result[id] = paths
        return result

    def _collective_transaction_currency_and_rate(
        self, entry: pd.DataFrame, suppress_error: bool = False
    ) -> Tuple[str, float]:
        """Extract a single currency and exchange rate from a collective transaction in pyledger
        format.

        - If all entries are in the base currency, return the base currency
          and an exchange rate of 1.0.
        - If more than one non-base currencies are present, raise a ValueError.
        - Otherwise, return the unique non-base currency and an exchange rate that converts all
        given non-base-currency amounts within the rounding precision to the base currency amounts.
        Raise a ValueError if no such exchange rate exists.

        In CashCtrl, collective transactions can be denominated in the accounting system's base
        currency and at most one additional foreign currency. This additional currency, if any,
        and a unique exchange rate to the base currency are recorded with the transaction.
        If all individual entries are denominated in the base currency, the base currency is
        set as the transaction currency.

        Individual entries can be linked to accounts denominated in the transaction's currency
        or the base currency. If in the base currency, the entry's amount is multiplied by the
        transaction's exchange rate when recorded in the account.

        This differs from pyledger, where each leg of a transaction specifies both foreign and
        base currency amounts. The present method facilitates mapping from CashCtrl to pyledger
        format.

        Args:
            entry (pd.DataFrame): The DataFrame representing individual entries of a collective
                                  transaction with columns 'currency', 'amount',
                                  and 'base_currency_amount'.
            suppress_error (bool): If True, suppresses ValueError when incoherent FX rates are
                                   found, otherwise raises ValueError. Defaults to False.

        Returns:
            Tuple[str, float]: The single currency and the corresponding exchange rate.

        Raises:
            ValueError: If more than one non-base currency is present or if no
                        coherent exchange rate is found.
            ValueError: If there are incoherent FX rates in the collective booking
                        and suppress_error is False.
        """
        if not isinstance(entry, pd.DataFrame) or entry.empty:
            raise ValueError("`entry` must be a pd.DataFrame with at least one row.")
        if "id" in entry.columns:
            id = entry["id"].iat[0]
        else:
            id = ""
        expected_columns = ["currency", "amount", "base_currency_amount"]
        if not set(expected_columns).issubset(entry.columns):
            missing = [col for col in expected_columns if col not in entry.columns]
            raise ValueError(f"Missing required column(s) {missing}: {id}.")

        # Check if all entries are denominated in base currency
        base_currency = self.base_currency
        is_base_txn = (
            entry["currency"].isna() | (entry["currency"] == base_currency) | (entry["amount"] == 0)
        )
        if all(is_base_txn):
            return base_currency, 1.0

        # Extract the sole non-base currency
        fx_entries = entry.loc[~is_base_txn]
        if fx_entries["currency"].nunique() != 1:
            raise ValueError(
                "CashCtrl allows only the base currency plus a single foreign currency in "
                f"a collective booking: {id}."
            )
        currency = fx_entries["currency"].iat[0]

        # Define precision parameters for exchange rate calculation
        # TODO: Derive `precision` from self.precision(base_currency) once this
        # method is implemented.
        precision = 0.01
        fx_rate_precision = 1e-8  # Precision for exchange rates in CashCtrl

        # Calculate the range of acceptable exchange rates
        base_amount = fx_entries["base_currency_amount"]
        tolerance = (fx_entries["amount"] * fx_rate_precision).clip(lower=precision / 2)
        lower_bound = base_amount - tolerance * np.where(base_amount < 0, -1, 1)
        upper_bound = base_amount + tolerance * np.where(base_amount < 0, -1, 1)
        min_fx_rate = (lower_bound / fx_entries["amount"]).max()
        max_fx_rate = (upper_bound / fx_entries["amount"]).min()

        # Select the exchange rate within the acceptable range closest to the preferred rate
        # derived from the largest absolute amount
        max_abs_amount = fx_entries["amount"].abs().max()
        is_max_abs = fx_entries["amount"].abs() == max_abs_amount
        fx_rates = fx_entries["base_currency_amount"] / fx_entries["amount"]
        preferred_rate = fx_rates.loc[is_max_abs].median()
        if min_fx_rate <= max_fx_rate:
            fx_rate = min(max(preferred_rate, min_fx_rate), max_fx_rate)
        elif suppress_error:
            fx_rate = round(preferred_rate, 8)
        else:
            raise ValueError("Incoherent FX rates in collective booking.")

        # Confirm fx_rate converts amounts to the expected base currency amount
        if not suppress_error:
            # TODO: Once precision() is implemented, use `round_to_precision()`
            if any(
                (fx_entries["amount"] * fx_rate).round(2)
                != fx_entries["base_currency_amount"].round(2)
            ):
                raise ValueError("Incoherent FX rates in collective booking.")

        return currency, fx_rate

    def _map_ledger_entry(self, entry: pd.DataFrame) -> dict:
        """Converts a single ledger entry to a data structure for upload to CashCtrl.

        Args:
            entry (pd.DataFrame): DataFrame with ledger entry in pyledger schema.

        Returns:
            dict: A data structure to post as json to the CashCtrl REST API.
        """
        entry = self.standardize_ledger(entry)
        base_currency = self.base_currency

        # Individual ledger entry
        if len(entry) == 1:
            amount = entry["amount"].iat[0]
            base_amount = entry["base_currency_amount"].iat[0]
            currency = entry["currency"].iat[0]
            if amount == 0 and not pd.isna(base_amount) and base_amount != 0:
                # Foreign currency adjustment: Solely changes in base currency amount
                currency = base_currency
                amount = base_amount
                fx_rate = 1
            else:
                amount = entry["amount"].iat[0]
                if currency == self.base_currency or amount == 0:
                    fx_rate = 1
                else:
                    fx_rate = base_amount / amount
            payload = {
                "dateAdded": entry["date"].iat[0],
                "amount": amount,
                "debitId": self._client.account_to_id(entry["account"].iat[0]),
                "creditId": self._client.account_to_id(entry["counter_account"].iat[0]),
                "currencyId": None
                if pd.isna(currency)
                else self._client.currency_to_id(currency),
                "title": entry["text"].iat[0],
                "taxId": None
                if pd.isna(entry["vat_code"].iat[0])
                else self._client.tax_code_to_id(entry["vat_code"].iat[0]),
                "currencyRate": fx_rate,
                "reference": None
                if pd.isna(entry["document"].iat[0])
                else entry["document"].iat[0],
            }

        # Collective ledger entry
        elif len(entry) > 1:
            # Individual transaction entries (line items)
            items = []
            currency, fx_rate = self._collective_transaction_currency_and_rate(entry)
            for _, row in entry.iterrows():
                if currency == base_currency and row["currency"] != currency:
                    amount = row["base_currency_amount"]
                elif row["currency"] == currency:
                    amount = row["amount"]
                elif row["currency"] == base_currency:
                    # TODO: Once precision() is implemented, use `round_to_precision()`
                    # instead of hard-coded rounding
                    amount = round(row["amount"] / fx_rate, 2)
                else:
                    raise ValueError(
                        "Currencies oder than base or transaction currency are not "
                        "allowed in CashCtrl collective transactions."
                    )
                # TODO: Once precision() is implemented, use `round_to_precision()`
                # instead of hard-coded rounding
                amount = round(amount, 2)
                items.append(
                    {
                        "accountId": self._client.account_to_id(row["account"]),
                        "credit": -amount if amount < 0 else None,
                        "debit": amount if amount >= 0 else None,
                        "taxId": None
                        if pd.isna(row["vat_code"])
                        else self._client.tax_code_to_id(row["vat_code"]),
                        "description": row["text"],
                    }
                )

            # Transaction-level attributes
            date = entry["date"].dropna().unique()
            document = entry["document"].dropna().unique()
            if len(date) == 0:
                raise ValueError("Date is not specified in collective booking.")
            elif len(date) > 1:
                raise ValueError("Date needs to be unique in a collective booking.")
            if len(document) > 1:
                raise ValueError(
                    "CashCtrl allows only one reference in a collective booking."
                )
            payload = {
                "dateAdded": date[0].strftime("%Y-%m-%d"),
                "currencyId": self._client.currency_to_id(currency),
                "reference": document[0] if len(document) == 1 else None,
                "currencyRate": fx_rate,
                "items": items,
            }
        else:
            raise ValueError("The ledger entry contains no transaction.")
        return payload

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
                # TODO: Once precision() is implemented, use `round_to_precision()`
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
                    # TODO: Once precision() is implemented, use `round_to_precision()`
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
                    # TODO: Once precision() is implemented, use `round_to_precision()`
                    result["amount"] = result["amount"].astype(pd.Float64Dtype()).round(2)
                    result["base_currency_amount"] = result["base_currency_amount"].astype(
                        pd.Float64Dtype()
                    ).round(2)
                    return self.standardize_ledger(result)

        else:
            raise ValueError("Expecting at least one `entry` row.")

    # ----------------------------------------------------------------------
    # Currencies

    @property
    def base_currency(self) -> str:
        """Returns the base currency of the CashCtrl account.

        Returns:
            str: The base currency code.
        """
        currencies = self._client.list_currencies()
        is_base_currency = currencies["isDefault"].astype("bool")
        if is_base_currency.sum() == 1:
            return currencies.loc[is_base_currency, "code"].item()
        elif is_base_currency.sum() == 0:
            raise ValueError("No base currency set.")
        else:
            raise ValueError("Multiple base currencies defined.")

    def precision(self, *args, **kwargs):
        """Returns the precision for given currencies.

        TODO: retuning precision for USD, CHF, EUR for now.
        Needs to be implemented for other tickers.

        Returns:
            float: The precision value.
        """
        return 0.01

    def price(self):
        """Not implemented yet."""
        raise NotImplementedError

    def add_price(self):
        """Not implemented yet."""
        raise NotImplementedError

    def price_history(self):
        """Not implemented yet."""
        raise NotImplementedError

    def delete_price(self):
        """Not implemented yet."""
        raise NotImplementedError
