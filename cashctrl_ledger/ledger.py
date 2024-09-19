"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

from datetime import datetime
from typing import Dict, List, Tuple, Union
from cashctrl_api import CachedCashCtrlClient
from consistent_df import df_to_consistent_str, nest, unnest, enforce_dtypes
import numpy as np
import pandas as pd
import zipfile
import json
from pyledger import LedgerEngine, StandaloneLedger
from .constants import JOURNAL_ITEM_COLUMNS, SETTINGS_KEYS


class CashCtrlLedger(LedgerEngine):
    """Class that Implements the pyledger interface by connecting
    to the CashCtrl online accounting software.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    _precision = {
        "AUD": 0.01,
        "CAD": 0.01,
        "CHF": 0.01,
        "EUR": 0.01,
        "GBP": 0.01,
        "JPY": 1.00,
        "NZD": 0.01,
        "NOK": 0.01,
        "SEK": 0.01,
        "USD": 0.01,
    }

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, client: Union[CachedCashCtrlClient, None] = None):
        super().__init__()
        self._client = CachedCashCtrlClient() if client is None else client

    # ----------------------------------------------------------------------
    # File operations

    def dump_to_zip(self, archive_path: str):
        with zipfile.ZipFile(archive_path, 'w') as archive:
            settings = {"BASE_CURRENCY": self.base_currency}

            roundings = self._client.get("rounding/list.json")["data"]
            for rounding in roundings:
                rounding["accountId"] = self._client.account_from_id(rounding["accountId"])
            settings["DEFAULT_ROUNDINGS"] = roundings

            default_settings = {}
            system_settings = self._client.get("setting/read.json")
            for key in SETTINGS_KEYS:
                if system_settings.get(key, None) is not None:
                    default_settings[key] = self._client.account_from_id(system_settings[key])
            settings["DEFAULT_SETTINGS"] = default_settings

            archive.writestr('settings.json', json.dumps(settings))
            archive.writestr('ledger.csv', self.ledger().to_csv(index=False))
            archive.writestr('vat_codes.csv', self.vat_codes().to_csv(index=False))
            archive.writestr('accounts.csv', self.account_chart().to_csv(index=False))

    def restore(
        self,
        settings: dict | None = None,
        vat_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        ledger: pd.DataFrame | None = None,
    ):
        self.clear()
        vat_accounts = None
        roundings = None
        base_currency = None
        system_settings = None

        if settings is not None:
            roundings = settings.get("DEFAULT_ROUNDINGS", None)
            base_currency = settings.get("BASE_CURRENCY", None)
            system_settings = settings.get("DEFAULT_SETTINGS", None)

        if base_currency is not None:
            self.base_currency = base_currency
        if accounts is not None:
            vat_accounts = accounts[accounts["vat_code"].notna()]
            accounts["vat_code"] = pd.NA
            self.mirror_account_chart(accounts, delete=True)
        if vat_codes is not None:
            self.mirror_vat_codes(vat_codes, delete=True)
        if vat_accounts is not None and not vat_accounts.empty:
            self.mirror_account_chart(vat_accounts)
        if ledger is not None:
            self.mirror_ledger(ledger, delete=True)
        if system_settings is not None:
            for key in SETTINGS_KEYS:
                if system_settings.get(key, None) is not None:
                    system_settings[key] = self._client.account_to_id(system_settings[key])
            self._client.post("setting/update.json", data=system_settings)
        if roundings is not None:
            for rounding in roundings:
                rounding["accountId"] = self._client.account_to_id(rounding["accountId"])
                self._client.post("rounding/create.json", data=rounding)
        # TODO: Implement price history, precision settings,
        # and FX adjustments restoration logic

    def clear(self):
        self.mirror_ledger(None, delete=True)

        # Clear default System settings
        empty_settings = {key: "" for key in SETTINGS_KEYS}
        self._client.post("setting/update.json", empty_settings)
        roundings = self._client.get("rounding/list.json")["data"]
        if len(roundings):
            ids = ','.join(str(item['id']) for item in roundings)
            self._client.post("rounding/delete.json", data={"ids": ids})

        # Manually reset accounts VAT to none
        accounts = self.account_chart()
        vat_accounts = accounts[accounts["vat_code"].notna()]
        vat_accounts["vat_code"] = pd.NA
        self.mirror_account_chart(vat_accounts)
        self.mirror_vat_codes(None, delete=True)
        self.mirror_account_chart(None, delete=True)
        # TODO: Implement price history, precision settings, and FX adjustments clearing logic

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

    def modify_vat_code(
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
            self.modify_vat_code(
                code=row["id"],
                text=row["text"],
                account=row["account"],
                rate=row["rate"],
                inclusive=row["inclusive"],
            )

    # ----------------------------------------------------------------------
    # Accounts

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
        return self.standardize_account_chart(result)

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

    def delete_accounts(self, accounts: List[int] = [], allow_missing: bool = False):
        """Deletes an account from the remote CashCtrl instance.

        Args:
            accounts (str[]): The account numbers to be deleted.
            allow_missing (bool, optional): If True, do not raise an error if the
                                            account is missing. Defaults to False.
        """
        ids = []
        for account in accounts:
            id = self._client.account_to_id(account, allow_missing)
            if id is not None:
                ids.append(str(id))
        if len(ids):
            self._client.post("account/delete.json", {"ids": ", ".join(ids)})
            self._client.invalidate_accounts_cache()

    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = False):
        """Synchronizes remote CashCtrl accounts with a desired target state
        provided as a DataFrame.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes accounts on the remote that are not
                                     present in the target DataFrame.
        """
        if target is not None:
            target = target.copy()
        target_df = StandaloneLedger.standardize_account_chart(target).reset_index()
        current_state = self.account_chart().reset_index()

        # Delete superfluous accounts on remote
        if delete:
            self.delete_accounts(
                set(current_state["account"]).difference(set(target_df["account"]))
            )

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
            if df is None or df.empty:
                return {}

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
                "base_currency_amount": self.round_to_precision(
                    np.where(
                        is_fx_adjustment,
                        pd.NA,
                        individual["amount"] * individual["currencyRate"],
                    ),
                    self.base_currency,
                ),
                "document": individual["reference"],
            }
        )

        # Collective ledger entries represent a group of transactions and
        # map to multiple rows in the resulting data frame with the same id.
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
                "amount": self.round_to_precision(foreign_amount, currency),
                "base_currency_amount": self.round_to_precision(base_amount, base_currency),
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
        precision = self.precision(base_currency)
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
            rounded_amounts = self.round_to_precision(
                fx_entries["amount"] * fx_rate, self.base_currency,
            )
            expected_rounded_amounts = self.round_to_precision(
                fx_entries["base_currency_amount"], self.base_currency
            )
            if rounded_amounts != expected_rounded_amounts:
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
                    amount = row["amount"] / fx_rate
                else:
                    raise ValueError(
                        "Currencies other than base or transaction currency are not "
                        "allowed in CashCtrl collective transactions."
                    )
                amount = self.round_to_precision(amount, currency)
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

    @base_currency.setter
    def base_currency(self, currency):
        currencies = self._client.list_currencies()
        target_currency = currencies[currencies["code"] == currency].iloc[0]
        payload = {
            "code": currency,
            "id": target_currency["id"],
            "isDefault": True,
            "description": target_currency["description"],
            "rate": target_currency["rate"]
        }

        self._client.post("currency/update.json", data=payload)
        self._client.invalidate_currencies_cache()

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        return self._precision.get(ticker, 0.01)

    def set_precision(self, ticker: str, precision: float):
        """
        Set the precision or minimal price increment for a given asset or currency.

        Args:
            ticker (str): Unique identifier of the currency or asset.
            precision (float): Minimal price increment to round to.
        """
        self._precision[ticker] = precision

    def price(self, currency: str, date: datetime.date = None) -> float:
        """
        Retrieves the price (exchange rate) of a given currency in terms
        of the base currency.

        Args:
            currency (str): The currency code to retrieve the price for.
            date (datetime.date, optional): The date for which the price is
                requested. Defaults to None, which retrieves the latest price.

        Returns:
            float: The exchange rate between the currency and the base currency.
        """
        return self._client.get_exchange_rate(
            from_currency=currency,
            to_currency=self.base_currency,
            date=date
        )

    def add_price(self):
        raise NotImplementedError(
            "Cashctrl doesn't support adding exchange rates through the API."
        )

    def delete_price(self):
        raise NotImplementedError(
            "Cashctrl doesn't support deleting exchange rates through the API."
        )

    def price_history(self):
        raise NotImplementedError(
            "Cashctrl doesn't support reading the exchange rate history through the API."
        )
