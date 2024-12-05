"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

import datetime
import json
from typing import Dict, List, Tuple, Union
import zipfile
from cashctrl_api import CachedCashCtrlClient
import numpy as np
import pandas as pd
from pathlib import Path
from .tax_code import TaxCode
from .accounts import Account
from .ledger_entity import Ledger
from pyledger import LedgerEngine, CSVAccountingEntity
from pyledger.constants import (
    TAX_CODE_SCHEMA,
    ACCOUNT_SCHEMA,
    PRICE_SCHEMA,
    LEDGER_SCHEMA,
    ASSETS_SCHEMA
)
from .constants import (
    FX_REVALUATION_SCHEMA_CSV,
    JOURNAL_ITEM_COLUMNS,
    SETTINGS_KEYS
)
from consistent_df import unnest, enforce_dtypes, enforce_schema


class CashCtrlLedger(LedgerEngine):
    """Class that Implements the pyledger interface by connecting
    to the CashCtrl online accounting software.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    # TODO: remove when assets are implemented
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

    def __init__(
        self,
        client: CachedCashCtrlClient | None = None,
        price_history_path: Path = Path.cwd() / "price_history.csv",
        assets_path: Path = Path.cwd() / "assets.csv",
    ):
        super().__init__()
        client = CachedCashCtrlClient() if client is None else client
        self._client = client
        self._tax_codes = TaxCode(client=client, schema=TAX_CODE_SCHEMA)
        self._accounts = Account(client=client, schema=ACCOUNT_SCHEMA)
        self._price_history = CSVAccountingEntity(schema=PRICE_SCHEMA, path=price_history_path)
        self._assets = CSVAccountingEntity(schema=ASSETS_SCHEMA, path=assets_path)
        self._ledger = Ledger(
            client=client,
            schema=LEDGER_SCHEMA,
            list=self._ledger_list,
            add=self._ledger_add,
            modify=self._ledger_modify,
            delete=self._ledger_delete,
            standardize=self._ledger_standardize,
            prepare_for_mirroring=self.sanitize_ledger
        )

    # ----------------------------------------------------------------------
    # File operations

    def dump_to_zip(self, archive_path: str):
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('settings.json', json.dumps(self.settings_list()))
            archive.writestr('tax_codes.csv', self.tax_codes.list().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts.list().to_csv(index=False))
            archive.writestr('price_history.csv', self.price_history.list().to_csv(index=False))
            archive.writestr('ledger.csv', self.ledger.list().to_csv(index=False))
            archive.writestr('assets.csv', self.price_history.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        required_files = {
            'tax_codes.csv', 'accounts.csv', 'settings.json', 'price_history.csv', 'assets.csv'
        }

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            tax_codes = pd.read_csv(archive.open('tax_codes.csv'))
            price_history = pd.read_csv(archive.open('price_history.csv'))
            ledger = pd.read_csv(archive.open('ledger.csv'))
            assets = pd.read_csv(archive.open('assets.csv'))
            self.restore(
                settings=settings,
                tax_codes=tax_codes,
                accounts=accounts,
                price_history=price_history,
                ledger=ledger,
                assets=assets,
            )

    def restore(
        self,
        settings: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        price_history: pd.DataFrame | None = None,
        ledger: pd.DataFrame | None = None,
        assets: pd.DataFrame | None = None,
    ):
        self.clear()
        if accounts is not None:
            self.accounts.mirror(accounts.assign(tax_code=pd.NA), delete=True)
        if tax_codes is not None:
            self.tax_codes.mirror(tax_codes, delete=True)
        if accounts is not None:
            self.accounts.mirror(accounts, delete=True)
        if settings is not None:
            self.settings_modify(settings)
        if assets is not None:
            self.assets.mirror(assets, delete=True)
        if price_history is not None:
            self.price_history.mirror(price_history, delete=True)
        if ledger is not None:
            self.ledger.mirror(ledger, delete=True)

        # TODO: Implement logic for other entities

    def clear(self):
        self.ledger.mirror(None, delete=True)
        self.settings_clear()

        # Manually reset accounts tax to none
        accounts = self.accounts.list()
        self.accounts.mirror(accounts.assign(tax_code=pd.NA))
        self.tax_codes.mirror(None, delete=True)
        self.accounts.mirror(None, delete=True)
        self.price_history.mirror(None, delete=True)
        self.assets.mirror(None, delete=True)
        # TODO: Implement logic for other entities

    # ----------------------------------------------------------------------
    # Settings

    def settings_list(self) -> dict:
        roundings = self._client.get("rounding/list.json")["data"]
        for rounding in roundings:
            rounding["account"] = self._client.account_from_id(rounding["accountId"])
            rounding.pop("accountId")

        system_settings = self._client.get("setting/read.json")
        cash_ctrl_settings = {
            key: self._client.account_from_id(system_settings[key])
            for key in SETTINGS_KEYS if key in system_settings
        }

        return {
            "REPORTING_CURRENCY": self.reporting_currency,
            "ROUNDING": roundings,
            "CASH_CTRL": cash_ctrl_settings
        }

    def settings_modify(self, settings: dict = {}):
        if "REPORTING_CURRENCY" in settings:
            self.reporting_currency = settings["REPORTING_CURRENCY"]

        if "ROUNDING" in settings:
            for rounding in settings["ROUNDING"]:
                rounding["accountId"] = self._client.account_to_id(rounding["account"])
                self._client.post("rounding/create.json", data=rounding)

        if "CASH_CTRL" in settings:
            system_settings = {
                key: self._client.account_to_id(settings["CASH_CTRL"][key])
                for key in SETTINGS_KEYS if key in settings["CASH_CTRL"]
            }
            self._client.post("setting/update.json", data=system_settings)

    def settings_clear(self):
        empty_settings = {key: "" for key in SETTINGS_KEYS}
        self._client.post("setting/update.json", empty_settings)
        roundings = self._client.get("rounding/list.json")["data"]
        if len(roundings):
            ids = ','.join(str(item['id']) for item in roundings)
            self._client.post("rounding/delete.json", data={"ids": ids})

    # ----------------------------------------------------------------------
    # Accounts

    def _single_account_balance(
        self, account: int, date: Union[datetime.date, None] = None
    ) -> dict:
        """Calculate the balance of a single account in both account currency
        and reporting currency.

        Args:
            account (int): The account number.
            date (datetime.date, optional): The date for the balance. Defaults to None,
                in which case the balance on the last day of the current fiscal period is returned.

        Returns:
            dict: A dictionary with the balance in the account currency and the reporting currency.
        """
        account_id = self._client.account_to_id(account)
        params = {"id": account_id, "date": date}
        response = self._client.request("GET", "account/balance", params=params)
        balance = float(response.text)

        account_currency = self._client.account_to_currency(account)
        if self.reporting_currency == account_currency:
            reporting_currency_balance = balance
        else:
            response = self._client.get(
                "fiscalperiod/exchangediff.json", params={"date": date}
            )
            exchange_diff = pd.DataFrame(response["data"])
            reporting_currency_balance = exchange_diff.loc[
                exchange_diff["accountId"] == account_id, "dcBalance"
            ].item()

        return {account_currency: balance, "reporting_currency": reporting_currency_balance}

        # ----------------------------------------------------------------------
    # Ledger

    def _ledger_list(self) -> pd.DataFrame:
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
        reporting_currency = self.reporting_currency
        is_fx_adjustment = (
            (currency == reporting_currency)
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
                "contra": individual["credit_account"],
                "currency": individual["currencyCode"],
                "amount": individual["amount"],
                "report_amount": self.round_to_precision(
                    np.where(
                        is_fx_adjustment,
                        pd.NA,
                        individual["amount"] * individual["currencyRate"],
                    ),
                    self.reporting_currency,
                ),
                "tax_code": individual["taxName"],
                "description": individual["title"],
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

            # Identify reporting currency or foreign currency adjustment transactions
            reporting_currency = self.reporting_currency
            is_fx_adjustment = (collective["account_currency"] != reporting_currency) & (
                collective["currency"].isna() | (collective["currency"] == reporting_currency)
            )

            amount = collective["debit"].fillna(0) - collective["credit"].fillna(0)
            currency = collective["account_currency"]
            reporting_amount = np.where(
                currency == reporting_currency,
                pd.NA,
                np.where(is_fx_adjustment, amount, amount * collective["fx_rate"]),
            )
            foreign_amount = np.where(
                currency == reporting_currency,
                amount * collective["fx_rate"],
                np.where(is_fx_adjustment, 0, amount),
            )
            mapped_collective = pd.DataFrame({
                "id": collective["id"],
                "date": collective["date"],
                "account": collective["account"],
                "currency": currency,
                "amount": self.round_to_precision(foreign_amount, currency),
                "report_amount": self.round_to_precision(reporting_amount, reporting_currency),
                "tax_code": collective["taxName"],
                "description": collective["description"],
                "document": collective["document"]
            })
            result = pd.concat([
                self.ledger.standardize(result),
                self.ledger.standardize(mapped_collective),
            ])

        return self.ledger.standardize(result).reset_index(drop=True)

    def ledger_entry(self):
        """Not implemented yet."""
        raise NotImplementedError

    def _ledger_add(self, data: pd.DataFrame) -> list[str]:
        ids = []
        incoming = self.ledger.standardize(data)
        for id in incoming["id"].unique():
            entry = incoming.query("id == @id")
            payload = self._map_ledger_entry(entry)
            res = self._client.post("journal/create.json", data=payload)
            ids.append(str(res["insertId"]))
            self._client.invalidate_journal_cache()
        return ids

    def _ledger_modify(self, data: pd.DataFrame):
        incoming = self.ledger.standardize(data)
        for id in incoming["id"].unique():
            entry = incoming.query("id == @id")
            payload = self._map_ledger_entry(entry)
            payload["id"] = id
            self._client.post("journal/update.json", data=payload)
            self._client.invalidate_journal_cache()

    def _ledger_delete(self, id: pd.DataFrame, allow_missing=False):
        incoming = enforce_schema(pd.DataFrame(id), LEDGER_SCHEMA.query("id"))
        self._client.post(
            "journal/delete.json", {"ids": ",".join([str(id) for id in incoming["id"]])}
        )
        self._client.invalidate_journal_cache()

    def _ledger_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the ledger DataFrame to conform to CashCtrl format.

        Args:
            ledger (pd.DataFrame): The ledger DataFrame to be standardized.

        Returns:
            pd.DataFrame: The standardized ledger DataFrame.
        """
        # Drop redundant report_amount for transactions in reporting currency
        set_na = (
            (df["currency"] == self.reporting_currency)
            & (df["report_amount"].isna() | (df["report_amount"] == df["amount"]))
        )
        df.loc[set_na, "report_amount"] = pd.NA

        # Set reporting amount to 0 for transactions not in reporting currency with 0 amount
        mask = (df["currency"] != self.reporting_currency) & (df["amount"] == 0)
        df.loc[mask, "report_amount"] = 0

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
            is_collective & df["account"].notna() & df["contra"].notna()
        )
        if items_to_split.any():
            new = df.loc[items_to_split].copy()
            new["account"] = new["contra"]
            new.loc[:, "contra"] = pd.NA
            for col in ["amount", "report_amount"]:
                new[col] = np.where(
                    new[col].isna() | (new[col] == 0), new[col], -1 * new[col]
                )
            df.loc[items_to_split, "contra"] = pd.NA
            df = pd.concat([df, new])

        # TODO: move this code block to parent class
        # Swap accounts if a contra but no account is provided,
        # or if individual transaction amount is negative
        swap_accounts = df["contra"].notna() & (
            (df["amount"] < 0) | df["account"].isna()
        )
        if swap_accounts.any():
            initial_account = df.loc[swap_accounts, "account"]
            df.loc[swap_accounts, "account"] = df.loc[
                swap_accounts, "contra"
            ]
            df.loc[swap_accounts, "contra"] = initial_account
            df.loc[swap_accounts, "amount"] = -1 * df.loc[swap_accounts, "amount"]
            df.loc[swap_accounts, "report_amount"] = (
                -1 * df.loc[swap_accounts, "report_amount"]
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
        ledger["id"] = ledger["id"].astype("string[python]")
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
                e.g. for files in the recycle bin. Otherwise raise a ValueError. Defaults to True.

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
                result[str(id)] = paths
        return result

    def _collective_transaction_currency_and_rate(
        self, entry: pd.DataFrame, suppress_error: bool = False
    ) -> Tuple[str, float]:
        """Extract a single currency and exchange rate from a collective transaction in pyledger
        format.

        - If all entries are in the reporting currency, return the reporting currency
          and an exchange rate of 1.0.
        - If more than one non-reporting currencies are present, raise a ValueError.
        - Otherwise, return the unique non-reporting currency and an exchange rate that converts all
        given non-reporting-currency amounts within the rounding precision to the reporting
        currency amounts. Raise a ValueError if no such exchange rate exists.

        In CashCtrl, collective transactions can be denominated in the accounting system's reporting
        currency and at most one additional foreign currency. This additional currency, if any,
        and a unique exchange rate to the reporting currency are recorded with the transaction.
        If all individual entries are denominated in the reporting currency, the reporting currency
        is set as the transaction currency.

        Individual entries can be linked to accounts denominated in the transaction's currency
        or the reporting currency. If in the reporting currency, the entry's amount is multiplied
        by the transaction's exchange rate when recorded in the account.

        This differs from pyledger, where each leg of a transaction specifies both foreign and
        reporting currency amounts. The present method facilitates mapping from CashCtrl to pyledger
        format.

        Args:
            entry (pd.DataFrame): The DataFrame representing individual entries of a collective
                                  transaction with columns 'currency', 'amount',
                                  and 'report_amount'.
            suppress_error (bool): If True, suppresses ValueError when incoherent FX rates are
                                   found, otherwise raises ValueError. Defaults to False.

        Returns:
            Tuple[str, float]: The single currency and the corresponding exchange rate.

        Raises:
            ValueError: If more than one non-reporting currency is present or if no
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
        expected_columns = ["currency", "amount", "report_amount"]
        if not set(expected_columns).issubset(entry.columns):
            missing = [col for col in expected_columns if col not in entry.columns]
            raise ValueError(f"Missing required column(s) {missing}: {id}.")

        # Check if all entries are denominated in reporting currency
        reporting_currency = self.reporting_currency
        is_reporting_txn = (
            entry["currency"].isna()
            | (entry["currency"] == reporting_currency)
            | (entry["amount"] == 0)
        )
        if all(is_reporting_txn):
            return reporting_currency, 1.0

        # Extract the sole non-reporting currency
        fx_entries = entry.loc[~is_reporting_txn]
        if fx_entries["currency"].nunique() != 1:
            raise ValueError(
                "CashCtrl allows only the reporting currency plus a single foreign currency in "
                f"a collective booking: {id}."
            )
        currency = fx_entries["currency"].iat[0]

        # Define precision parameters for exchange rate calculation
        precision = self.precision(reporting_currency)
        fx_rate_precision = 1e-8  # Precision for exchange rates in CashCtrl

        # Calculate the range of acceptable exchange rates
        reporting_amount = fx_entries["report_amount"]
        tolerance = (fx_entries["amount"] * fx_rate_precision).clip(lower=precision / 2)
        lower_bound = reporting_amount - tolerance * np.where(reporting_amount < 0, -1, 1)
        upper_bound = reporting_amount + tolerance * np.where(reporting_amount < 0, -1, 1)
        min_fx_rate = (lower_bound / fx_entries["amount"]).max()
        max_fx_rate = (upper_bound / fx_entries["amount"]).min()

        # Select the exchange rate within the acceptable range closest to the preferred rate
        # derived from the largest absolute amount
        max_abs_amount = fx_entries["amount"].abs().max()
        is_max_abs = fx_entries["amount"].abs() == max_abs_amount
        fx_rates = fx_entries["report_amount"] / fx_entries["amount"]
        preferred_rate = fx_rates.loc[is_max_abs].median()
        if min_fx_rate <= max_fx_rate:
            fx_rate = min(max(preferred_rate, min_fx_rate), max_fx_rate)
        elif suppress_error:
            fx_rate = round(preferred_rate, 8)
        else:
            raise ValueError("Incoherent FX rates in collective booking.")

        # Confirm fx_rate converts amounts to the expected reporting currency amount
        if not suppress_error:
            rounded_amounts = self.round_to_precision(
                fx_entries["amount"] * fx_rate, self.reporting_currency,
            )
            expected_rounded_amounts = self.round_to_precision(
                fx_entries["report_amount"], self.reporting_currency
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
        entry = self.ledger.standardize(entry)
        reporting_currency = self.reporting_currency

        # Individual ledger entry
        if len(entry) == 1:
            amount = entry["amount"].iat[0]
            reporting_amount = entry["report_amount"].iat[0]
            currency = entry["currency"].iat[0]
            if amount == 0 and not pd.isna(reporting_amount) and reporting_amount != 0:
                # Foreign currency adjustment: Solely changes in reporting currency amount
                currency = reporting_currency
                amount = reporting_amount
                fx_rate = 1
            else:
                amount = entry["amount"].iat[0]
                if currency == self.reporting_currency or amount == 0:
                    fx_rate = 1
                else:
                    fx_rate = reporting_amount / amount
            payload = {
                "dateAdded": entry["date"].iat[0],
                "amount": amount,
                "debitId": self._client.account_to_id(entry["account"].iat[0]),
                "creditId": self._client.account_to_id(entry["contra"].iat[0]),
                "currencyId": None
                if pd.isna(currency)
                else self._client.currency_to_id(currency),
                "title": entry["description"].iat[0],
                "taxId": None
                if pd.isna(entry["tax_code"].iat[0])
                else self._client.tax_code_to_id(entry["tax_code"].iat[0]),
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
                if currency == reporting_currency and row["currency"] != currency:
                    amount = row["report_amount"]
                elif row["currency"] == currency:
                    amount = row["amount"]
                elif row["currency"] == reporting_currency:
                    amount = row["amount"] / fx_rate
                else:
                    raise ValueError(
                        "Currencies other than reporting or transaction currency are not "
                        "allowed in CashCtrl collective transactions."
                    )
                amount = self.round_to_precision(amount, currency)
                items.append(
                    {
                        "accountId": self._client.account_to_id(row["account"]),
                        "credit": -amount if amount < 0 else None,
                        "debit": amount if amount >= 0 else None,
                        "taxId": None
                        if pd.isna(row["tax_code"])
                        else self._client.tax_code_to_id(row["tax_code"]),
                        "description": row["description"],
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

def book_revaluations(self, revaluations: pd.DataFrame) -> pd.DataFrame:
    """Book foreign exchange revaluations."""

    def get_fx_gain_loss_account(allow_missing=False) -> int:
        """Retrieves the FX gain/loss account from the settings."""
        settings = self._client.get("setting/read.json")
        account_id = settings.get("DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID", None)
        return self._client.account_from_id(account_id, allow_missing=allow_missing)

    def set_fx_gain_loss_account(account: int, allow_missing=False):
        """Sets the FX gain/loss account in the settings."""
        account_id = self._client.account_to_id(account, allow_missing=allow_missing)
        payload = {"DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID": account_id}
        self._client.post("setting/update.json", params=payload)

    initial_fx_gain_loss_account = get_fx_gain_loss_account(allow_missing=True)

    revaluations = revaluations.copy().sort_values(by=["date"])
    for row in revaluations.to_dict('records'):
        if row['credit'] == row['debit']:
            raise ValueError("CashCtrl allows to solely specify a single account")

        accounts = self.account_range(row['account'])
        accounts = set(accounts['add']) - set(accounts['subtract'])
        fx_gain_loss_account = row['credit'] if pd.isna(row['credit']) else row['debit']
        set_fx_gain_loss_account(fx_gain_loss_account) # TODO: if amount > 0 else row['debit']
        exchange_diff = [
            {
                "accountId": self._client.account_to_id(account),
                "currencyRate": self.price(
                    ticker=self.account_currency(account),
                    date=row['date'],
                    currency=self.reporting_currency
                )[1]
            }
            for account in accounts
        ]
        payload = {"date": row["date"], "exchangeDiff": exchange_diff}
        self._client.post("fiscalperiod/bookexchangediff.json", params=payload)

    # Restore initial setting
    set_fx_gain_loss_account(initial_fx_gain_loss_account, allow_missing=True)

    # ----------------------------------------------------------------------
    # Currencies

    @property
    def reporting_currency(self) -> str:
        """Returns the reporting currency of the CashCtrl account.

        Returns:
            str: The reporting currency code.
        """
        currencies = self._client.list_currencies()
        is_reporting_currency = currencies["isDefault"].astype("bool")
        if is_reporting_currency.sum() == 1:
            return currencies.loc[is_reporting_currency, "code"].item()
        elif is_reporting_currency.sum() == 0:
            raise ValueError("No reporting currency set.")
        else:
            raise ValueError("Multiple reporting currencies defined.")

    @reporting_currency.setter
    def reporting_currency(self, currency):
        # TODO: Perform testing of this method after restore() for currencies implemented
        currencies = self._client.list_currencies()
        if currency in set(currencies["code"]):
            target_currency = currencies[currencies["code"] == currency].iloc[0]
            payload = {
                "id": target_currency["id"],
                "code": currency,
                "isDefault": True,
                "description": target_currency["description"],
                "rate": target_currency["rate"]
            }
            self._client.post("currency/update.json", data=payload)
        else:
            payload = {
                "code": currency,
                "isDefault": True,
                "description": "Reporting Currency",
                "rate": 1
            }
            self._client.post("currency/create.json", data=payload)

        self._client.invalidate_currencies_cache()
