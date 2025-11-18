"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

import datetime
import json
from typing import Dict, List, Tuple
import zipfile
from cashctrl_api import CashCtrlClient
import numpy as np
import pandas as pd
from pathlib import Path
from cashctrl_ledger.profit_center import ProfitCenter
from .tax_code import TaxCode
from .accounts import Account
from .journal_entity import Journal
from pyledger import LedgerEngine, CSVAccountingEntity
from pyledger.constants import (
    TAX_CODE_SCHEMA,
    ACCOUNT_SCHEMA,
    PRICE_SCHEMA,
    JOURNAL_SCHEMA,
    ASSETS_SCHEMA,
    PROFIT_CENTER_SCHEMA
)
from .constants import (
    ACCOUNT_CATEGORIES_NEED_TO_NEGATE,
    ACCOUNT_ROOT_CATEGORIES,
    FISCAL_PERIOD_SCHEMA,
    JOURNAL_ITEM_COLUMNS,
    CONFIGURATION_KEYS,
    REPORT_ELEMENT,
    REPORTING_CURRENCY_TAG
)
from consistent_df import unnest, enforce_dtypes, enforce_schema
from pyledger.time import parse_date_span
from difflib import get_close_matches


class CashCtrlLedger(LedgerEngine):
    """Class that Implements the pyledger interface by connecting
    to the CashCtrl online accounting software.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(
        self,
        client: CashCtrlClient | None = None,
        root: Path = Path.cwd(),
        price_history_path: str = "settings/price_history.csv",
        assets_path: str = "settings/assets.csv",
    ):
        super().__init__()
        self.root = Path(root).expanduser()
        settings_dir = self.root / "settings"
        settings_dir.mkdir(parents=True, exist_ok=True)
        client = CashCtrlClient() if client is None else client
        self._client = client
        self._tax_codes = TaxCode(
            client=client,
            schema=TAX_CODE_SCHEMA,
            source_column="source",
        )
        self._accounts = Account(
            client=client,
            schema=ACCOUNT_SCHEMA,
            source_column="source",
        )
        self._price_history = CSVAccountingEntity(
            schema=PRICE_SCHEMA,
            root=self.root,
            path=price_history_path,
            source_column="source"
        )
        # CashCtrl does not allow to store Assets with corresponding precision
        # in the historical order, which prevents the implementation of coherent
        # accessors and mutators storing assets on the remote instance.
        # Instead, we use the CSVAccountingEntity to store assets in a local
        # text file, supplemented by the _ensure_currencies_exist() method to
        # ensure all tickers present in the local text file also exists remotely as currencies.
        self._assets = CSVAccountingEntity(
            schema=ASSETS_SCHEMA,
            root=self.root,
            path=assets_path,
            on_change=self._on_assets_change,
            source_column="source",
        )
        self._ensure_currencies_exist()
        self._journal = Journal(
            client=client,
            schema=JOURNAL_SCHEMA,
            source_column="source",
            list=self._journal_list,
            add=self._journal_add,
            modify=self._journal_modify,
            delete=self._journal_delete,
            standardize=self._journal_standardize,
            prepare_for_mirroring=self.sanitize_journal
        )
        self._profit_centers = ProfitCenter(
            client=client,
            schema=PROFIT_CENTER_SCHEMA,
            source_column="source",
        )

    # ----------------------------------------------------------------------
    # File operations

    def dump_to_zip(self, archive_path: str):
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('configuration.json', json.dumps(self.configuration_list()))
            archive.writestr('tax_codes.csv', self.tax_codes.list().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts.list().to_csv(index=False))
            archive.writestr('price_history.csv', self.price_history.list().to_csv(index=False))
            archive.writestr('journal.csv', self.journal.list().to_csv(index=False))
            archive.writestr('assets.csv', self.assets.list().to_csv(index=False))
            archive.writestr('profit_centers.csv', self.profit_centers.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        required_files = {
            'journal.csv', 'tax_codes.csv', 'accounts.csv', 'configuration.json', 'assets.csv',
            'price_history.csv'
        }

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            configuration = json.loads(archive.open('configuration.json').read().decode('utf-8'))
            journal = pd.read_csv(archive.open('journal.csv'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            tax_codes = pd.read_csv(archive.open('tax_codes.csv'))
            assets = pd.read_csv(archive.open('assets.csv'))
            price_history = pd.read_csv(archive.open('price_history.csv'))
            profit_centers = pd.read_csv(archive.open('profit_centers.csv'))
            self.restore(
                configuration=configuration,
                journal=journal,
                tax_codes=tax_codes,
                accounts=accounts,
                assets=assets,
                price_history=price_history,
                profit_centers=profit_centers,
            )

    def restore(
        self,
        configuration: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
        price_history: pd.DataFrame | None = None,
        journal: pd.DataFrame | None = None,
        assets: pd.DataFrame | None = None,
        profit_centers: pd.DataFrame | None = None,
    ):
        self.clear()
        if configuration is not None and "REPORTING_CURRENCY" in configuration:
            self.reporting_currency = configuration["REPORTING_CURRENCY"]
        if accounts is not None:
            self.accounts.mirror(accounts.assign(tax_code=pd.NA), delete=True)
        if tax_codes is not None:
            self.tax_codes.mirror(tax_codes, delete=True)
        if accounts is not None:
            self.accounts.mirror(accounts, delete=True)
        if configuration is not None:
            self.configuration_modify(configuration)
        if assets is not None:
            self.assets.mirror(assets, delete=True)
        if price_history is not None:
            self.price_history.mirror(price_history, delete=True)
        if profit_centers is not None:
            self.profit_centers.mirror(profit_centers, delete=True)
        if journal is not None:
            self.journal.mirror(journal, delete=True)

    def clear(self):
        self.journal.mirror(None, delete=True)
        self.configuration_clear()

        # Manually reset accounts tax to none
        accounts = self.accounts.list()
        self.accounts.mirror(accounts.assign(tax_code=pd.NA))
        self.tax_codes.mirror(None, delete=True)
        self.accounts.mirror(None, delete=True)
        self.profit_centers.mirror(None, delete=True)
        self.price_history.mirror(None, delete=True)
        self.assets.mirror(None, delete=True)

    # ----------------------------------------------------------------------
    # configuration

    def configuration_list(self) -> dict:
        roundings = self._client.get("rounding/list.json")["data"]
        for rounding in roundings:
            rounding["account"] = self._client.account_from_id(rounding["accountId"])
            rounding.pop("accountId")

        system_configuration = self._client.get("setting/read.json")
        cash_ctrl_configuration = {
            key: self._client.account_from_id(system_configuration[key])
            for key in CONFIGURATION_KEYS if key in system_configuration
        }

        return {
            "REPORTING_CURRENCY": self.reporting_currency,
            "ROUNDING": roundings,
            "CASH_CTRL": cash_ctrl_configuration
        }

    def configuration_modify(self, configuration: dict = {}):
        if "REPORTING_CURRENCY" in configuration:
            self.reporting_currency = configuration["REPORTING_CURRENCY"]

        if "ROUNDING" in configuration:
            for rounding in configuration["ROUNDING"]:
                payload = {
                    **rounding,
                    "accountId": self._client.account_to_id(rounding["account"])
                }
                self._client.post("rounding/create.json", data=payload)

        if "CASH_CTRL" in configuration:
            system_configuration = {
                key: self._client.account_to_id(configuration["CASH_CTRL"][key])
                for key in CONFIGURATION_KEYS if key in configuration["CASH_CTRL"]
            }
            self._client.post("setting/update.json", data=system_configuration)

    def configuration_clear(self):
        empty_configuration = {key: "" for key in CONFIGURATION_KEYS}
        self._client.post("setting/update.json", empty_configuration)
        roundings = self._client.get("rounding/list.json")["data"]
        if len(roundings):
            ids = ','.join(str(item['id']) for item in roundings)
            self._client.post("rounding/delete.json", data={"ids": ids})

    # ----------------------------------------------------------------------
    # Accounts

    def sanitize_account_groups(self, groups: pd.Series) -> pd.Series:
        """Ensure account groups start with a leading slash and a valid root node.

        Account categories in CashCtrl must be assigned to one of the pre-defined root nodes.
        Additional root categories are not allowed, and the pre-defined root categories
        cannot be deleted.

        Args:
            groups (pd.Series): A pandas Series containing account group paths.

        Returns:
            pd.Series: Sanitized account group series.
        """
        groups = groups.str.replace(r'^/', '', regex=True)
        first_nodes = groups.str.replace(r'/.*', '', regex=True)
        first_nodes = first_nodes.apply(
            lambda g: get_close_matches(g, ACCOUNT_ROOT_CATEGORIES, cutoff=0)[0]
        )
        groups = groups.where(
            groups.isna(),
            "/" + first_nodes + groups.str.replace(r'^[^/]+', '', regex=True)
        ).astype("string[python]")

        return groups

    def sanitize_accounts(self, df: pd.DataFrame, tax_codes: pd.DataFrame = None) -> pd.DataFrame:
        df["group"] = self.sanitize_account_groups(df["group"])
        df = super().sanitize_accounts(df, tax_codes=tax_codes)
        return df

    def _account_balances(self, period: str) -> pd.DataFrame:
        """Generate a report of account balances for a chosen period.

        Args:
            period (datetime.date | str): Target period for the report.

        Returns:
            pd.DataFrame: With these columns:
                - amount (float): Account balance.
                - report_amount (float): Reporting balance.
                - account (str): Account name.
                - currency (str): Currency code.
        """
        prefixes = tuple(ACCOUNT_CATEGORIES_NEED_TO_NEGATE)

        def extract_nodes(items: list[dict], parent_path: str = "") -> list[dict]:
            """Flatten a hierarchical node structure into a list of leaf nodes with full paths."""
            nodes: list[dict] = []
            for node in items:
                path = f"{parent_path}/{node.get('text', '')}"
                if node.get("leaf"):
                    entry = dict(node)
                    entry["path"] = path
                    nodes.append(entry)
                elif isinstance(node.get("data"), list):
                    nodes.extend(extract_nodes(node["data"], parent_path=path))
                else:
                    raise ValueError(f"Unexpected data format at {path}.")
            return nodes

        def fetch_element(element_id: int, end) -> pd.DataFrame:
            resp = self._client.json_request(
                "GET", "report/element/data.json",
                params={"elementId": element_id, "startDate": datetime.date.min, "endDate": end},
            )
            return enforce_schema(pd.DataFrame(extract_nodes(resp["data"])), REPORT_ELEMENT)

        def adjust_sign(values: pd.Series, paths: pd.Series) -> pd.Series:
            return np.where(paths.str.startswith(prefixes), -values, values)

        def fetch_balances(end) -> pd.DataFrame:
            df = pd.concat([fetch_element(1, end), fetch_element(2, end)], ignore_index=True)
            df = df[df["accountId"].notna()].copy()
            df["account"] = df["accountId"].map(self._client.account_from_id)
            df["currency"] = df["currencyCode"].fillna(self.reporting_currency)
            df["amount"] = adjust_sign(df["endAmount2"], df["path"])
            df["report_amount"] = adjust_sign(df["dcEndAmount2"], df["path"])
            return df.loc[:, ["amount", "report_amount", "account", "currency"]]

        start, end = parse_date_span(period)
        balance = fetch_balances(end=end)
        if start is not None:
            start = start - datetime.timedelta(days=1)
            start_balance = fetch_balances(end=start)
            balance = balance.merge(
                start_balance, on="account", how="outer", suffixes=("", "_start")
            )
            balance["amount"] = balance["amount"].fillna(0) - \
                balance["amount_start"].fillna(0)
            balance["report_amount"] = balance["report_amount"].fillna(0) - \
                balance["report_amount_start"].fillna(0)
            balance.drop(columns=["amount_start", "report_amount_start"], inplace=True)
        return balance.reset_index(drop=True)

    def account_balances(
        self, df: pd.DataFrame, reporting_currency_only: bool = False
    ) -> pd.DataFrame:
        unique_periods = df["period"].unique()
        balance_lookup = {p: self._account_balances(period=p) for p in unique_periods}

        def _calc_balances(period, account):
            balance = balance_lookup[period]
            _, end = parse_date_span(period)
            multipliers = self.account_multipliers(self.account_range(account, mode="parts"))
            multipliers = pd.DataFrame(
                list(multipliers.items()), columns=["account", "multiplier"]
            )
            balance = balance.merge(multipliers, on="account", how="inner")
            balance["amount"] *= balance["multiplier"]
            balance["report_amount"] *= balance["multiplier"]
            report_balance = balance["report_amount"].sum()
            report_balance = self.round_to_precision([report_balance], ["reporting_currency"])[0]

            if reporting_currency_only:
                return {"report_balance": report_balance}

            balance = (
                balance.groupby("currency", sort=False, observed=True)["amount"]
                .sum()
                .reset_index()
            )
            balance["amount"] = self.round_to_precision(
                balance["amount"], balance["currency"], end
            )
            return {
                "report_balance": report_balance,
                "balance": dict(zip(balance["currency"], balance["amount"]))
            }

        results = [
            _calc_balances(period=row["period"], account=row["account"])
            for _, row in df.iterrows()
        ]
        return pd.DataFrame(results)

    # ----------------------------------------------------------------------
    # Journal

    def _journal_list(self, fiscal_period: str | None = None) -> pd.DataFrame:
        """Retrieves journal entries from the remote CashCtrl account.

        Args:
            fiscal_period (str | None, optional): Specifies which fiscal period to retrieve:
                - `None` (default): Returns entries for all fiscal periods.
                - `"current"`: Returns entries for the selected fiscal period.
                - Any other string: Returns entries for the given fiscal period (e.g., `"2025"`).

        Returns:
            pd.DataFrame: A DataFrame following the `LedgerEngine.JOURNAL_SCHEMA` column schema.

        Raises:
            ValueError: If the fiscal period does not exist or no current period is defined.
        """
        if fiscal_period is None:
            ids = self.fiscal_period_list()["id"]
        elif fiscal_period == "current":
            ids = [None]
        else:
            ids = [self._client.fiscal_period_to_id(fiscal_period)]
        journal_entries = [self._client.list_journal_entries(fiscal_period_id=id) for id in ids]
        return self._map_journal_entries(pd.concat(journal_entries, ignore_index=True))

    def _map_journal_entries(self, journal: pd.DataFrame) -> pd.DataFrame:
        """Convert CashCtrl journal entries to pyledger format.

        Args:
            journal (pd.DataFrame): Raw journal entries from CashCtrl.

        Returns:
            pd.DataFrame: Standardized journal DataFrame following the
                `LedgerEngine.JOURNAL_SCHEMA` column schema.
        """
        # Individual ledger entries represent a single transaction and
        # map to a single row in the resulting data frame.
        individual = journal[journal["type"] != "COLLECTIVE"]

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
        currency = np.where(
            is_fx_adjustment,
            np.where(
                individual["credit_currency"] != currency,
                individual["credit_currency"],
                individual["debit_currency"]
            ),
            currency
        )
        amount = np.where(is_fx_adjustment, 0, individual["amount"])
        reporting_amount = individual["amount"] * individual["currencyRate"]

        def resolve_profit_center(raw_id: int | str | None) -> str | None:
            """Resolve a profit center name from an integer or a list.
            If the input is malformed, ambiguous (e.g. multiple IDs), or resolution fails,
            a warning is logged and None is returned.
            """
            result = None
            if raw_id is None:
                return result
            try:
                if isinstance(raw_id, int):
                    result = self._client.profit_center_from_id(raw_id)
                else:
                    parsed = json.loads(raw_id)
                    if isinstance(parsed, list) and len(parsed) == 1:
                        result = self._client.profit_center_from_id(int(parsed[0]))
                    else:
                        self._logger.warning(
                            "Ledger entry can have only one assigned profit center, "
                            f"got: {raw_id}. Setting profit center to None for this ledger entry."
                        )
            except Exception as e:
                self._logger.warning(
                    f"Failed to resolve profit center {raw_id}: {e}. "
                    "Setting profit center to None for this ledger entry."
                )

            return result

        profit_centers = individual["costCenterIds"].apply(resolve_profit_center)
        result = pd.DataFrame(
            {
                "id": individual["id"],
                "date": individual["dateAdded"].dt.date,
                "account": individual["debit_account"],
                "contra": individual["credit_account"],
                "currency": currency,
                "amount": self.round_to_precision(amount, currency),
                "report_amount": self.round_to_precision(reporting_amount, reporting_currency),
                "tax_code": individual["taxName"],
                "profit_center": profit_centers,
                "description": individual["title"],
                "document": individual["reference"],
            }
        )

        # Collective journal entries represent a group of transactions and
        # map to multiple rows in the resulting data frame with the same id.
        collective_ids = journal.loc[journal["type"] == "COLLECTIVE", "id"]
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
            # Use reporting currency if the row was tagged with REPORTING_CURRENCY_TAG,
            # else use transaction-level currency, and fallback to account currency.
            # This recovers the original row-level currency intent lost during Cashctrl export.
            currency = pd.Series(np.where(
                collective["associateName"].fillna("") == REPORTING_CURRENCY_TAG,
                reporting_currency,
                collective["currency"]
            ), index=collective.index).fillna(collective["account_currency"])
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
            profit_centers = collective["allocations"].apply(
                lambda allocation: resolve_profit_center(allocation[0].get("toCostCenterId"))
                if isinstance(allocation, list) and allocation else None
            )
            mapped_collective = pd.DataFrame({
                "id": collective["id"],
                "date": collective["date"],
                "account": collective["account"],
                "currency": currency,
                "amount": self.round_to_precision(foreign_amount, currency),
                "report_amount": self.round_to_precision(reporting_amount, reporting_currency),
                "tax_code": collective["taxName"],
                "profit_center": profit_centers,
                "description": collective["description"],
                "document": collective["document"],
            })
            result = pd.concat([
                self.journal.standardize(result),
                self.journal.standardize(mapped_collective),
            ])

        return self.journal.standardize(result).reset_index(drop=True)

    def journal_entry(self):
        """Not implemented yet."""
        raise NotImplementedError

    def fiscal_period_list(self) -> pd.DataFrame:
        """Retrieve fiscal periods.

        Retrieve fiscal periods and check that each consecutive period
        starts exactly one day after the previous one ends.

        Returns:
            pd.DataFrame: A DataFrame of fiscal periods with FISCAL_PERIOD_SCHEMA.

        Raises:
            Exception: If any gap is detected between consecutive fiscal periods.
        """
        fiscal_periods = self._client.list_fiscal_periods()
        fp = enforce_schema(pd.DataFrame(fiscal_periods), FISCAL_PERIOD_SCHEMA)

        # Calculate the gap between consecutive periods (next start - current end)
        consecutive_gap = fp["start"].dt.date.shift(-1) - fp["end"].dt.date
        if any(consecutive_gap[:-1] != pd.Timedelta(days=1)):
            raise ValueError("Gaps between fiscal periods.")

        return fp

    def fiscal_period_add(self, start: datetime.date, end: datetime.date, name: str):
        """Add a new fiscal period.

        Args:
            start (datetime.date): Start date of the fiscal period.
            end (datetime.date): End date of the fiscal period.
            name (str): Name of the fiscal period.
        """
        data = {
            "isCustom": True,
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "salaryStart": start.strftime("%Y-%m-%d"),
            "salaryEnd": end.strftime("%Y-%m-%d"),
            "name": name
        }
        self._client.post("fiscalperiod/create.json", data=data)
        self._client.list_fiscal_periods.cache_clear()

    def ensure_fiscal_periods_exist(self, start: datetime.date, end: datetime.date):
        """
        Ensure fiscal periods exist for the given date range.
        If any part of the range is not covered by existing fiscal periods,
        create new periods to fill in the gaps.

        Args:
            start (datetime.date): Start of the date range to ensure coverage for.
            end (datetime.date): End of the date range to ensure coverage for.
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        fiscal_periods = self.fiscal_period_list()

        # Extend fiscal periods backward if needed
        while start < fiscal_periods["start"].min():
            earliest_start = fiscal_periods["start"].min()
            # The new fiscal period will be one year before the earliest start
            new_end = earliest_start - pd.Timedelta(days=1)
            new_start = earliest_start - pd.DateOffset(years=1)
            new_name = str(new_end.year)
            self.fiscal_period_add(start=new_start.date(), end=new_end.date(), name=new_name)
            fiscal_periods = self.fiscal_period_list()

        # Extend fiscal periods forward if needed
        while end > fiscal_periods["end"].max():
            latest_end = fiscal_periods["end"].max()
            # The new fiscal period will be one year after the latest end
            new_start = latest_end + pd.Timedelta(days=1)
            new_end = latest_end + pd.DateOffset(years=1)
            new_name = str(new_end.year)
            self.fiscal_period_add(start=new_start.date(), end=new_end.date(), name=new_name)
            fiscal_periods = self.fiscal_period_list()

    def _journal_add(self, data: pd.DataFrame) -> str:
        ids = []
        incoming = self.journal.standardize(data)
        self.ensure_fiscal_periods_exist(incoming["date"].min(), incoming["date"].max())
        for id in incoming["id"].unique():
            entry = incoming.query("id == @id")
            payload = self._map_journal_entry(entry)
            res = self._client.post("journal/create.json", data=payload)
            ids.append(str(res["insertId"]))
            self._client.list_journal_entries.cache_clear()
        return ids

    def _journal_modify(self, data: pd.DataFrame):
        incoming = self.journal.standardize(data)
        self.ensure_fiscal_periods_exist(incoming["date"].min(), incoming["date"].max())
        for id in incoming["id"].unique():
            entry = incoming.query("id == @id")
            payload = self._map_journal_entry(entry)
            payload["id"] = id
            self._client.post("journal/update.json", data=payload)
            self._client.list_journal_entries.cache_clear()

    def _journal_delete(self, id: pd.DataFrame, allow_missing=False):
        incoming = enforce_schema(pd.DataFrame(id), JOURNAL_SCHEMA.query("id"))
        self._client.post(
            "journal/delete.json", {"ids": ",".join([str(id) for id in incoming["id"]])}
        )
        self._client.list_journal_entries.cache_clear()

    def _journal_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the journal DataFrame to conform to CashCtrl format.

        Args:
            journal (pd.DataFrame): The journal DataFrame to be standardized.

        Returns:
            pd.DataFrame: The standardized journal DataFrame.
        """
        # Drop redundant report_amount for transactions in reporting currency
        set_na = (
            (df["currency"] == self.reporting_currency)
            & (df["report_amount"].isna() | (df["report_amount"] == df["amount"]))
        )
        df.loc[set_na, "report_amount"] = pd.NA

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
            (df["amount"] < 0) | (df["report_amount"] < 0) | df["account"].isna()
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

    def attach_journal_files(self, detach: bool = False):
        """Updates the attachments of all journal entries based on the file paths specified
        in the 'reference' field of each journal entry. If a file with the specified path
        exists in the remote CashCtrl account, it will be attached to the corresponding
        journal entry.

        Note: The 'reference' field in CashCtrl corresponds to the 'document' column in pyledger.

        Args:
            detach (bool, optional): If True, any files currently attached to journal entries that
                        do not have a valid reference path or whose reference path does not
                        match an actual file will be detached. Defaults to False.
        """
        # Map journal entries to their actual and targeted attachments
        attachments = self._get_journal_attachments()
        journal = self._client.list_journal_entries()
        journal["id"] = journal["id"].astype("string[python]")
        journal["reference"] = "/" + journal["reference"]
        files = self._client.list_files()
        df = pd.DataFrame(
            {
                "journal": journal["id"],
                "target_attachment": np.where(
                    journal["reference"].isin(files["path"]), journal["reference"], pd.NA
                ),
                "actual_attachments": [
                    attachments.get(id, []) for id in journal["id"]
                ],
            }
        )

        # Update attachments to align with the target attachments
        for id, target, actual in zip(
            df["journal"], df["target_attachment"], df["actual_attachments"]
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
        self._client.list_journal_entries.cache_clear()

    def _get_journal_attachments(self, allow_missing=True) -> Dict[str, List[str]]:
        """Retrieves paths of files attached to CashCtrl journal entries.

        Args:
            allow_missing (bool, optional): If True, return None if the file has no path,
                e.g. for files in the recycle bin. Otherwise raise a ValueError. Defaults to True.

        Returns:
            Dict[str, List[str]]: A Dict that contains journal ids with attached
            files as keys and a list of file paths as values.
        """
        journal = self._client.list_journal_entries()
        result = {}
        for id in journal.loc[journal["attachmentCount"] > 0, "id"]:
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

    def _collective_transaction_currency_and_rate(self, entry: pd.DataFrame) -> Tuple[str, float]:
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

        Returns:
            Tuple[str, float]: The single currency and the corresponding exchange rate.

        Raises:
            ValueError: If more than one non-reporting currency is present or if no
                        coherent exchange rate is found.
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
        precision = self.precision_vectorized([reporting_currency], [None])[0]
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
        else:
            fx_rate = round(preferred_rate, 8)

        return currency, fx_rate

    def _map_journal_entry(self, entry: pd.DataFrame) -> dict:
        """Converts a single journal entry to a data structure for upload to CashCtrl.

        Args:
            entry (pd.DataFrame): DataFrame with journal entry in pyledger schema.

        Returns:
            dict: A data structure to post as json to the CashCtrl REST API.
        """
        entry = self.journal.standardize(entry)
        reporting_currency = self.reporting_currency

        # Individual journal entry
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
            if pd.isna(entry["profit_center"].iat[0]):
                profit_center = None
            else:
                profit_center = self._client.profit_center_to_id(entry["profit_center"].iat[0])
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
                "allocations": None
                if profit_center is None
                else [{"share": 1.0, "toCostCenterId": profit_center}],
            }

        # Collective journal entry
        elif len(entry) > 1:
            # Individual transaction entries (line items)
            items = []
            currency, fx_rate = self._collective_transaction_currency_and_rate(entry)
            for _, row in entry.iterrows():
                if row["currency"] == currency:
                    amount = row["amount"]
                elif currency == reporting_currency:
                    amount = row["report_amount"]
                elif row["currency"] == reporting_currency:
                    amount = row["amount"] / fx_rate
                else:
                    raise ValueError(
                        "Currencies other than reporting or transaction currency are not "
                        "allowed in CashCtrl collective transactions."
                    )
                amount = self.round_to_precision(amount, currency)
                if pd.isna(row["profit_center"]):
                    profit_center = None
                else:
                    profit_center = self._client.profit_center_to_id(row["profit_center"])
                items.append(
                    {
                        "accountId": self._client.account_to_id(row["account"]),
                        "credit": -amount if amount < 0 else None,
                        "debit": amount if amount >= 0 else None,
                        "taxId": None
                        if pd.isna(row["tax_code"])
                        else self._client.tax_code_to_id(row["tax_code"]),
                        "description": row["description"],
                        # Use the associateId field (not its original purpose) to tag the row if
                        # the original currency is the reporting currency. This helps recover
                        # the original row-level currency intent lost during Cashctrl import.
                        "associateId": REPORTING_CURRENCY_TAG
                        if (row["currency"] != currency) and (row["currency"] == reporting_currency)
                        else None,
                        "allocations": None
                        if profit_center is None
                        else [{"share": 1.0, "toCostCenterId": profit_center}],
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
            raise ValueError("The journal entry contains no transaction.")
        return payload

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

        self._client.list_currencies.cache_clear()

    # ----------------------------------------------------------------------
    # Assets

    def _on_assets_change(self):
        self._ensure_currencies_exist()
        self.__class__._assets_as_df.fget.cache_clear()

    def _ensure_currencies_exist(self):
        """Ensure all local asset tickers definitions exist remotely"""
        local = set(self.assets.list()["ticker"].dropna())
        remote = set(self._client.list_currencies()["code"].dropna())
        to_add = local - remote

        for currency in to_add:
            if len(currency) != 3:
                raise ValueError(
                    "CashCtrl allows only 3-character currency codes."
                )
            self._client.post("currency/create.json", data={"code": currency})
            self._client.list_currencies.cache_clear()
