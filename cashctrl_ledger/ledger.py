"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

import datetime
import json
from typing import Dict, List, Tuple
import zipfile
from cashctrl_api import CashCtrlClient
import pandas as pd
import polars as pl
from pathlib import Path
from cashctrl_ledger.profit_center import ProfitCenter
from .tax_code import TaxCode, extract_pyledger_id, cashctrl_tax_code
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
from pyledger.schema import enforce_schema, ensure_polars, to_polars, to_pandas
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
            archive.writestr('tax_codes.csv', self.tax_codes.list().write_csv())
            archive.writestr('accounts.csv', self.accounts.list().write_csv())
            archive.writestr('price_history.csv', self.price_history.list().write_csv())
            archive.writestr('journal.csv', self.journal.list().write_csv())
            archive.writestr('assets.csv', self.assets.list().write_csv())
            archive.writestr('profit_centers.csv', self.profit_centers.list().write_csv())

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
            journal = pl.read_csv(archive.open('journal.csv').read())
            accounts = pl.read_csv(archive.open('accounts.csv').read())
            tax_codes = pl.read_csv(archive.open('tax_codes.csv').read())
            assets = pl.read_csv(archive.open('assets.csv').read())
            price_history = pl.read_csv(archive.open('price_history.csv').read())
            profit_centers = pl.read_csv(archive.open('profit_centers.csv').read())
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
        tax_codes: pd.DataFrame | pl.DataFrame | None = None,
        accounts: pd.DataFrame | pl.DataFrame | None = None,
        price_history: pd.DataFrame | pl.DataFrame | None = None,
        journal: pd.DataFrame | pl.DataFrame | None = None,
        assets: pd.DataFrame | pl.DataFrame | None = None,
        profit_centers: pd.DataFrame | pl.DataFrame | None = None,
    ):
        self.clear()
        if configuration is not None and "REPORTING_CURRENCY" in configuration:
            self.reporting_currency = configuration["REPORTING_CURRENCY"]
        if accounts is not None:
            accounts_pl = ensure_polars(accounts, "CashCtrlLedger.restore")
            if "group" in accounts_pl.columns:
                accounts_pl = accounts_pl.with_columns(
                    group=self.sanitize_account_groups(
                        accounts_pl["group"], pandas=False,
                    )
                )
            self.accounts.mirror(
                accounts_pl.with_columns(tax_code=pl.lit(None).cast(pl.String)),
                delete=True,
            )
        if tax_codes is not None:
            self.tax_codes.mirror(tax_codes, delete=True)
        if accounts is not None:
            self.accounts.mirror(accounts_pl, delete=True)
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
        accounts = self.accounts.list(pandas=False)
        self.accounts.mirror(
            accounts.with_columns(tax_code=pl.lit(None).cast(pl.String))
        )
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

    def sanitize_account_groups(
        self, groups: pd.Series | pl.Series,
        pandas: bool = False,
    ) -> pd.Series | pl.Series:
        """Ensure account groups start with a leading slash and a valid root node.

        Account categories in CashCtrl must be assigned to one of the pre-defined root nodes.
        Additional root categories are not allowed, and the pre-defined root categories
        cannot be deleted.

        Args:
            groups (pd.Series | pl.Series): A pandas or polars Series containing account group
                paths.
            pandas: If True and input is pandas, return pandas Series.

        Returns:
            pd.Series | pl.Series: Sanitized account group series.
        """
        original_index = None
        if isinstance(groups, pd.Series):
            original_index = groups.index
            groups = pl.Series(groups.name, groups.to_list(), dtype=pl.String)
        values = groups.to_list()
        result = []
        for g in values:
            if g is None:
                result.append(None)
                continue
            stripped = g.lstrip("/")
            first_node = stripped.split("/")[0] if "/" in stripped else stripped
            matched = get_close_matches(first_node, ACCOUNT_ROOT_CATEGORIES, cutoff=0)[0]
            rest = stripped[len(first_node):]
            result.append("/" + matched + rest)

        if pandas:
            return pd.Series(
                result, index=original_index, dtype="string[python]",
            )
        return pl.Series(groups.name, result, dtype=pl.String)

    def sanitize_accounts(
        self, df: pd.DataFrame | pl.DataFrame, tax_codes: pd.DataFrame | pl.DataFrame = None,
        pandas: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        df = ensure_polars(df, "CashCtrlLedger.sanitize_accounts")
        df = df.with_columns(
            group=self.sanitize_account_groups(df["group"], pandas=False)
        )
        return super().sanitize_accounts(df, tax_codes=tax_codes, pandas=pandas)

    def _account_balances(self, period: str) -> pl.DataFrame:
        """Generate a report of account balances for a chosen period.

        Args:
            period (datetime.date | str): Target period for the report.

        Returns:
            pl.DataFrame: With these columns:
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

        def fetch_element(element_id: int, end) -> pl.DataFrame:
            resp = self._client.json_request(
                "GET", "report/element/data.json",
                params={"elementId": element_id, "startDate": datetime.date.min, "endDate": end},
            )
            return enforce_schema(
                pl.DataFrame(extract_nodes(resp["data"])), REPORT_ELEMENT,
            )

        def fetch_balances(end) -> pl.DataFrame:
            df = pl.concat([fetch_element(1, end), fetch_element(2, end)], how="diagonal")
            df = df.filter(pl.col("accountId").is_not_null())
            account_map = {
                id_val: self._client.account_from_id(id_val)
                for id_val in df["accountId"].unique().to_list()
            }
            df = df.with_columns(
                account=pl.col("accountId").replace_strict(account_map, default=None),
                currency=pl.col("currencyCode").fill_null(self.reporting_currency),
            )
            # Adjust sign for liability/revenue accounts
            negate = pl.col("path").str.starts_with(prefixes[0])
            for prefix in prefixes[1:]:
                negate = negate | pl.col("path").str.starts_with(prefix)
            df = df.with_columns(
                amount=pl.when(negate).then(-pl.col("endAmount2")).otherwise(pl.col("endAmount2")),
                report_amount=pl.when(negate)
                .then(-pl.col("dcEndAmount2")).otherwise(pl.col("dcEndAmount2")),
            )
            result = df.select("amount", "report_amount", "account", "currency")
            return result.cast({
                "account": pl.Int64, "amount": pl.Float64,
                "report_amount": pl.Float64, "currency": pl.String,
            })

        start, end = parse_date_span(period)
        balance = fetch_balances(end=end)
        if start is not None:
            start = start - datetime.timedelta(days=1)
            start_balance = fetch_balances(end=start)
            balance = balance.join(
                start_balance, on="account", how="full", coalesce=True,
                suffix="_start", nulls_equal=True,
            )
            balance = balance.with_columns(
                amount=pl.col("amount").fill_null(0) - pl.col("amount_start").fill_null(0),
                report_amount=pl.col("report_amount").fill_null(0)
                - pl.col("report_amount_start").fill_null(0),
            ).drop("amount_start", "report_amount_start", "currency_start")

        return balance

    def account_balances(
        self, df: pd.DataFrame | pl.DataFrame, reporting_currency_only: bool = False,
        pandas: bool = False, **kwargs,
    ) -> pd.DataFrame | pl.DataFrame:
        df = ensure_polars(df, "CashCtrlLedger.account_balances")
        unique_periods = df["period"].unique(maintain_order=True).to_list()
        balance_lookup = {p: self._account_balances(period=p) for p in unique_periods}

        def _calc_balances(period, account):
            balance = balance_lookup[period]
            _, end = parse_date_span(period)
            multipliers = self.account_multipliers(self.account_range(account, mode="parts"))
            mult_df = pl.DataFrame({
                "account": list(multipliers.keys()),
                "multiplier": list(multipliers.values()),
            })
            balance = balance.join(
                mult_df, on="account", how="inner", nulls_equal=True,
            )
            balance = balance.with_columns(
                amount=pl.col("amount") * pl.col("multiplier"),
                report_amount=pl.col("report_amount") * pl.col("multiplier"),
            )
            report_balance = balance["report_amount"].sum()
            report_balance = self.round_to_precision(
                [report_balance], ["reporting_currency"]
            )[0]

            if reporting_currency_only:
                return {"report_balance": report_balance}

            grouped = balance.group_by("currency").agg(
                amount=pl.col("amount").sum()
            )
            amounts = self.round_to_precision(
                grouped["amount"], grouped["currency"], end
            )
            balance_list = [
                {"currency": c, "amount": a}
                for c, a in zip(grouped["currency"].to_list(), amounts)
            ]
            return {
                "report_balance": report_balance,
                "balance": balance_list if balance_list else None,
            }

        results = [
            _calc_balances(period=row["period"], account=row["account"])
            for row in df.iter_rows(named=True)
        ]

        balance_type = pl.List(pl.Struct({"currency": pl.String, "amount": pl.Float64}))
        if not results:
            cols = {"report_balance": pl.Float64}
            if not reporting_currency_only:
                cols["balance"] = balance_type
            result = pl.DataFrame(schema=cols)
        else:
            result = pl.DataFrame(results)
            if "balance" in result.columns:
                result = result.cast({"balance": balance_type})

        if pandas:
            rows = []
            for row_dict in result.iter_rows(named=True):
                out = {"report_balance": row_dict["report_balance"]}
                if not reporting_currency_only:
                    out["balance"] = self._balance_list_to_dict(
                        row_dict.get("balance")
                    )
                rows.append(out)
            return pd.DataFrame(rows)
        return result

    # ----------------------------------------------------------------------
    # Journal

    def _journal_list(
        self, fiscal_period: str | None = None, pandas: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Retrieves journal entries from the remote CashCtrl account.

        Args:
            fiscal_period (str | None, optional): Specifies which fiscal period to retrieve:
                - `None` (default): Returns entries for all fiscal periods.
                - `"current"`: Returns entries for the selected fiscal period.
                - Any other string: Returns entries for the given fiscal period (e.g., `"2025"`).
            pandas (bool): If True, return pandas DataFrame.

        Returns:
            pd.DataFrame | pl.DataFrame: A DataFrame following the
                `LedgerEngine.JOURNAL_SCHEMA` column schema.

        Raises:
            ValueError: If the fiscal period does not exist or no current period is defined.
        """
        if fiscal_period is None:
            ids = self.fiscal_period_list(pandas=False)["id"].to_list()
        elif fiscal_period == "current":
            ids = [None]
        else:
            ids = [self._client.fiscal_period_to_id(fiscal_period)]
        journal_entries = []
        for id in ids:
            df = self._client.list_journal_entries(fiscal_period_id=id)
            # Strip timezone before converting to polars to preserve local dates
            if "dateAdded" in df.columns and hasattr(df["dateAdded"].dt, "tz"):
                df["dateAdded"] = df["dateAdded"].dt.tz_localize(None)
            journal_entries.append(to_polars(df))
        result = self._map_journal_entries(pl.concat(journal_entries, how="diagonal"))

        if pandas:
            return to_pandas(result, self._journal._schema)
        return result

    def _map_journal_entries(self, journal: pl.DataFrame) -> pl.DataFrame:
        """Convert CashCtrl journal entries to pyledger format.

        Args:
            journal (pl.DataFrame): Raw journal entries from CashCtrl.

        Returns:
            pl.DataFrame: Standardized journal DataFrame following the
                `LedgerEngine.JOURNAL_SCHEMA` column schema.
        """
        # Reverse-map CashCtrl tax code strings back to pyledger ids. The adapter
        # embeds the original pyledger id as a `[id]:...` prefix in description.
        tax_rates = to_polars(self._client.list_tax_rates())
        tax_code_map = {
            tr["code"]: extract_pyledger_id(tr["description"], tr["code"])
            for tr in tax_rates.iter_rows(named=True)
        }

        # Individual ledger entries represent a single transaction and
        # map to a single row in the resulting data frame.
        individual = journal.filter(pl.col("type") != "COLLECTIVE")

        # Map to credit and debit account number and account currency
        accounts = to_polars(self._client.list_accounts())
        credit_map = accounts.select(
            creditId=pl.col("id"),
            credit_currency=pl.col("currencyCode"),
            credit_account=pl.col("number"),
        )
        individual = individual.join(
            credit_map, on="creditId", how="left", nulls_equal=True,
        )
        debit_map = accounts.select(
            debitId=pl.col("id"),
            debit_currency=pl.col("currencyCode"),
            debit_account=pl.col("number"),
        )
        individual = individual.join(
            debit_map, on="debitId", how="left", nulls_equal=True,
        )

        # Identify foreign currency adjustment transactions
        reporting_currency = self.reporting_currency
        is_fx_adjustment = (
            (pl.col("currencyCode") == reporting_currency)
            & (
                (pl.col("currencyCode") != pl.col("credit_currency"))
                | (pl.col("currencyCode") != pl.col("debit_currency"))
            )
        )
        individual = individual.with_columns(
            _is_fx=is_fx_adjustment,
            _currency=pl.when(is_fx_adjustment).then(
                pl.when(pl.col("credit_currency") != pl.col("currencyCode"))
                .then(pl.col("credit_currency"))
                .otherwise(pl.col("debit_currency"))
            ).otherwise(pl.col("currencyCode")),
            _amount=pl.when(is_fx_adjustment).then(0).otherwise(pl.col("amount")),
            _reporting_amount=pl.col("amount") * pl.col("currencyRate"),
        )

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

        profit_centers = [
            resolve_profit_center(v) for v in individual["costCenterIds"].to_list()
        ]
        currency_series = individual["_currency"]
        amount_series = individual["_amount"]
        reporting_amount_series = individual["_reporting_amount"]

        result = pl.DataFrame({
            "id": individual["id"],
            "date": individual["dateAdded"].cast(pl.Date),
            "account": individual["debit_account"],
            "contra": individual["credit_account"],
            "currency": currency_series,
            "amount": self.round_to_precision(amount_series, currency_series),
            "report_amount": self.round_to_precision(
                reporting_amount_series,
                pl.Series([reporting_currency] * len(reporting_amount_series)),
            ),
            "tax_code": individual["taxCode"].replace(tax_code_map),
            "profit_center": profit_centers,
            "description": individual["title"],
            "document": individual["reference"],
        })

        # Collective journal entries represent a group of transactions and
        # map to multiple rows in the resulting data frame with the same id.
        collective_ids = journal.filter(pl.col("type") == "COLLECTIVE")["id"].to_list()
        if len(collective_ids) > 0:

            # Fetch individual legs (line 'items') of collective transaction as flat rows
            rows = []
            for cid in collective_ids:
                res = self._client.get("journal/read.json", params={"id": cid})["data"]
                for item in res.get("items", []):
                    row = {
                        "id": res["id"],
                        "document": res["reference"],
                        "date": datetime.date.fromisoformat(res["dateAdded"][:10]),
                        "currency": res["currencyCode"],
                        "fx_rate": res["currencyRate"],
                    }
                    for col in JOURNAL_ITEM_COLUMNS:
                        row[col] = item.get(col)
                    row["allocations"] = item.get("allocations")
                    rows.append(row)
            if not rows:
                return self.journal.standardize(result, pandas=False)
            collective = to_polars(rows)

            # Map to account number and account currency
            account_map = accounts.select(
                accountId=pl.col("id"),
                account_currency=pl.col("currencyCode"),
                account=pl.col("number"),
            )
            collective = collective.join(
                account_map, on="accountId", how="left", nulls_equal=True,
            )

            # Identify reporting currency or foreign currency adjustment transactions
            is_fx_adjustment = (
                (pl.col("account_currency") != reporting_currency)
                & (pl.col("currency").is_null() | (pl.col("currency") == reporting_currency))
            )

            collective = collective.with_columns(
                _amount=pl.col("debit").fill_null(0) - pl.col("credit").fill_null(0),
            )

            # Use reporting currency if the row was tagged with REPORTING_CURRENCY_TAG,
            # else use transaction-level currency, and fallback to account currency.
            # This recovers the original row-level currency intent lost during Cashctrl export.
            collective = collective.with_columns(
                _currency=pl.when(
                    pl.col("associateName").fill_null("") == REPORTING_CURRENCY_TAG
                ).then(pl.lit(reporting_currency))
                .otherwise(pl.col("currency"))
                .fill_null(pl.col("account_currency")),
                _is_fx=is_fx_adjustment,
            )

            collective = collective.with_columns(
                _reporting_amount=pl.when(pl.col("_currency") == reporting_currency)
                .then(None)
                .otherwise(
                    pl.when(pl.col("_is_fx"))
                    .then(pl.col("_amount"))
                    .otherwise(pl.col("_amount") * pl.col("fx_rate"))
                ),
                _foreign_amount=pl.when(pl.col("_currency") == reporting_currency)
                .then(pl.col("_amount") * pl.col("fx_rate"))
                .otherwise(
                    pl.when(pl.col("_is_fx")).then(0).otherwise(pl.col("_amount"))
                ),
            )

            profit_centers_col = []
            for alloc in collective["allocations"].to_list():
                if isinstance(alloc, list) and alloc:
                    pc = resolve_profit_center(alloc[0].get("toCostCenterId"))
                else:
                    pc = None
                profit_centers_col.append(pc)

            currency_col = collective["_currency"]
            foreign_amount_col = collective["_foreign_amount"]
            reporting_amount_col = collective["_reporting_amount"]

            mapped_collective = pl.DataFrame({
                "id": collective["id"],
                "date": collective["date"],
                "account": collective["account"],
                "currency": currency_col,
                "amount": self.round_to_precision(foreign_amount_col, currency_col),
                "report_amount": self.round_to_precision(
                    reporting_amount_col,
                    pl.Series([reporting_currency] * len(reporting_amount_col)),
                ),
                "tax_code": collective["taxCode"].replace(tax_code_map),
                "profit_center": profit_centers_col,
                "description": collective["description"],
                "document": collective["document"],
            })
            result = pl.concat([
                self.journal.standardize(result, pandas=False),
                self.journal.standardize(mapped_collective, pandas=False),
            ], how="diagonal")

        return self.journal.standardize(result, pandas=False)

    def journal_entry(self):
        """Not implemented yet."""
        raise NotImplementedError

    def fiscal_period_list(self, pandas: bool = False) -> pd.DataFrame | pl.DataFrame:
        """Retrieve fiscal periods.

        Retrieve fiscal periods and check that each consecutive period
        starts exactly one day after the previous one ends.

        Returns:
            pd.DataFrame | pl.DataFrame: A DataFrame of fiscal periods with
                FISCAL_PERIOD_SCHEMA.

        Raises:
            Exception: If any gap is detected between consecutive fiscal periods.
        """
        fiscal_periods = self._client.list_fiscal_periods()
        fp = enforce_schema(pl.DataFrame(fiscal_periods), FISCAL_PERIOD_SCHEMA)

        # Calculate the gap between consecutive periods (next start - current end)
        if len(fp) > 1:
            gaps = (
                fp["start"].shift(-1).slice(0, len(fp) - 1)
                - fp["end"].slice(0, len(fp) - 1)
            )
            if (gaps != datetime.timedelta(days=1)).any():
                raise ValueError("Gaps between fiscal periods.")

        if pandas:
            return to_pandas(fp, FISCAL_PERIOD_SCHEMA)
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
        fiscal_periods = self.fiscal_period_list(pandas=False)

        def _to_date(dt):
            """Convert datetime to date if needed."""
            return dt.date() if isinstance(dt, datetime.datetime) else dt

        def _offset_year(dt, years):
            """Offset a date by a number of years, handling leap year edge cases."""
            try:
                return dt.replace(year=dt.year + years)
            except ValueError:
                # Feb 29 in a leap year -> Feb 28 in a non-leap year
                return dt.replace(year=dt.year + years, day=dt.day - 1)

        # Extend fiscal periods backward if needed
        while start < _to_date(fiscal_periods["start"].min()):
            earliest_start = _to_date(fiscal_periods["start"].min())
            # The new fiscal period will be one year before the earliest start
            new_end = earliest_start - datetime.timedelta(days=1)
            new_start = _offset_year(earliest_start, -1)
            new_name = str(new_end.year)
            self.fiscal_period_add(start=new_start, end=new_end, name=new_name)
            fiscal_periods = self.fiscal_period_list(pandas=False)

        # Extend fiscal periods forward if needed
        while end > _to_date(fiscal_periods["end"].max()):
            latest_end = _to_date(fiscal_periods["end"].max())
            # The new fiscal period will be one year after the latest end
            new_start = latest_end + datetime.timedelta(days=1)
            new_end = _offset_year(latest_end, 1)
            new_name = str(new_end.year)
            self.fiscal_period_add(start=new_start, end=new_end, name=new_name)
            fiscal_periods = self.fiscal_period_list(pandas=False)

    def _journal_add(self, data: pd.DataFrame | pl.DataFrame) -> str:
        ids = []
        incoming = self.journal.standardize(data, pandas=False)
        self.ensure_fiscal_periods_exist(incoming["date"].min(), incoming["date"].max())
        for entry in incoming.partition_by("id", maintain_order=True):
            payload = self._map_journal_entry(entry)
            res = self._client.post("journal/create.json", data=payload)
            ids.append(str(res["insertId"]))
            self._client.list_journal_entries.cache_clear()
        return ids

    def _journal_modify(self, data: pd.DataFrame | pl.DataFrame):
        incoming = self.journal.standardize(data, pandas=False)
        self.ensure_fiscal_periods_exist(incoming["date"].min(), incoming["date"].max())
        for id in incoming["id"].unique(maintain_order=True).to_list():
            entry = incoming.filter(pl.col("id") == id)
            payload = self._map_journal_entry(entry)
            payload["id"] = id
            self._client.post("journal/update.json", data=payload)
            self._client.list_journal_entries.cache_clear()

    def _journal_delete(self, id: pd.DataFrame | pl.DataFrame, allow_missing=False):
        incoming = enforce_schema(
            ensure_polars(id, "CashCtrlLedger._journal_delete"),
            self._journal._schema.filter(pl.col("id")),
        )
        self._client.post(
            "journal/delete.json",
            {"ids": ",".join([str(i) for i in incoming["id"].to_list()])}
        )
        self._client.list_journal_entries.cache_clear()

    def _journal_standardize(self, df: pd.DataFrame | pl.DataFrame) -> pl.DataFrame:
        """Standardizes the journal DataFrame to conform to CashCtrl format.

        Args:
            df (pd.DataFrame | pl.DataFrame): The journal DataFrame to be standardized.

        Returns:
            pl.DataFrame: The standardized journal DataFrame.
        """
        df = ensure_polars(df, "CashCtrlLedger._journal_standardize")

        # Normalize NaN to null on float columns
        df = df.with_columns(
            pl.col("amount").fill_nan(None),
            pl.col("report_amount").fill_nan(None),
        )

        # Drop redundant report_amount for transactions in reporting currency
        set_na = (
            (pl.col("currency") == self.reporting_currency)
            & (pl.col("report_amount").is_null()
               | (pl.col("report_amount") == pl.col("amount")))
        )
        df = df.with_columns(
            report_amount=pl.when(set_na).then(None).otherwise(pl.col("report_amount"))
        )

        # In CashCtrl, attachments are stored at the transaction level rather than
        # for each individual line item within collective transactions. To ensure
        # consistency between equivalent transactions, we fill any missing (NA)
        # document paths with non-missing paths from other line items in the same
        # transaction.
        df = df.with_columns(
            document=pl.col("document").forward_fill().over("id")
        )
        df = df.with_columns(
            document=pl.col("document").backward_fill().over("id")
        )

        # Split collective transaction line items with both debit and credit into
        # two items with a single account each
        is_collective = pl.col("id").is_duplicated()
        items_to_split = (
            is_collective & pl.col("account").is_not_null() & pl.col("contra").is_not_null()
        )
        split_mask = df.select(items_to_split).to_series()
        if split_mask.any():
            new = df.filter(split_mask)
            new = new.with_columns(
                account=pl.col("contra"),
                contra=pl.lit(None).cast(pl.Int64),
                amount=pl.when(
                    pl.col("amount").is_null() | (pl.col("amount") == 0)
                ).then(pl.col("amount")).otherwise(-1 * pl.col("amount")),
                report_amount=pl.when(
                    pl.col("report_amount").is_null() | (pl.col("report_amount") == 0)
                ).then(pl.col("report_amount")).otherwise(-1 * pl.col("report_amount")),
            )
            df = df.with_columns(
                contra=pl.when(items_to_split)
                .then(pl.lit(None).cast(pl.Int64))
                .otherwise(pl.col("contra"))
            )
            df = pl.concat([df, new], how="diagonal")

        # TODO: move this code block to parent class
        # Swap accounts if a contra but no account is provided,
        # or if individual transaction amount is negative
        swap_accounts = pl.col("contra").is_not_null() & (
            (pl.col("amount") < 0) | (pl.col("report_amount") < 0) | pl.col("account").is_null()
        )
        swap_mask = df.select(swap_accounts).to_series()
        if swap_mask.any():
            df = df.with_columns(
                _orig_account=pl.col("account"),
            )
            df = df.with_columns(
                account=pl.when(swap_accounts).then(pl.col("contra")).otherwise(pl.col("account")),
                contra=pl.when(swap_accounts)
                .then(pl.col("_orig_account")).otherwise(pl.col("contra")),
                amount=pl.when(swap_accounts).then(-1 * pl.col("amount"))
                .otherwise(pl.col("amount")),
                report_amount=pl.when(swap_accounts).then(-1 * pl.col("report_amount"))
                .otherwise(pl.col("report_amount")),
            ).drop("_orig_account")

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
        journal = to_polars(self._client.list_journal_entries())
        journal = journal.with_columns(
            id=pl.col("id").cast(pl.String),
            reference=pl.lit("/") + pl.col("reference"),
        )
        files = to_polars(self._client.list_files())
        file_paths = files["path"].to_list()

        # Update attachments to align with the target attachments
        for row in journal.iter_rows(named=True):
            id = row["id"]
            reference = row["reference"]
            actual = attachments.get(id, [])
            target = reference if reference in file_paths else None

            if target is None:
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
        journal = to_polars(self._client.list_journal_entries())
        result = {}
        has_attachments = journal.filter(pl.col("attachmentCount") > 0)
        for id in has_attachments["id"].to_list():
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
        self, entry: pd.DataFrame | pl.DataFrame,
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
            entry (pd.DataFrame | pl.DataFrame): The DataFrame representing individual entries of
                a collective transaction with columns 'currency', 'amount', and 'report_amount'.

        Returns:
            Tuple[str, float]: The single currency and the corresponding exchange rate.

        Raises:
            ValueError: If more than one non-reporting currency is present or if no
                        coherent exchange rate is found.
        """
        entry = ensure_polars(entry, "CashCtrlLedger._collective_transaction_currency_and_rate")
        if len(entry) == 0:
            raise ValueError("`entry` must be a DataFrame with at least one row.")
        if "id" in entry.columns:
            id = entry["id"][0]
        else:
            id = ""
        expected_columns = ["currency", "amount", "report_amount"]
        missing = [col for col in expected_columns if col not in entry.columns]
        if missing:
            raise ValueError(f"Missing required column(s) {missing}: {id}.")

        # Check if all entries are denominated in reporting currency
        reporting_currency = self.reporting_currency
        is_reporting_txn = (
            pl.col("currency").is_null()
            | (pl.col("currency") == reporting_currency)
            | (pl.col("amount") == 0)
        )
        entry = entry.with_columns(_is_reporting=is_reporting_txn)
        if entry["_is_reporting"].all():
            return reporting_currency, 1.0

        # Extract the sole non-reporting currency
        fx_entries = entry.filter(~pl.col("_is_reporting"))
        unique_currencies = fx_entries["currency"].unique().to_list()
        if len(unique_currencies) != 1:
            raise ValueError(
                "CashCtrl allows only the reporting currency plus a single foreign currency in "
                f"a collective booking: {id}."
            )
        currency = unique_currencies[0]

        # Define precision parameters for exchange rate calculation
        precision = self.precision_vectorized([reporting_currency], [None])[0]
        fx_rate_precision = 1e-8  # Precision for exchange rates in CashCtrl

        # Calculate the range of acceptable exchange rates
        reporting_amount = fx_entries["report_amount"]
        amount = fx_entries["amount"]
        tolerance = (amount * fx_rate_precision).abs().clip(precision / 2, float("inf"))
        sign = pl.select(
            pl.when(reporting_amount < 0).then(pl.lit(-1)).otherwise(pl.lit(1))
        ).to_series().cast(pl.Int8)
        lower_bound = reporting_amount - tolerance * sign
        upper_bound = reporting_amount + tolerance * sign
        min_fx_rate = (lower_bound / amount).max()
        max_fx_rate = (upper_bound / amount).min()

        # Select the exchange rate within the acceptable range closest to the preferred rate
        # derived from the largest absolute amount
        max_abs_amount = amount.abs().max()
        is_max_abs = amount.abs() == max_abs_amount
        fx_rates = reporting_amount / amount
        preferred_rate = fx_rates.filter(is_max_abs).median()
        if min_fx_rate <= max_fx_rate:
            fx_rate = min(max(preferred_rate, min_fx_rate), max_fx_rate)
        else:
            fx_rate = round(preferred_rate, 8)

        return currency, fx_rate

    def _map_journal_entry(self, entry: pd.DataFrame | pl.DataFrame) -> dict:
        """Converts a single journal entry to a data structure for upload to CashCtrl.

        Args:
            entry (pd.DataFrame | pl.DataFrame): DataFrame with journal entry in pyledger schema.

        Returns:
            dict: A data structure to post as json to the CashCtrl REST API.
        """
        entry = self.journal.standardize(entry, pandas=False)
        reporting_currency = self.reporting_currency

        # Individual journal entry
        if len(entry) == 1:
            row = entry.row(0, named=True)
            amount = row["amount"]
            reporting_amount = row["report_amount"]
            currency = row["currency"]
            if amount == 0 and reporting_amount is not None and reporting_amount != 0:
                # Foreign currency adjustment: Solely changes in reporting currency amount
                currency = reporting_currency
                amount = reporting_amount
                fx_rate = 1
            else:
                if currency == self.reporting_currency or amount == 0:
                    fx_rate = 1
                elif reporting_amount is None:
                    fx_rate = None
                else:
                    fx_rate = reporting_amount / amount
            profit_center = None if row["profit_center"] is None else \
                self._client.profit_center_to_id(row["profit_center"])
            payload = {
                "dateAdded": row["date"],
                "amount": amount,
                "debitId": self._client.account_to_id(row["account"]),
                "creditId": self._client.account_to_id(row["contra"]),
                "currencyId": None if currency is None
                else self._client.currency_to_id(currency),
                "title": row["description"],
                "taxId": None if row["tax_code"] is None
                else self._client.tax_code_to_id(cashctrl_tax_code(row["tax_code"])),
                "currencyRate": fx_rate,
                "reference": row["document"],
                "allocations": None if profit_center is None
                else [{"share": 1.0, "toCostCenterId": profit_center}],
            }

        # Collective journal entry
        elif len(entry) > 1:
            # Individual transaction entries (line items)
            items = []
            currency, fx_rate = self._collective_transaction_currency_and_rate(entry)
            for row in entry.iter_rows(named=True):
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
                profit_center = None if row["profit_center"] is None else \
                    self._client.profit_center_to_id(row["profit_center"])
                items.append(
                    {
                        "accountId": self._client.account_to_id(row["account"]),
                        "credit": -amount if amount < 0 else None,
                        "debit": amount if amount >= 0 else None,
                        "taxId": None if row["tax_code"] is None
                        else self._client.tax_code_to_id(cashctrl_tax_code(row["tax_code"])),
                        "description": row["description"],
                        # Use the associateId field (not its original purpose) to tag the row if
                        # the original currency is the reporting currency. This helps recover
                        # the original row-level currency intent lost during Cashctrl import.
                        "associateId": REPORTING_CURRENCY_TAG
                        if (row["currency"] != currency) and (row["currency"] == reporting_currency)
                        else None,
                        "allocations": None if profit_center is None
                        else [{"share": 1.0, "toCostCenterId": profit_center}],
                    }
                )

            # Transaction-level attributes
            date = entry["date"].drop_nulls().unique().to_list()
            document = entry["document"].drop_nulls().unique().to_list()
            if len(date) == 0:
                raise ValueError("Date is not specified in collective booking.")
            elif len(date) > 1:
                raise ValueError("Date needs to be unique in a collective booking.")
            if len(document) > 1:
                raise ValueError(
                    "CashCtrl allows only one reference in a collective booking."
                )
            payload = {
                "dateAdded": date[0].strftime("%Y-%m-%d")
                if isinstance(date[0], (datetime.date, datetime.datetime)) else str(date[0]),
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
        currencies = to_polars(self._client.list_currencies())
        is_reporting_currency = currencies["isDefault"].cast(pl.Boolean)
        reporting = currencies.filter(is_reporting_currency)
        if len(reporting) == 1:
            return reporting["code"][0]
        elif len(reporting) == 0:
            raise ValueError("No reporting currency set.")
        else:
            raise ValueError("Multiple reporting currencies defined.")

    @reporting_currency.setter
    def reporting_currency(self, currency):
        # TODO: Perform testing of this method after restore() for currencies implemented
        currencies = to_polars(self._client.list_currencies())
        matching = currencies.filter(pl.col("code") == currency)
        if len(matching) > 0:
            target_currency = matching.row(0, named=True)
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
        self.__class__._assets_lookup.fget.cache_clear()

    def _ensure_currencies_exist(self):
        """Ensure all local asset tickers definitions exist remotely"""
        local_assets = self.assets.list(pandas=False)
        local = set(local_assets["ticker"].drop_nulls().to_list())
        remote_currencies = to_polars(self._client.list_currencies())
        remote = set(remote_currencies["code"].drop_nulls().to_list())
        to_add = local - remote

        for currency in to_add:
            if len(currency) != 3:
                raise ValueError(
                    "CashCtrl allows only 3-character currency codes."
                )
            self._client.post("currency/create.json", data={"code": currency})
            self._client.list_currencies.cache_clear()
