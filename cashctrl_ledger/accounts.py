"""Provides a class with account accessors and mutators for CashCtrl."""

from typing import Dict, List
import pandas as pd
import polars as pl
from pyledger.schema import enforce_schema, ensure_polars, to_polars
from .cashctrl_accounting_entity import CashCtrlAccountingEntity
from .tax_code import extract_pyledger_id, cashctrl_tax_code


class Account(CashCtrlAccountingEntity):
    """Provides account accessors and mutators for CashCtrl."""

    def list(self, pandas: bool = True) -> pd.DataFrame | pl.DataFrame:
        accounts = to_polars(self._client.list_accounts())
        result = pl.DataFrame({
            "account": accounts["number"],
            "currency": accounts["currencyCode"],
            "description": accounts["name"],
            "tax_code": accounts["taxCode"].map(tax_code_map).fillna(accounts["taxCode"]),
            "group": accounts["path"],
        })
        result = self.standardize(result, pandas=False)
        # Fill persistent and multiplier columns with values based on account number
        # (same logic as LedgerEngine.sanitize_accounts) since CashCtrl doesn't store them
        result = result.with_columns(
            persistent=(pl.col("account") < 3000),
            multiplier=pl.when(pl.col("account") < 2000)
            .then(pl.lit(1, dtype=pl.Int64)).otherwise(pl.lit(-1, dtype=pl.Int64)),
        )

        if pandas:
            from pyledger.schema import to_pandas
            return to_pandas(result, self._schema)
        return result

    def add(self, data: pd.DataFrame | pl.DataFrame) -> None:
        incoming = self.standardize(
            ensure_polars(data, "Account.add"), pandas=False,
        )

        # Update account categories
        self._client.update_categories(
            resource="account", target=self._account_groups(incoming),
            delete=False, ignore_account_root_nodes=True,
        )

        for row in incoming.iter_rows(named=True):
            payload = {
                "number": row["account"],
                "currencyId": self._client.currency_to_id(row["currency"]),
                "name": row["description"],
                "taxId": None if row["tax_code"] is None
                else self._client.tax_code_to_id(row["tax_code"]),
                "categoryId": self._client.account_category_to_id(
                    "/" + row["group"].strip("/")
                ),
            }
            self._client.post("account/create.json", data=payload)
        self._client.list_accounts.cache_clear()

    def modify(self, data: pd.DataFrame | pl.DataFrame) -> None:
        data = ensure_polars(data, "Account.modify")
        schema_cols = self._schema["column"].to_list()
        id_cols = self._schema.filter(pl.col("id"))["column"].to_list()
        cols = list(set(schema_cols).intersection(data.columns).union(set(id_cols)))
        reduced_schema = self._schema.filter(pl.col("column").is_in(cols))
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        current = self.list(pandas=False)

        # Update account categories
        if "group" in cols:
            self._client.update_categories(
                resource="account", target=self._account_groups(incoming),
                delete=False, ignore_account_root_nodes=True,
            )

        for row in incoming.iter_rows(named=True):
            existing = current.filter(pl.col("account") == row["account"])

            # Specify required fields for CashCtrl
            payload = {"id": self._client.account_to_id(row["account"])}
            group = row["group"] if "group" in incoming.columns else existing["group"][0]
            payload["categoryId"] = self._client.account_category_to_id(
                "/" + group.strip("/")
            )

            # Specify optional fields for CashCtrl
            if "account" in incoming.columns:
                payload["number"] = row["account"]
            if "currency" in incoming.columns:
                payload["currencyId"] = self._client.currency_to_id(row["currency"])
            if "description" in incoming.columns:
                payload["name"] = row["description"]
            if "tax_code" in incoming.columns:
                payload["taxId"] = None if row["tax_code"] is None else \
                    self._client.tax_code_to_id(row["tax_code"])
            self._client.post("account/update.json", data=payload)
        self._client.list_accounts.cache_clear()

    def delete(self, id: pd.DataFrame | pl.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(
            ensure_polars(id, "Account.delete"),
            self._schema.filter(pl.col("id")),
        )
        ids = []
        for account in incoming["account"].to_list():
            remote_id = self._client.account_to_id(account, allow_missing)
            if remote_id is not None:
                ids.append(str(remote_id))
        if len(ids):
            self._client.post("account/delete.json", {"ids": ", ".join(ids)})
            self._client.list_accounts.cache_clear()

    def mirror(self, target: pd.DataFrame | pl.DataFrame, delete: bool = False):
        """Synchronize remote CashCtrl accounts with the target DataFrame.

        Updates categories first, then invokes the parent class method.
        - Creates categories present in the target but not on the remote.
        - If `delete=True`, deletes remote categories not present in the target.

        CashCtrl has pre-defined root categories that cannot be altered.
        - Existing root categories are never erased, even if orphaned.
        - Mirroring accounts with non-existing root categories raises an error.

        Args:
            target: DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes remote accounts not present in the target.
        """
        current = self.list(pandas=False)
        target = self.standardize(
            ensure_polars(target, "Account.mirror"), pandas=False,
        )

        # Delete superfluous accounts on remote
        if delete:
            to_delete = current.filter(
                ~pl.col("account").is_in(target["account"].to_list())
            )
            self.delete(to_delete)

        # Update account categories
        self._client.update_categories(
            resource="account",
            target=self._account_groups(target),
            delete=delete,
            ignore_account_root_nodes=True,
        )
        return super().mirror(target, delete)

    def _get_nodes_list(self, path: str) -> List[str]:
        """Split a path into a list of node paths."""
        parts = path.strip("/").split("/")
        return ["/" + "/".join(parts[:i]) for i in range(1, len(parts) + 1)]

    def _account_groups(self, df: pl.DataFrame) -> Dict[str, str]:
        """Find lowest account number associated with each node in the group tree."""
        if df is None or len(df) == 0:
            return {}

        rows = []
        for row in df.iter_rows(named=True):
            for node in self._get_nodes_list(row["group"]):
                rows.append({"items": node, "account": row["account"]})

        expanded = pl.DataFrame(rows)
        return dict(
            expanded.group_by("items").agg(
                account=pl.col("account").min()
            ).iter_rows()
        )
