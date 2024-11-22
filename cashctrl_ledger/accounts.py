"""Provides a class with account accessors and mutators for CashCtrl."""

from typing import Dict, List
import pandas as pd
from consistent_df import enforce_schema, unnest
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


class Account(CashCtrlAccountingEntity):
    """Provides account accessors and mutators for CashCtrl."""

    def list(self) -> pd.DataFrame:
        accounts = self._client.list_accounts()
        result = pd.DataFrame({
            "account": accounts["number"],
            "currency": accounts["currencyCode"],
            "description": accounts["name"],
            "tax_code": accounts["taxName"],
            "group": accounts["path"],
        })
        return self.standardize(result)

    def add(self, data: pd.DataFrame):
        incoming = self.standardize(pd.DataFrame(data))
        for _, row in incoming.iterrows():
            payload = {
                "number": row["account"],
                "currencyId": self._client.currency_to_id(row["currency"]),
                "name": row["description"],
                "taxId": None if pd.isna(row["tax_code"])
                else self._client.tax_code_to_id(row["tax_code"]),
                "categoryId": self._client.account_category_to_id(row["group"]),
            }
            self._client.post("account/create.json", data=payload)
        self._client.invalidate_accounts_cache()

    def modify(self, data: pd.DataFrame) -> None:
        data = pd.DataFrame(data)
        cols = set(self._schema["column"]).intersection(data.columns)
        cols = cols.union(self._schema.query("id")["column"])
        reduced_schema = self._schema.query("column in @cols")
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        current = self.list()

        for _, row in incoming.iterrows():
            existing = current.query("account == @row['account']")

            # Specify required fields for CashCtrl
            payload = {"id": self._client.account_to_id(row["account"])}
            group = row["group"] if "group" in incoming.columns else existing["group"].item()
            payload["categoryId"] = self._client.account_category_to_id(group)

            # Specify optional fields for CashCtrl
            if "account" in incoming.columns:
                payload["number"] = row["account"]
            if "currency" in incoming.columns:
                payload["currencyId"] = self._client.currency_to_id(row["currency"])
            if "description" in incoming.columns:
                payload["name"] = row["description"]
            if "tax_code" in incoming.columns:
                payload["taxId"] = None if pd.isna(row["tax_code"]) else \
                    self._client.tax_code_to_id(row["tax_code"])
            self._client.post("account/update.json", data=payload)
        self._client.invalidate_accounts_cache()

    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        ids = []
        for account in incoming["account"]:
            id = self._client.account_to_id(account, allow_missing)
            if id is not None:
                ids.append(str(id))
        if len(ids):
            self._client.post("account/delete.json", {"ids": ", ".join(ids)})
            self._client.invalidate_accounts_cache()

    def mirror(self, target: pd.DataFrame, delete: bool = False):
        """Synchronize remote CashCtrl accounts with the target DataFrame.

        Updates categories first, then invokes the parent class method.
        - Creates categories present in the target but not on the remote.
        - If `delete=True`, deletes remote categories not present in the target.

        CashCtrl has pre-defined root categories that cannot be altered.
        - Existing root categories are never erased, even if orphaned.
        - Mirroring accounts with non-existing root categories raises an error.

        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes remote accounts not present in the target.
        """
        current = self.list()
        target = self.standardize(target)

        # Delete superfluous accounts on remote
        if delete:
            self.delete(current[~current["account"].isin(target["account"])])

        # Update account categories
        def get_nodes_list(path: str) -> List[str]:
            parts = path.strip("/").split("/")
            return ["/" + "/".join(parts[:i]) for i in range(1, len(parts) + 1)]

        def account_groups(df: pd.DataFrame) -> Dict[str, str]:
            if df is None or df.empty:
                return {}

            df = df.copy()
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
        super().mirror(target, delete)
