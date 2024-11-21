"""Provides a class for storing Tax Code entity in CashCtrl."""

import pandas as pd
from consistent_df import enforce_schema
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


class TaxCode(CashCtrlAccountingEntity):
    """Class for storing Tax Code entity in CashCtrl"""

    def list(self) -> pd.DataFrame:
        tax_rates = self._client.list_tax_rates()
        accounts = self._client.list_accounts()
        account_map = accounts.set_index("id")["number"].to_dict()
        if not tax_rates["accountId"].isin(account_map).all():
            raise ValueError("Unknown 'accountId' in CashCtrl tax rates.")
        result = pd.DataFrame({
            "id": tax_rates["name"],
            "description": tax_rates["documentName"],
            "account": tax_rates["accountId"].map(account_map),
            "rate": tax_rates["percentage"] / 100,
            "is_inclusive": ~tax_rates["isGrossCalcType"],
        })

        duplicates = set(result.loc[result["id"].duplicated(), "id"])
        if duplicates:
            raise ValueError(
                f"Duplicated tax codes in the remote system: '{', '.join(map(str, duplicates))}'"
            )
        return self.standardize(result)

    def add(self, data: pd.DataFrame) -> None:
        incoming = self.standardize(pd.DataFrame(data))
        for _, row in incoming.iterrows():
            self._client.account_to_id(row["account"])
            payload = {
                "name": row["id"],
                "percentage": row["rate"] * 100,
                "accountId": self._client.account_to_id(row["account"]),
                "documentName": row["description"],
                "calcType": "NET" if row["is_inclusive"] else "GROSS",
            }
            self._client.post("tax/create.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def modify(self, data: pd.DataFrame) -> None:
        data = pd.DataFrame(data)
        cols = set(self._schema["column"]).intersection(data.columns)
        cols = cols.union(self._schema.query("id")["column"])
        reduced_schema = self._schema.query("column in @cols")
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        current = self.list()

        for _, row in incoming.iterrows():
            existing = current.query("id == @row['id']")
            rate = row["rate"] if "rate" in incoming.columns else existing["rate"].item()
            account = row["account"] if "account" in incoming.columns else \
                existing["account"].item()

            # Specify required fields for CashCtrl
            payload = {"id": self._client.tax_code_to_id(row["id"])}
            payload["name"] = row["id"]
            payload["percentage"] = rate * 100
            payload["accountId"] = self._client.account_to_id(account)

            # Specify optional fields for CashCtrl
            if "is_inclusive" in incoming.columns:
                payload["calcType"] = "NET" if row["is_inclusive"] else "GROSS"
            if "description" in incoming.columns:
                payload["documentName"] = row["description"]
            self._client.post("tax/update.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        ids = []
        for code in incoming["id"]:
            id = self._client.tax_code_to_id(code, allow_missing=allow_missing)
            if id:
                ids.append(str(id))
        if len(ids):
            self._client.post("tax/delete.json", {"ids": ", ".join(ids)})
            self._client.invalidate_tax_rates_cache()
