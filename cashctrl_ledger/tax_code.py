"""Provides a class with tax_code accessors and mutators for CashCtrl."""

import re
import pandas as pd
import polars as pl
from pyledger.schema import enforce_schema, ensure_polars, to_pandas, to_polars
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


# Round-trip encoding: the adapter prefixes the original pyledger id into the
# CashCtrl description so ids with non-alphanumeric chars (e.g. IN_STD) survive
# CashCtrl's alphanumeric-only `code` constraint.
_DESCRIPTION_RE = re.compile(r"^\[([^\]]+)\]:(.*)", re.DOTALL)


def cashctrl_tax_code(pyledger_id: str) -> str:
    """Convert a pyledger tax code id into CashCtrl's stored alphanumeric form."""
    return re.sub(r"[^A-Za-z0-9]", "", str(pyledger_id)).upper()


def extract_pyledger_id(description, code: str) -> str:
    """Return the pyledger id from a `[id]:...` prefix, or fall back to `code`."""
    pyledger_id = code
    if description is not None and not pd.isna(description):
        match = _DESCRIPTION_RE.match(str(description))
        if match:
            pyledger_id = match.group(1)
    return pyledger_id


class TaxCode(CashCtrlAccountingEntity):
    """Provides tax code accessors and mutators for CashCtrl."""

    def list(self, pandas: bool = True) -> pd.DataFrame | pl.DataFrame:
        tax_rates = to_polars(self._client.list_tax_rates())
        accounts = to_polars(self._client.list_accounts())
        account_map = dict(accounts.select("id", "number").iter_rows())
        if not tax_rates["accountId"].is_in(list(account_map.keys())).all():
            raise ValueError("Unknown 'accountId' in CashCtrl tax rates.")
        result = pl.DataFrame({
            "id": tax_rates["name"],
            "description": tax_rates["documentName"],
            "account": tax_rates["accountId"].replace_strict(
                account_map, default=None
            ),
            "rate": tax_rates["percentage"] / 100,
            "is_inclusive": ~tax_rates["isGrossCalcType"],
        })

        duplicates = result.filter(result["id"].is_duplicated())["id"].unique().to_list()
        if duplicates:
            raise ValueError(
                f"Duplicated tax codes in the remote system: '{', '.join(map(str, duplicates))}'"
            )
        result = self.standardize(result, pandas=False)

        if pandas:
            return to_pandas(result, self._schema)
        return result

    def add(self, data: pd.DataFrame | pl.DataFrame) -> None:
        incoming = self.standardize(
            ensure_polars(data, "TaxCode.add"), pandas=False,
        )
        incoming = incoming.with_columns(
            is_inclusive=pl.col("is_inclusive").fill_null(False)
        )
        for row in incoming.iter_rows(named=True):
            self._client.account_to_id(row["account"])
            payload = {
                "name": row["id"],
                "percentage": row["rate"] * 100,
                "accountId": self._client.account_to_id(row["account"]),
                "documentName": row["description"],
                "calcType": "NET" if row["is_inclusive"] else "GROSS",
            }
            self._client.post("tax/create.json", data=payload)
        self._client.list_tax_rates.cache_clear()

    def modify(self, data: pd.DataFrame | pl.DataFrame) -> None:
        data = ensure_polars(data, "TaxCode.modify")
        schema_cols = self._schema["column"].to_list()
        id_cols = self._schema.filter(pl.col("id"))["column"].to_list()
        cols = list(set(schema_cols).intersection(data.columns).union(set(id_cols)))
        reduced_schema = self._schema.filter(pl.col("column").is_in(cols))
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        current = self.list(pandas=False)

        for row in incoming.iter_rows(named=True):
            # Specify required fields for CashCtrl
            existing = current.filter(pl.col("id") == row["id"])
            rate = row["rate"] if "rate" in incoming.columns else existing["rate"][0]
            account = row["account"] if "account" in incoming.columns else \
                existing["account"][0]
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
        self._client.list_tax_rates.cache_clear()

    def delete(self, id: pd.DataFrame | pl.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(
            ensure_polars(id, "TaxCode.delete"),
            self._schema.filter(pl.col("id")),
        )
        ids = []
        for code in incoming["id"].to_list():
            id = self._client.tax_code_to_id(code, allow_missing=allow_missing)
            if id:
                ids.append(str(id))
        if len(ids):
            self._client.post("tax/delete.json", {"ids": ", ".join(ids)})
            self._client.list_tax_rates.cache_clear()
