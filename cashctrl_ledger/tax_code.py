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

    @staticmethod
    def tax_payload(row: dict, id_map: dict, class_map: dict) -> dict:
        """Build the nested tax/create or tax/update payload from a pyledger row."""
        calc_type = "NET" if row["is_inclusive"] else "GROSS"

        # `LEGACY_EXP` / `LEGACY_REV` are the multi-rung auto-pick rules CashCtrl
        # used to apply pre-2.3 — the closest match to the 2.2 implicit behaviour
        # the adapter relied on.
        def component(account):
            is_asset = class_map[account] == "ASSET"
            rule = "LEGACY_EXP" if is_asset else "LEGACY_REV"
            return {"code": "", "accountId": id_map[account],
                    "calcType": calc_type, "applyRule": rule}

        components = [component(row["account"])]
        contra = row.get("contra")
        if contra is not None and not pd.isna(contra):
            components.append(component(contra))

        return {
            "code": cashctrl_tax_code(row["id"]),
            "description": f"[{row['id']}]:{row['description']}",
            "documentName": row["description"],
            "isDisplayTaxRate": True,
            "components": components,
            "rates": [{"percentage": row["rate"] * 100}],
        }

    def list(self, pandas: bool = False) -> pd.DataFrame | pl.DataFrame:
        tax_rates = to_polars(self._client.list_tax_rates())
        if tax_rates.is_empty():
            result = self.standardize(pl.DataFrame(), pandas=False)
            if pandas:
                return to_pandas(result, self._schema)
            return result
        accounts = to_polars(self._client.list_accounts())
        account_map = dict(accounts.select("id", "number").iter_rows())

        records = []
        for row in tax_rates.iter_rows(named=True):
            components = sorted(row["components"] or [], key=lambda c: c.get("pos", 0))
            if not components:
                raise ValueError(f"Tax code '{row['code']}' has no components.")

            match = _DESCRIPTION_RE.match(str(row["description"] or ""))
            if match:
                pyledger_id, description = match.group(1), match.group(2)
            else:
                pyledger_id, description = row["code"], row["description"] or ""

            percentage = row["currentPercentage"]
            records.append({
                "id": pyledger_id,
                "account": account_map.get(components[0]["accountId"]),
                "rate": (percentage if percentage is not None else 0) / 100,
                "is_inclusive": components[0]["calcType"] == "NET",
                "description": description,
                "contra": account_map.get(components[1]["accountId"])
                if len(components) > 1 else None,
            })

        result = pl.DataFrame(records)
        duplicates = result.filter(pl.col("id").is_duplicated())["id"].unique().to_list()
        if duplicates:
            raise ValueError(
                f"Duplicated tax codes in the remote system: '{', '.join(map(str, duplicates))}'."
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
        accounts = to_polars(self._client.list_accounts())
        id_map = dict(accounts.select("number", "id").iter_rows())
        class_map = dict(accounts.select("number", "accountClass").iter_rows())
        for row in incoming.iter_rows(named=True):
            self._client.post(
                "tax/create.json",
                data=self.tax_payload(row=row, id_map=id_map, class_map=class_map),
            )
        self._client.list_tax_rates.cache_clear()

    def modify(self, data: pd.DataFrame | pl.DataFrame) -> None:
        data = ensure_polars(data, "TaxCode.modify")
        schema_cols = self._schema["column"].to_list()
        id_cols = self._schema.filter(pl.col("id"))["column"].to_list()
        cols = list(set(schema_cols).intersection(data.columns).union(set(id_cols)))
        reduced_schema = self._schema.filter(pl.col("column").is_in(cols))
        incoming = enforce_schema(data, reduced_schema, keep_extra_columns=True)
        current = self.list(pandas=False)
        accounts = to_polars(self._client.list_accounts())
        id_map = dict(accounts.select("number", "id").iter_rows())
        class_map = dict(accounts.select("number", "accountClass").iter_rows())

        for row in incoming.iter_rows(named=True):
            cashctrl_id = self._client.tax_code_to_id(code=cashctrl_tax_code(row["id"]))
            merged = dict(current.filter(pl.col("id") == row["id"]).row(0, named=True))
            for col in incoming.columns:
                merged[col] = row[col]
            payload = self.tax_payload(row=merged, id_map=id_map, class_map=class_map)
            payload["id"] = cashctrl_id
            self._client.post("tax/update.json", data=payload)
        self._client.list_tax_rates.cache_clear()

    def delete(self, id: pd.DataFrame | pl.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(
            ensure_polars(id, "TaxCode.delete"),
            self._schema.filter(pl.col("id")),
        )
        ids = []
        for code in incoming["id"].to_list():
            cashctrl_id = self._client.tax_code_to_id(
                code=cashctrl_tax_code(code), allow_missing=allow_missing
            )
            if cashctrl_id:
                ids.append(str(cashctrl_id))
        if ids:
            self._client.post("tax/delete.json", {"ids": ", ".join(ids)})
            self._client.list_tax_rates.cache_clear()
