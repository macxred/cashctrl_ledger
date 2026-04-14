"""Provides a class with profit_center accessors and mutators for CashCtrl."""

import pandas as pd
import polars as pl
from pyledger.schema import enforce_schema, ensure_polars, to_pandas, to_polars
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


class ProfitCenter(CashCtrlAccountingEntity):
    """Provides profit center accessors and mutators for CashCtrl."""

    def list(self, pandas: bool = True) -> pd.DataFrame | pl.DataFrame:
        profit_centers = to_polars(self._client.list_profit_centers())
        result = pl.DataFrame({
            "profit_center": profit_centers["name"],
        })

        duplicates = result.filter(
            result["profit_center"].is_duplicated()
        )["profit_center"].unique().to_list()
        if duplicates:
            raise ValueError(
                "Duplicated profit centers in the remote system: "
                f"'{', '.join(map(str, duplicates))}'"
            )
        result = self.standardize(result, pandas=False)

        if pandas:
            return to_pandas(result, self._schema)
        return result

    def add(self, data: pd.DataFrame | pl.DataFrame) -> None:
        incoming = self.standardize(
            ensure_polars(data, "ProfitCenter.add"), pandas=False,
        )
        profit_centers = to_polars(self._client.list_profit_centers())
        if len(profit_centers) == 0 or profit_centers["number"].null_count() == len(profit_centers):
            start_number = 1
        else:
            start_number = profit_centers["number"].max() + 1

        existing_names = profit_centers["name"].to_list()
        for offset, row in enumerate(incoming.iter_rows(named=True)):
            if row["profit_center"] in existing_names:
                raise ValueError(
                    f"Profit center already exists in the remote system: "
                    f"'{row['profit_center']}'."
                )
            payload = {
                "name": row["profit_center"],
                "number": start_number + offset,
            }
            self._client.post("account/costcenter/create.json", data=payload)
        self._client.list_profit_centers.cache_clear()

    def modify(self, data: pd.DataFrame | pl.DataFrame) -> None:
        raise NotImplementedError(
            "Profit center modification is not supported."
        )

    def delete(self, id: pd.DataFrame | pl.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(
            ensure_polars(id, "ProfitCenter.delete"),
            self._schema.filter(pl.col("id")),
        )
        ids = []
        for profit_center in incoming["profit_center"].to_list():
            id = self._client.profit_center_to_id(
                profit_center, allow_missing=allow_missing
            )
            if id:
                ids.append(str(id))
        if len(ids):
            self._client.post("account/costcenter/delete.json", {"ids": ", ".join(ids)})
            self._client.list_profit_centers.cache_clear()
