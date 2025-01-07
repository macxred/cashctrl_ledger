"""Provides a class with profit center accessors and mutators for CashCtrl."""

import numpy as np
import pandas as pd
from consistent_df import enforce_schema
from .cashctrl_accounting_entity import CashCtrlAccountingEntity


class ProfitCenter(CashCtrlAccountingEntity):
    """Provides profit center accessors and mutators for CashCtrl."""

    def list(self) -> pd.DataFrame:
        profit_centers = self._client.list_profit_centers()
        result = pd.DataFrame({
            "profit_center": profit_centers["name"],
        })
        duplicates = set(result.loc[result["profit_center"].duplicated(), "profit_center"])
        if duplicates:
            raise ValueError(
                "Duplicated profit centers in the remote system: "
                f"'{', '.join(map(str, duplicates))}'"
            )
        return self.standardize(result)

    def add(self, data: pd.DataFrame) -> None:
        incoming = self.standardize(pd.DataFrame(data))
        profit_centers = self._client.list_profit_centers()
        max = np.nan_to_num(profit_centers["number"].max(), nan=0)
        incoming["number"] = pd.RangeIndex(start=max + 1, stop=max + 1 + len(incoming))
        for _, row in incoming.iterrows():
            payload = {
                "name": row["profit_center"],
                "number": row["number"],
            }
            self._client.post("account/costcenter/create.json", data=payload)
        self._client.invalidate_profit_centers_cache()

    def modify(self) -> None:
        raise NotImplementedError(
            "Profit centers cannot be modified as there are no fields available for modification."
        )

    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        ids = []
        for name in incoming["profit_center"]:
            id = self._client.profit_center_to_id(name, allow_missing=allow_missing)
            if id:
                ids.append(str(id))
        if len(ids):
            self._client.post("account/costcenter/delete.json", {"ids": ", ".join(ids)})
            self._client.invalidate_profit_centers_cache()
