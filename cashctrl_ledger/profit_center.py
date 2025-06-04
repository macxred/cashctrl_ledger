"""Provides a class with profit_center accessors and mutators for CashCtrl."""

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
        return self.standardize(result).reset_index(drop=True)

    def add(self, data: pd.DataFrame) -> None:
        incoming = self.standardize(pd.DataFrame(data))
        profit_centers = self._client.list_profit_centers()
        if profit_centers["number"].empty:
            start_number = 1
        else:
            start_number = profit_centers["number"].max() + 1
        for offset, (_, row) in enumerate(incoming.iterrows()):
            if row["profit_center"] in profit_centers["name"].values:
                raise ValueError(
                    f"Profit center already exists in the remote system: '{row['profit_center']}'."
                )
            payload = {
                "name": row["profit_center"],
                "number": start_number + offset,
            }
            self._client.post("account/costcenter/create.json", data=payload)
        self._client.list_profit_centers.cache_clear()

    def modify(self, data: pd.DataFrame) -> None:
        raise NotImplementedError(
            "Profit center modification is not supported."
        )

    def delete(self, id: pd.DataFrame, allow_missing: bool = False) -> None:
        incoming = enforce_schema(pd.DataFrame(id), self._schema.query("id"))
        ids = []
        for profit_center in incoming["profit_center"]:
            id = self._client.profit_center_to_id(profit_center, allow_missing=allow_missing)
            if id:
                ids.append(str(id))
        if len(ids):
            self._client.post("account/costcenter/delete.json", {"ids": ", ".join(ids)})
            self._client.list_profit_centers.cache_clear()
