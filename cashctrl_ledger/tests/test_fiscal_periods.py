"""Unit tests for fiscal periods operations."""

from io import StringIO
from cashctrl_ledger import CashCtrlLedger
import pandas as pd
import pytest


@pytest.fixture()
def engine():
    cashctrl = CashCtrlLedger()
    # Retrieve initial fiscal periods ids
    fiscal_periods = cashctrl._client.get("fiscalperiod/list.json")['data']
    initial_ids = [fp["id"] for fp in fiscal_periods]

    yield cashctrl

    # Delete any created fiscal period
    fiscal_periods = cashctrl._client.get("fiscalperiod/list.json")['data']
    new_ids = [fp["id"] for fp in fiscal_periods]
    created_ids = set(new_ids) - set(initial_ids)
    if len(created_ids):
        ids = ",".join([str(id) for id in created_ids])
        breakpoint()
        cashctrl._client.post("fiscalperiod/delete.json", params={"ids": ids})

def test_fiscal_period_exist(engine):
    fp = engine.fiscal_period_list()

    engine.ensure_fiscal_periods_exist('2022-01-01', '2028-12-31')