"""Unit tests for fiscal periods operations."""

import pandas as pd
from cashctrl_ledger import CashCtrlLedger
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
        cashctrl._client.post("fiscalperiod/delete.json", params={"ids": ids})

def test_fiscal_period_exist(engine):
    fiscal_periods = engine.fiscal_period_list()
    earliest_start = fiscal_periods["start"].min()
    latest_end = fiscal_periods["end"].max()

    # Extend 3 years in the past and future
    past_extension = earliest_start - pd.DateOffset(years=3)
    future_extension = latest_end + pd.DateOffset(years=3)
    engine.ensure_fiscal_periods_exist(start=past_extension.date(), end=future_extension.date())
    updated_fiscal_periods = engine.fiscal_period_list()

    assert len(updated_fiscal_periods) - len(fiscal_periods) == 6, (
        "Should create 6 new fiscal periods"
    )

    # Initialize the first expected start and end dates
    first_start = past_extension
    first_end = (pd.Timestamp(past_extension.year, 12, 31))

    # Loop over all fiscal periods and validate start and end dates
    for i, row in updated_fiscal_periods.iterrows():
        expected_start = first_start + pd.DateOffset(years=i)
        expected_end = first_end + pd.DateOffset(years=i)
        assert row["start"] == expected_start, (
            f"Fiscal period start date {row["start"]} does not match expected {expected_start}"
        )
        assert row["end"] == expected_end, (
            f"Fiscal period end date {row["end"]} does not match expected {expected_end}"
        )

def test_fiscal_period_list_raise_error_with_gap(engine):
    fiscal_periods = engine.fiscal_period_list()
    latest_end = fiscal_periods["end"].max()
    new_start = latest_end + pd.DateOffset(years=1)
    new_end = latest_end + pd.DateOffset(years=2)
    engine.fiscal_period_add(start=new_start, end=new_end, name="test_fiscal_period")
    with pytest.raises(ValueError, match="Gaps between fiscal periods."):
        engine.fiscal_period_list()