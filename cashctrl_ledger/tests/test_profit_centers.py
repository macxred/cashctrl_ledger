"""Unit tests for profit center accessor, mutator, and mirror methods."""

import pytest
from pyledger.tests import BaseTestProfitCenters
from base_test import BaseTestCashCtrl


class TestProfitCenters(BaseTestCashCtrl, BaseTestProfitCenters):

    @pytest.fixture()
    def engine(self, initial_engine):
        initial_engine.clear()
        return initial_engine

    def test_profit_center_accessor_mutators(self, engine):
        super().test_profit_center_accessor_mutators(engine, ignore_row_order=True)

    @pytest.mark.skip(reason="Cashctrl allows creating duplicate Profit centers")
    def test_add_existing_profit_center_raises_error(self):
        pass

    @pytest.mark.skip(reason="Profit centers cannot be modified in CashCtrl")
    def test_modify_nonexistent_profit_center_raises_error(self):
        pass

    def test_delete_profit_center_allow_missing(self, engine):
        super().test_delete_profit_center_allow_missing(
            engine, error_message="No id found for profit center Bank"
        )
