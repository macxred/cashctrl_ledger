"""Test suite for profit center operations."""

import pytest
from cashctrl_ledger.tests.base_test import BaseTestCashCtrl
from pyledger.tests import BaseTestProfitCenters


class TestProfitCenters(BaseTestCashCtrl, BaseTestProfitCenters):

    @pytest.fixture
    def engine(self, initial_engine):
        return initial_engine

    def test_profit_center_accessor_mutators(self, engine):
        super().test_profit_center_accessor_mutators(engine, ignore_row_order=True)

    def test_add_existing_profit_center_raises_error(self, engine):
        super().test_add_existing_profit_center_raises_error(
            engine, error_message="Profit center already exists"
        )

    def test_modify_nonexistent_profit_center_raises_error(self, engine):
        super().test_modify_nonexistent_profit_center_raises_error(
            engine, error_class=NotImplementedError,
            error_message="Profit center modification is not supported.",
        )

    def test_delete_profit_center_allow_missing(self, engine):
        super().test_delete_profit_center_allow_missing(
            engine, error_message="No id found for profit center"
        )
