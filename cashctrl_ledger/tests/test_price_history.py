"""Test suite for price history operations."""

import pytest
from pyledger.tests import BaseTestPriceHistory
from base_test import BaseTestCashCtrl


class TestPriceHistory(BaseTestCashCtrl, BaseTestPriceHistory):

    @pytest.fixture
    def engine(self, initial_engine):
        initial_engine.price_history.mirror(None, delete=True)
        return initial_engine

    def test_price_accessor_mutators(self, engine):
        super().test_price_accessor_mutators(engine, ignore_columns=["source"])

    def test_mirror_prices(self, engine):
        super().test_mirror_prices(engine, ignore_columns=["source"])
