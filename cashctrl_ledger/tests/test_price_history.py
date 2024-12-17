"""Test suite for price history operations."""

import pytest
from pyledger.tests import BaseTestPriceHistory
from base_test import BaseTestCashCtrl


class TestPriceHistory(BaseTestCashCtrl, BaseTestPriceHistory):

    @pytest.fixture
    def engine(self, initial_engine):
        initial_engine.price_history.mirror(None, delete=True)
        return initial_engine
