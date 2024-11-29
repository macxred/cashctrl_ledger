"""Test suite for price history operations."""

import pytest
from pyledger.tests import BaseTestPriceHistory
# flake8: noqa: F401
from base_test import initial_engine


class TestPriceHistory(BaseTestPriceHistory):

    @pytest.fixture
    def engine(self, initial_engine):
        initial_engine.price_history.mirror(None, delete=True)
        return initial_engine
