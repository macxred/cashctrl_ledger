"""Test suite for assets operations."""

import pytest
from pyledger.tests import BaseTestAssets
# flake8: noqa: F401
from base_test import initial_engine


class TestAssets(BaseTestAssets):

    @pytest.fixture
    def engine(self, initial_engine):
        initial_engine.assets.mirror(None, delete=True)
        return initial_engine
