"""Unit tests for testing dump, restore, and clear logic."""

import pandas as pd
import pytest
from pyledger import BaseTestDumpRestoreClear
# flake8: noqa: F401
from base_test import initial_ledger


class TestDumpRestoreClear(BaseTestDumpRestoreClear):
    @pytest.fixture()
    def ledger(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger
