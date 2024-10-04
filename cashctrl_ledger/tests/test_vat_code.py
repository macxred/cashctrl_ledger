"""Unit tests for tax codes accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from pyledger.tests import BaseTestTaxCodes
# flake8: noqa: F401
from base_test import initial_ledger


class TestTaxCodes(BaseTestTaxCodes):
    @pytest.fixture(scope="class")
    def ledger(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger

    @pytest.mark.skip(reason="Cashctrl allows creating duplicate tax codes")
    def test_create_already_existed_raise_error(self):
        pass

    def test_update_non_existent_raise_error(self, ledger):
        super().test_update_non_existent_raise_error(ledger, error_message="No id found for tax code")

    def test_add_tax_with_not_valid_account_raise_error(self, ledger):
        ledger.delete_accounts([8888], allow_missing=True)
        assert 8888 not in ledger.accounts()["account"].values
        with pytest.raises(ValueError):
            ledger.add_tax_code(
                id="TestCode", description="tax 20%", account=8888, rate=0.02, is_inclusive=True
            )

    def test_update_tax_with_not_valid_account_raise_error(self, ledger):
        ledger.delete_accounts([8888], allow_missing=True)
        assert 8888 not in ledger.accounts()["account"].values
        with pytest.raises(ValueError):
            ledger.modify_tax_code(
                id="TestCode", description="tax 20%", account=8888, rate=0.02, is_inclusive=True
            )

    def test_delete_tax_non_existent(self, ledger):
        ledger.delete_tax_codes(["TestCode"], allow_missing=True)
        assert "TestCode" not in ledger.tax_codes()["id"].values

    def test_delete_non_existent_tax_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.delete_tax_codes(["TestCode"])
