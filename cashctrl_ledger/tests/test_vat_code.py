"""Unit tests for vat codes accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from pyledger.tests import BaseTestVatCode
# flake8: noqa: F401
from base_test import initial_ledger


class TestVatCodes(BaseTestVatCode):
    @pytest.fixture(scope="class")
    def ledger(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger

    @pytest.mark.skip(reason="Cashctrl allows creating duplicate VAT codes")
    def test_create_already_existed_raise_error(self):
        pass

    def test_update_non_existent_raise_error(self, ledger):
        super().test_update_non_existent_raise_error(ledger, error_message="No id found for tax code")

    def test_add_vat_with_not_valid_account_raise_error(self, ledger):
        ledger.delete_account(8888, allow_missing=True)
        assert 8888 not in ledger.account_chart()["account"].values
        with pytest.raises(ValueError):
            ledger.add_vat_code(
                code="TestCode", text="VAT 20%", account=8888, rate=0.02, inclusive=True
            )

    def test_update_vat_with_not_valid_account_raise_error(self, ledger):
        ledger.delete_account(8888, allow_missing=True)
        assert 8888 not in ledger.account_chart()["account"].values
        with pytest.raises(ValueError):
            ledger.modify_vat_code(
                code="TestCode", text="VAT 20%", account=8888, rate=0.02, inclusive=True
            )

    def test_delete_vat_non_existent(self, ledger):
        ledger.delete_vat_code("TestCode", allow_missing=True)
        assert "TestCode" not in ledger.vat_codes()["id"].values

    def test_delete_non_existent_vat_raise_error(self, ledger):
        with pytest.raises(ValueError):
            ledger.delete_vat_code("TestCode")
