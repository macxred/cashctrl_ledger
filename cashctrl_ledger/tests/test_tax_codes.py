"""Unit tests for vat codes accessor, mutator, and mirror methods."""

import pytest
from pyledger.tests import BaseTestTaxCodes
from base_test import BaseTestCashCtrl


class TestTaxCodes(BaseTestCashCtrl, BaseTestTaxCodes):

    @pytest.fixture(scope="class")
    def engine(self, initial_engine):
        initial_engine.clear()
        return initial_engine

    def test_tax_codes_accessor_mutators(self, engine):
        super().test_tax_codes_accessor_mutators(engine, ignore_row_order=True)

    @pytest.mark.skip(reason="Cashctrl allows creating duplicate VAT codes")
    def test_create_existing_tax_code_raise_error(self):
        pass

    def test_update_nonexistent_tax_code_raise_error(self, engine):
        super().test_update_nonexistent_tax_code_raise_error(
            engine, error_message="No id found for tax code"
        )

    def test_delete_tax_code_allow_missing(self, engine):
        super().test_delete_tax_code_allow_missing(
            engine, error_message="No id found for tax code"
        )

    def test_add_tax_code_with_not_valid_account_raise_error(self, engine):
        engine.accounts.delete([{"account": 8888}], allow_missing=True)
        assert 8888 not in engine.accounts.list()["account"].values
        with pytest.raises(ValueError):
            engine.accounts.add({
                "code": "TestCode", "text": "VAT 20%",
                "account": 8888, "rate": 0.02, "inclusive": True
            })

    def test_update_tax_code_with_not_valid_account_raise_error(self, engine):
        engine.accounts.delete([{"account": 8888}], allow_missing=True)
        assert 8888 not in engine.accounts.list()["account"].values
        with pytest.raises(ValueError):
            engine.tax_codes.modify({
                "code": "TestCode", "text": "VAT 20%",
                "account": 8888, "rate": 0.02, "inclusive": True
            })
