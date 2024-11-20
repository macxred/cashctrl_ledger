"""Unit tests for vat codes accessor, mutator, and mirror methods."""

import pytest
import pandas as pd
from pyledger.tests import BaseTestTaxCodes
# flake8: noqa: F401
from base_test import initial_ledger


class TestTaxCodes(BaseTestTaxCodes):
    ACCOUNTS = BaseTestTaxCodes.ACCOUNTS.copy()
    # In CashCtrl it is not possible to create root nodes, use default group
    ACCOUNTS["group"] = "/Assets"
    # TODO: Remove when Assets will be implemented
    ACCOUNTS.loc[ACCOUNTS["currency"] == "JPY", "currency"] = "USD"

    TAX_CODES = BaseTestTaxCodes.TAX_CODES.copy()
    # In CashCtrl it is not possible to create TAX CODE without specified account
    TAX_CODES = TAX_CODES[~(TAX_CODES["id"] == "EXEMPT")]


    @pytest.fixture(scope="class")
    def engine(self, initial_ledger):
        initial_ledger.clear()
        return initial_ledger

    def test_tax_codes_accessor_mutators(self, engine):
        super().test_tax_codes_accessor_mutators(engine, ignore_row_order=True)

    @pytest.mark.skip(reason="Cashctrl allows creating duplicate VAT codes")
    def test_create_existing__tax_code_raise_error(self):
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
                "account": 8888, "rate": 0.02, "inclusive":True
            })

    def test_update_tax_code_with_not_valid_account_raise_error(self, engine):
        engine.accounts.delete([{"account": 8888}], allow_missing=True)
        assert 8888 not in engine.accounts.list()["account"].values
        with pytest.raises(ValueError):
            engine.tax_codes.modify({
                "code": "TestCode", "text": "VAT 20%",
                "account": 8888, "rate": 0.02, "inclusive":True
            })
