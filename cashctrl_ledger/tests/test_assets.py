"""Test suite for assets operations."""

import pytest
from pyledger.tests import BaseTestAssets
from base_test import BaseTestCashCtrl


class TestAssets(BaseTestCashCtrl, BaseTestAssets):

    @pytest.fixture
    def engine(self, initial_engine):
        initial_engine.assets.mirror(None, delete=True)
        return initial_engine

    @pytest.fixture
    def restore_currencies(self, engine):
        # Retrieve initial currencies ids
        initial_ids = engine._client.list_currencies()["id"].to_list()

        yield

        # Delete any created currency
        new_ids = engine._client.list_currencies()["id"].to_list()
        created_ids = set(new_ids) - set(initial_ids)
        if len(created_ids):
            ids = ",".join([str(id) for id in created_ids])
            engine._client.post("currency/delete.json", params={"ids": ids})

    def test_asset_accessor_mutators(self, engine):
        super().test_asset_accessor_mutators(engine, ignore_columns=["source"])

    def test_mirror_assets(self, engine):
        super().test_mirror_assets(engine, ignore_columns=["source"])

    def test_ensure_currency_exist(self, engine, restore_currencies):
        new_asset = {"ticker": "AAA", "increment": 1, "date": "2023-01-01"}
        codes = engine._client.list_currencies()["code"].to_list()
        assert not new_asset["ticker"] in codes, "Currency with code 'AAA' already exists"

        engine.assets.add([new_asset])

        codes = engine._client.list_currencies()["code"].to_list()
        assert new_asset["ticker"] in codes, "Currency with code 'AAA' was not created"

    def test_ensure_currency_exist_raise_error_with_invalid_code(self, engine, restore_currencies):
        new_asset = {"ticker": "Invalid_ticker", "increment": 1, "date": "2023-01-01"}
        codes = engine._client.list_currencies()["code"].to_list()
        assert not new_asset["ticker"] in codes, (
            "Currency with code 'Invalid_ticker' already exists"
        )

        with pytest.raises(ValueError, match="CashCtrl allows only 3-character currency codes."):
            engine.assets.add([new_asset])
