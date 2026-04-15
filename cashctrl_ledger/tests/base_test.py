"""Definition of CashCtrl base class for testing."""

import polars as pl
import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger
from pyledger.tests import BaseTest


class BaseTestCashCtrl(BaseTest):

    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    default_account = BaseTest.TAX_CODES.filter(pl.col("id") == "IN_STD")["account"][0]
    TAX_CODES = BaseTest.TAX_CODES.with_columns(
        account=pl.col("account").fill_null(default_account)
    )

    # CashCtrl doesn't support filtering balances by profit centers, so we exclude those entries.
    # After the serialize_ledger fix for reporting currency accounts, CashCtrl and PyLedger
    # now return the same balance values, except for minor rounding differences.
    EXPECTED_BALANCES = BaseTest.EXPECTED_BALANCES.filter(pl.col("profit_center").is_null())
    # Override: CashCtrl reports -0.01 due to rounding, PyLedger reports 0.0
    EXPECTED_BALANCES = EXPECTED_BALANCES.with_columns(
        report_balance=pl.when(
            (pl.col("period") == "2024-12-31") & (pl.col("account") == "3000:9999")
        ).then(-0.01).otherwise(pl.col("report_balance"))
    )

    @pytest.fixture(scope="module")
    def initial_engine(self, tmp_path_factory):
        tmp_path = tmp_path_factory.mktemp("temp")
        engine = ExtendedCashCtrlLedger(
            transitory_account=9999,
            root=tmp_path,
            price_history_path="settings/price_history.csv",
            assets_path="settings/assets.csv",
        )
        engine.dump_to_zip(tmp_path / "ledger.zip")
        BaseTestCashCtrl.ACCOUNTS = engine.sanitize_accounts(
            df=self.ACCOUNTS, tax_codes=self.TAX_CODES, pandas=False,
        )

        yield engine

        engine.restore_from_zip(tmp_path / "ledger.zip")
        engine.accounts.delete([{"account": 9999}])

    @pytest.fixture()
    def engine(self, initial_engine):
        return initial_engine
