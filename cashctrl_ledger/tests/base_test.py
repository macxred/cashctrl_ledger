"""Definition of CashCtrl base class for testing."""

import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger
from pyledger.tests import BaseTest


class BaseTestCashCtrl(BaseTest):

    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    TAX_CODES = BaseTest.TAX_CODES.copy()
    default_account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = default_account

    # CashCtrl doesn't support filtering balances by profit centers, so we exclude those entries.
    # After the serialize_ledger fix for reporting currency accounts, CashCtrl and PyLedger
    # now return the same balance values, except for minor rounding differences.
    EXPECTED_BALANCES = BaseTest.EXPECTED_BALANCES.query("profit_center.isna()").copy()
    # Override: CashCtrl reports -0.01 due to rounding, PyLedger reports 0.0
    mask = ((EXPECTED_BALANCES["period"] == "2024-12-31")
            & (EXPECTED_BALANCES["account"] == "3000:9999"))
    EXPECTED_BALANCES.loc[mask, "report_balance"] = -0.01

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
            df=self.ACCOUNTS, tax_codes=self.TAX_CODES,
        )

        yield engine

        engine.restore_from_zip(tmp_path / "ledger.zip")
        engine.accounts.delete([{"account": 9999}])

    @pytest.fixture()
    def engine(self, initial_engine):
        return initial_engine
