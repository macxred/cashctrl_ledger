"""Definition of CashCtrl base class for testing."""

import pandas as pd
import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger
from pyledger.tests import BaseTest


class BaseTestCashCtrl(BaseTest):

    # Assign a default account to TAX_CODES where account is missing,
    # CashCtrl does not support tax codes without accounts assigned
    TAX_CODES = BaseTest.TAX_CODES.copy()
    default_account = TAX_CODES.query("id == 'IN_STD'")["account"].values[0]
    TAX_CODES.loc[TAX_CODES["account"].isna(), "account"] = default_account

    # TODO: Remove when profit centers entity integrates with CashCtrl
    JOURNAL = BaseTest.JOURNAL.copy()
    JOURNAL.loc[:, "profit_center"] = pd.NA
    columns = [col for col in JOURNAL.columns if col != "profit_center"] + ["profit_center"]
    JOURNAL = JOURNAL[columns]

    @pytest.fixture(scope="module")
    def initial_engine(self, tmp_path_factory):
        tmp_path = tmp_path_factory.mktemp("temp")
        engine = ExtendedCashCtrlLedger(
            transitory_account=9999,
            price_history_path=tmp_path / "price_history.csv",
            assets_path=tmp_path / "assets.csv",
            profit_centers_path=tmp_path / "profit_centers.csv",
        )
        engine.dump_to_zip(tmp_path / "ledger.zip")
        self.ACCOUNTS = engine.sanitize_accounts(self.ACCOUNTS)

        yield engine

        engine.restore_from_zip(tmp_path / "ledger.zip")
        engine.accounts.delete([{"account": 9999}])

    @pytest.fixture()
    def engine(self, initial_engine):
        return initial_engine
