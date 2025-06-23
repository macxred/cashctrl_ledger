"""Definition of CashCtrl base class for testing."""

from io import StringIO
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

    filtered_balances = BaseTest.EXPECTED_BALANCES.query("profit_center.isna()")
    end_year_entries = ["2024-12-31", "2024", "2024-Q4", "2025-01-02"]
    filtered_balances = filtered_balances.query("period not in @ end_year_entries")
    EXPECTED_BALANCES_CSV = """
        period,       account,            profit_center, report_balance,   balance
        2024-12-31,      4001,                         ,       -1198.26,   "{EUR: -1119.04}"
        2024-Q4,    1000:1999,                         ,    11654997.69,   "{USD: 299392.06, EUR: 10076638.88, JPY: 0.0, CHF: 14285714.3}"
        2024,       1000:1999,                         ,    12756171.60,   "{USD: 1075964.7, EUR: 10026667.1, JPY: 54345678.0, CHF: 14285714.3}"
        2024-08,    1000:1999,                         ,         -700.0,   "{USD: -700.0}"
        2024-12-31,      2970,                         ,           0.00,   "{USD:  0.00}"
        2024-12-31,      9200,                         ,   -12756871.60,   "{USD: -12756871.60}"
        2024-12-31,      2979,                         ,    12756871.60,   "{USD: 12756871.60}"
        2024-12-31,      1170,                         ,           0.00,   "{USD: 0.00}"
        2024-12-31,      1171,                         ,           0.00,   "{USD: 0.00}"
        2024-12-31,      1175,                         ,        -607.94,   "{USD: -607.94}"
        2024-12-31,      2200,                         ,           0.00,   "{USD: 0.00}"
        2025-01-02,      2979,                         ,          -0.00,   "{USD: 0.00}"
        2025-01-02,      2970,                         ,    12756871.60,   "{USD: 12756871.60}"
        2025-01-02,      9200,                         ,   -12756871.60,   "{USD: -12756871.60}"
    """
    EXPECTED_BALANCES = pd.read_csv(StringIO(EXPECTED_BALANCES_CSV), skipinitialspace=True)
    EXPECTED_BALANCES["profit_center"] = EXPECTED_BALANCES["profit_center"].apply(BaseTest.parse_profit_center)
    EXPECTED_BALANCES["balance"] = BaseTest.parse_balance_series(EXPECTED_BALANCES["balance"])
    EXPECTED_BALANCES = pd.concat([filtered_balances, EXPECTED_BALANCES])

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
