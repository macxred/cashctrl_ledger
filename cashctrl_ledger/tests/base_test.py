import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger


@pytest.fixture(scope="module")
def initial_engine(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("temp")
    engine = ExtendedCashCtrlLedger(
        transitory_account=9999,
        price_history_path=tmp_path / "price_history.csv",
        assets_path=tmp_path / "assets.csv",
    )
    engine.dump_to_zip(tmp_path / "ledger.zip")

    yield engine

    engine.restore_from_zip(tmp_path / "ledger.zip")
    engine.accounts.delete([{"account": 9999}])
