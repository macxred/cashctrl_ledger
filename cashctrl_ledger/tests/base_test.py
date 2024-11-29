import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger


@pytest.fixture(scope="module")
def initial_engine(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("temp")
    engine = ExtendedCashCtrlLedger(
        transitory_account=9999,
        price_history_path=tmp_path / "price_history.csv"
    )
    engine.dump_to_zip(tmp_path / "ledger.zip")
    # TODO: refactor logic for creating "JPY" currency within #89 issue
    currency_id = engine._client.post("currency/create.json", {"code": "JPY"})["insertId"]

    yield engine

    engine.restore_from_zip(tmp_path / "ledger.zip")
    engine.accounts.delete([{"account": 9999}])
    engine._client.post("currency/delete.json", {"ids": currency_id})
