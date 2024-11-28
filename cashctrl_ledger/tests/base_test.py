import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger


@pytest.fixture(scope="module")
def initial_engine(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("temp")
    ledger = ExtendedCashCtrlLedger(
        transitory_account=9999,
        price_history_path=tmp_path / "price_history.csv"
    )

    ledger.dump_to_zip(tmp_path / "ledger.zip")
    # Hack to create JPY currency
    currency_id = ledger._client.post("currency/create.json", {"code": "JPY"})["insertId"]

    yield ledger

    ledger.restore_from_zip(tmp_path / "ledger.zip")
    ledger.accounts.delete([{"account": 9999}])
    ledger._client.post("currency/delete.json", {"ids": currency_id})
