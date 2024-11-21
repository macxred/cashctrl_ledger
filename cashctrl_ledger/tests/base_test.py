import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger


@pytest.fixture(scope="module")
def initial_engine(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("temp")
    ledger = ExtendedCashCtrlLedger(transitory_account=9999)
    ledger.dump_to_zip(tmp_path / "ledger.zip")

    yield ledger

    ledger.restore_from_zip(tmp_path / "ledger.zip")
