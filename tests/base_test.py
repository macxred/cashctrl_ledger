import pytest
from cashctrl_ledger import ExtendedCashCtrlLedger


@pytest.fixture(scope="module")
def initial_ledger(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("temp")
    payload = {
        "account": 9999,
        "currency": "CHF",
        "text": "temp transitory account",
        "vat_code": None,
        "group": "/Assets",
    }
    ledger = ExtendedCashCtrlLedger(transitory_account=9999)
    # Create transitory account
    ledger.add_account(**payload)
    ledger.dump_to_zip(tmp_path / "ledger.zip")

    yield ledger

    ledger.restore_from_zip(tmp_path / "ledger.zip")
    # Delete transitory account
    ledger.delete_account(9999)
