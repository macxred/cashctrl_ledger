"""Unit tests for currencies."""

from cashctrl_ledger import CashCtrlLedger
import pytest


CURRENCY = {"code": "AAA", "rate": 0.11111}


@pytest.fixture(scope="module")
def currency():
    cashctrl = CashCtrlLedger()
    res = cashctrl._client.post("currency/create.json", data=CURRENCY)

    yield

    cashctrl._client.post("currency/delete.json", data={"ids": res["insertId"]})


def test_price_for_base_currency():
    cashctrl_ledger = CashCtrlLedger()

    price = cashctrl_ledger.price(cashctrl_ledger.base_currency)
    assert price == 1, "Price for base currency should be 1."


@pytest.mark.skip(reason="Cashctrl doesn't support looking up exchange rates for custom currencies")
def test_price_for_currency(currency):
    cashctrl_ledger = CashCtrlLedger()

    price = cashctrl_ledger.price(currency=CURRENCY["code"])
    assert price == 0.11111, f"Price for {CURRENCY["code"]} should be {CURRENCY["rate"]}"
