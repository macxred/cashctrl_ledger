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


def test_precision():
    cashctrl_ledger = CashCtrlLedger()

    precision = cashctrl_ledger.precision("USD")
    assert precision == 0.01, "Default precision should be 0.01"

    cashctrl_ledger.set_precision("USD", 0.0001)
    precision = cashctrl_ledger.precision("USD")
    assert cashctrl_ledger.precision("USD") == 0.0001, "Precision for USD should be 0.0001"
