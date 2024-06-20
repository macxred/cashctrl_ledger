"""
Unit tests for CashCtrlLedger._collective_transaction_currency_and_rate().

Test helper function to map collective transactions in foreign currencies
from pyledger to CashCtrl format.
"""

import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger

def test_collective_entry_currency_and_rate_without_currency():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': [None, None, None],
        'amount': [100, -200, 100],
        'base_currency_amount': [100, -200, 100]
    })
    result = cashctrl._collective_transaction_currency_and_rate(df)
    assert result == ("CHF", 1.0)

def test_collective_entry_currency_and_rate_in_base_currency():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["CHF", "CHF", "CHF"],
        'amount': [-100, 200, -100],
        'base_currency_amount': [-100, 200, -100]
    })
    result = cashctrl._collective_transaction_currency_and_rate(df)
    assert result == ("CHF", 1.0)

def test_collective_entry_currency_and_rate_in_single_foreign_currency():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "EUR", "EUR"],
        'amount': [100, -200, 100],
        'base_currency_amount': [120, -240, 120]
    })
    result = cashctrl._collective_transaction_currency_and_rate(df)
    assert result == ("EUR", 1.2)

def test_collective_entry_currency_and_rate_in_base_and_foreign_currency():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "GBP", "EUR"],
        'amount': [100, -200, 100],
        'base_currency_amount': [120, -240, 120]
    })
    err_msg = "CashCtrl allows only the base currency plus a single foreign currency in a collective booking."
    with pytest.raises(ValueError, match=err_msg):
        cashctrl._collective_transaction_currency_and_rate(df)

def test_collective_entry_currency_and_rate_multiple_foreign_currencies():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "CHF", "EUR"],
        'amount': [150, -200, 50],
        'base_currency_amount': [138, -200, 46]
    })
    result = cashctrl._collective_transaction_currency_and_rate(df)
    assert result == ("EUR", 0.92)

def test_collective_entry_currency_and_rate_incoherent_exchange_rate():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "CHF", "EUR"],
        'amount': [150, -200, 50],
        'base_currency_amount': [138, -200, 47]
    })
    with pytest.raises(ValueError, match="Incoherent FX rates in collective booking."):
        cashctrl._collective_transaction_currency_and_rate(df)

def test_collective_entry_currency_and_rate_precise_rate_calculation():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "EUR", "CHF"],
        'amount': [100, 1, -101],
        'base_currency_amount': [91.44, 0.91, -101]
    })
    result = cashctrl._collective_transaction_currency_and_rate(df)
    assert result == ("EUR", 0.9144)

def test_collective_entry_currency_and_rate_incoherent_exchange_rate():
    cashctrl = CashCtrlLedger()
    df = pd.DataFrame({
        'currency': ["EUR", "EUR", "CHF"],
        'amount': [100, 1, -101],
        'base_currency_amount': [91.51, 0.91, -101]
    })
    with pytest.raises(ValueError, match="Incoherent FX rates in collective booking."):
        cashctrl._collective_transaction_currency_and_rate(df)
