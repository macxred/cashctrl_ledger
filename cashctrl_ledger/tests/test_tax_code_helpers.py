"""Unit tests for pure helpers in tax_code."""

import pandas as pd
import pytest
from cashctrl_ledger.tax_code import cashctrl_tax_code, extract_pyledger_id


@pytest.mark.parametrize("pyledger_id, expected", [
    ("IN_STD", "INSTD"),
    ("OUT_STD", "OUTSTD"),
    ("IN-RED", "INRED"),
    ("A.B.C", "ABC"),
    ("EXEMPT", "EXEMPT"),
    ("UN1", "UN1"),
    ("tax 2.6%", "TAX26"),
    ("", ""),
])
def test_cashctrl_tax_code(pyledger_id, expected):
    assert cashctrl_tax_code(pyledger_id) == expected


def test_extract_pyledger_id_with_prefix():
    assert extract_pyledger_id("[IN_STD]:Input VAT 8.1%", "INSTD") == "IN_STD"


def test_extract_pyledger_id_without_prefix_falls_back_to_code():
    assert extract_pyledger_id("Input VAT 8.1%", "INSTD") == "INSTD"


def test_extract_pyledger_id_with_null_description():
    assert extract_pyledger_id(None, "INSTD") == "INSTD"
    assert extract_pyledger_id(pd.NA, "INSTD") == "INSTD"


def test_extract_pyledger_id_preserves_non_alphanumeric_original():
    assert extract_pyledger_id("[OUT_RED]:foo", "OUTRED") == "OUT_RED"


def test_extract_pyledger_id_only_matches_leading_bracket():
    assert extract_pyledger_id("Input VAT [5%]:note", "INSTD") == "INSTD"
