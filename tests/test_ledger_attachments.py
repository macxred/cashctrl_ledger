"""
Unit tests for listing, attaching and detaching remote files.
"""

import pytest
import pandas as pd
from typing import List
from io import StringIO
from cashctrl_ledger import CashCtrlLedger

LEDGER_CSV = """
    id,   date, account, counter_account,  currency, amount,      vat_code, text,                             document
    1, 2024-05-24, 2100,            2200,       CHF,    100, Test_VAT_code, pytest single transaction 1,
    2, 2024-05-24, 2100,            2200,       CHF,    100, Test_VAT_code, pytest single transaction 1,      /file1.txt
    3, 2024-05-24, 2100,            2200,       CHF,    100, Test_VAT_code, pytest single transaction 1,      /subdir/file2.txt
    4, 2024-05-24, 2100,            2200,       CHF,    100, Test_VAT_code, pytest single transaction 1,      /file1.txt
"""

VAT_CSV = """
    id,             rate, account, inclusive, text
    Test_VAT_code,  0.02,   2200,      True, Input Tax 2%
"""

TEST_VAT_CODE = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)

@pytest.fixture(scope="module")
def tmp_path_for_module(tmp_path_factory):
    return tmp_path_factory.mktemp("temp")

@pytest.fixture(scope="module")
def mock_directory(tmp_path_for_module):
    """Create a temporary directory, populate with files and folders."""
    tmp_path = tmp_path_for_module
    (tmp_path / 'file1.txt').write_text("This is a text file.")
    subdir = tmp_path / 'subdir'
    subdir.mkdir(exist_ok=True)
    (subdir / 'file2.txt').write_text("A Text file in a subdirectory.")
    return tmp_path

@pytest.fixture(scope="module")
def files(mock_directory):
    """Create a CachedCashCtrlClient, populate with files and folders."""
    cc_client = CashCtrlLedger()
    initial_files = cc_client._client.list_files()
    cc_client._client.mirror_directory(mock_directory, delete_files=False)
    updated_files = cc_client._client.list_files()
    created_ids = set(updated_files['id']).difference(initial_files['id'])

    # return created files
    yield updated_files[updated_files['id'].isin(created_ids)]

    # Delete files added in the test
    params = { 'ids': ','.join(str(i) for i in created_ids), 'force': True }
    cc_client._client.post("file/delete.json", params=params)

@pytest.fixture(scope="session")
def ledger_ids():
    """Populate remote ledger with three new entries and return their ids in a list."""
    entry = pd.DataFrame({
        'date': ['2024-05-24'],
        'account': [2270],
        'counter_account': [2210],
        'amount': [100],
        'currency': ['CHF'],
        'text': ['test entry'],
    })
    engine = CashCtrlLedger()
    initial_ledger = engine.ledger()
    ledger_ids = [engine.add_ledger_entry(entry) for _ in range(3)]

    yield ledger_ids

    # Restore original ledger state
    engine.mirror_ledger(initial_ledger, delete=True)

@pytest.fixture(scope="session")
def ledger_attached_ids():
    cashctrl = CashCtrlLedger()

    # Fetch original state
    initial_ledger = cashctrl.ledger()
    initial_vat_codes = cashctrl.vat_codes().reset_index()

    # Create test VAT code and ledger
    cashctrl.mirror_vat_codes(TEST_VAT_CODE, delete=False)
    cashctrl.mirror_ledger(pd.DataFrame({}), delete=True)
    ledger_ids = [cashctrl.add_ledger_entry(LEDGER_ENTRIES.query(f'id == {i}')) for i in range(1, 5)]

    yield ledger_ids

    # Restore initial state
    cashctrl.mirror_ledger(initial_ledger, delete=True)
    cashctrl.mirror_vat_codes(initial_vat_codes, delete=True)

def sort_dict_values(items):
    return {key: value.sort() for key, value in items.items()}

def test_get_ledger_attachments(files, ledger_ids):
    engine = CashCtrlLedger()
    initial = engine._get_ledger_attachments()

    engine._client.post("journal/update_attachments.json", data={'id': ledger_ids[0], 'fileIds': files['id'].iat[0]})
    engine._client.invalidate_journal_cache()
    expected = initial | {ledger_ids[0]: ['/file1.txt']}
    assert engine._get_ledger_attachments() == expected

    engine._client.post("journal/update_attachments.json", data={'id': ledger_ids[1], 'fileIds': files['id'].iat[1]})
    engine._client.invalidate_journal_cache()
    expected = initial | {ledger_ids[0]: ['/file1.txt'], ledger_ids[1]: ['/subdir/file2.txt']}
    assert engine._get_ledger_attachments() == expected

    file_ids = f'{files['id'].iat[0]},{files['id'].iat[1]}'
    engine._client.post("journal/update_attachments.json", data={'id': ledger_ids[1], 'fileIds': file_ids})
    engine._client.invalidate_journal_cache()
    expected = initial | {ledger_ids[0]: ['/file1.txt'], ledger_ids[1]:  ['/file1.txt', '/subdir/file2.txt']}
    assert sort_dict_values(engine._get_ledger_attachments()) == sort_dict_values(expected)

def test_attach_ledger_files(files, ledger_attached_ids):
    engine = CashCtrlLedger()
    # Attach file that should be deleted
    engine._client.post("journal/update_attachments.json", data={'id': ledger_attached_ids[0], 'fileIds': files['id'].iat[0]})
    # Attach file that should be updated
    engine._client.post("journal/update_attachments.json", data={'id': ledger_attached_ids[1], 'fileIds': files['id'].iat[0]})
    # Attach file that should left untouched
    engine._client.post("journal/update_attachments.json", data={'id': ledger_attached_ids[2], 'fileIds': files['id'].iat[0]})

    # Update attachments with detach=False
    engine.attach_ledger_files(detach=False)
    attachments = engine._get_ledger_attachments()
    expected = {
        ledger_attached_ids[0]: ['/file1.txt'],
        ledger_attached_ids[1]: ['/subdir/file2.txt'],
        ledger_attached_ids[2]: ['/file1.txt'],
        ledger_attached_ids[3]: ['/file1.txt'],
    }
    assert sort_dict_values(expected) == sort_dict_values(attachments)

    # Update attachments with detach=True
    engine.attach_ledger_files(detach=True)
    attachments = engine._get_ledger_attachments()
    expected = {
        ledger_attached_ids[1]: ['/subdir/file2.txt'],
        ledger_attached_ids[2]: ['/file1.txt'],
        ledger_attached_ids[3]: ['/file1.txt'],
    }
    assert sort_dict_values(expected) == sort_dict_values(attachments)