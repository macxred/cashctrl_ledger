"""Unit tests for listing, attaching and detaching remote files."""

from io import StringIO
from cashctrl_ledger import CashCtrlLedger
import pandas as pd
import pytest

JOURNAL_CSV = """
    id,   date, account, contra,  currency, amount,  description,                  document
    1, 2024-05-24, 2100,   2200,       CHF,    100,  pytest single transaction 1,
    2, 2024-05-24, 2100,   2200,       CHF,    100,  pytest single transaction 1,  file1.txt
    3, 2024-05-24, 2100,   2200,       CHF,    100,  pytest single transaction 1,  subdir/file2.txt
    4, 2024-05-24, 2100,   2200,       CHF,    100,  pytest single transaction 1,  file1.txt
    5, 2024-05-24, 2100,   2200,       CHF,    100,  pytest single transaction 1,  file_invalid.txt
"""
JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)


@pytest.fixture(scope="module")
def tmp_path_for_module(tmp_path_factory):
    return tmp_path_factory.mktemp("temp")


@pytest.fixture(scope="module")
def mock_directory(tmp_path_for_module):
    """Create a temporary directory, populate with files and folders."""
    tmp_path = tmp_path_for_module
    (tmp_path / "file1.txt").write_text("This is a text file.")
    subdir = tmp_path / "subdir"
    subdir.mkdir(exist_ok=True)
    (subdir / "file2.txt").write_text("A Text file in a subdirectory.")
    return tmp_path


@pytest.fixture(scope="module")
def files(mock_directory):
    """Create a CachedCashCtrlClient, populate with files and folders."""
    cc_client = CashCtrlLedger()
    initial_files = cc_client._client.list_files()
    cc_client._client.mirror_directory(mock_directory, delete_files=False)
    updated_files = cc_client._client.list_files()
    created_ids = set(updated_files["id"]).difference(initial_files["id"])

    # return created files
    yield updated_files[updated_files["id"].isin(created_ids)]

    # Delete files added in the test
    params = {"ids": ",".join(str(i) for i in created_ids), "force": True}
    cc_client._client.post("file/delete.json", params=params)


@pytest.fixture(scope="module")
def journal_ids():
    """Populate remote journal with three new entries and return their ids in a list."""
    entry = pd.DataFrame({
        "date": ["2024-05-24"],
        "account": [2270],
        "contra": [2210],
        "amount": [100],
        "currency": ["CHF"],
        "description": ["test entry"],
    })
    engine = CashCtrlLedger()
    journal_ids = [engine.journal.add(entry)[0] for _ in range(3)]

    yield journal_ids

    # Restore original journal state
    engine.journal.delete({"id": journal_ids})


@pytest.fixture(scope="module")
def journal_attached_ids():
    """Populate remote journal with four new entries with specified document
    field and return their ids in a list.
    """
    cashctrl = CashCtrlLedger()
    journal_ids = [
        cashctrl.journal.add(JOURNAL.query(f"id == {id}"))[0]
        for id in JOURNAL["id"]
    ]

    yield journal_ids

    # Restore original journal state
    cashctrl.journal.delete({"id": journal_ids})


def sort_dict_values(items):
    return {key: value.sort() for key, value in items.items()}


def test_get_journal_attachments(files, journal_ids):
    engine = CashCtrlLedger()
    initial = engine._get_journal_attachments()

    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_ids[0], "fileIds": files["id"].iat[0]},
    )
    engine._client.invalidate_journal_cache()
    expected = initial | {journal_ids[0]: ["/file1.txt"]}
    assert engine._get_journal_attachments() == expected

    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_ids[1], "fileIds": files["id"].iat[1]},
    )
    engine._client.invalidate_journal_cache()
    expected = initial | {
        journal_ids[0]: ["/file1.txt"],
        journal_ids[1]: ["/subdir/file2.txt"],
    }
    assert engine._get_journal_attachments() == expected

    file_ids = f'{files["id"].iat[0]},{files["id"].iat[1]}'
    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_ids[1], "fileIds": file_ids},
    )
    engine._client.invalidate_journal_cache()
    expected = initial | {
        journal_ids[0]: ["/file1.txt"],
        journal_ids[1]: ["/file1.txt", "/subdir/file2.txt"],
    }
    assert sort_dict_values(engine._get_journal_attachments()) == sort_dict_values(expected)


def test_attach_journal_files(files, journal_attached_ids):
    engine = CashCtrlLedger()
    # Attach file that should be deleted
    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_attached_ids[0], "fileIds": files["id"].iat[0]},
    )
    # Attach file that should be updated
    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_attached_ids[1], "fileIds": files["id"].iat[0]},
    )
    # Attach file that should left untouched
    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_attached_ids[2], "fileIds": files["id"].iat[0]},
    )
    engine._client.invalidate_journal_cache()

    # Update attachments with detach=False
    engine.attach_journal_files(detach=False)
    attachments = engine._get_journal_attachments()
    attachments = {k: v for k, v in attachments.items() if k in journal_attached_ids}
    expected = {
        journal_attached_ids[0]: ["/file1.txt"],
        journal_attached_ids[1]: ["/subdir/file2.txt"],
        journal_attached_ids[2]: ["/file1.txt"],
        journal_attached_ids[3]: ["/file1.txt"],
    }
    assert sort_dict_values(expected) == sort_dict_values(attachments)

    # Update attachments with detach=True
    engine.attach_journal_files(detach=True)
    attachments = engine._get_journal_attachments()
    attachments = {k: v for k, v in attachments.items() if k in journal_attached_ids}
    expected = {
        journal_attached_ids[1]: ["/subdir/file2.txt"],
        journal_attached_ids[2]: ["/file1.txt"],
        journal_attached_ids[3]: ["/file1.txt"],
    }
    assert sort_dict_values(expected) == sort_dict_values(attachments)


def test_attach_ledger_files_that_dont_match_remote_files(files, journal_attached_ids):
    engine = CashCtrlLedger()
    # Attach file that should trigger update of non-existent file
    engine._client.post(
        "journal/update_attachments.json",
        data={"id": journal_attached_ids[4], "fileIds": files["id"].iat[0]},
    )
    engine._client.invalidate_journal_cache()

    # With detach=false attached file should left the same
    engine.attach_journal_files(detach=False)
    attachments = engine._get_journal_attachments()
    attachments = {k: v for k, v in attachments.items() if k == journal_attached_ids[4]}
    expected = {journal_attached_ids[4]: ["/file1.txt"]}
    assert expected == attachments

    # With detach=true attached file should be deleted
    engine.attach_journal_files(detach=True)
    attachments = engine._get_journal_attachments()
    attachments = {k: v for k, v in attachments.items() if k == journal_attached_ids[4]}
    assert {} == attachments


def test_attach_journal_files_to_journal_with_multiple_attachments(files, journal_attached_ids):
    engine = CashCtrlLedger()
    # Attach files that should trigger update
    engine._client.post(
        "journal/update_attachments.json",
        data={
            "id": journal_attached_ids[1],
            "fileIds": f"{files['id'].iat[0]}, {files['id'].iat[1]}",
        },
    )
    engine._client.invalidate_journal_cache()

    # Should update attachments from multiple to only one specified
    engine.attach_journal_files(detach=False)
    attachments = engine._get_journal_attachments()
    attachments = {k: v for k, v in attachments.items() if k == journal_attached_ids[1]}
    expected = {journal_attached_ids[1]: ["/file1.txt"]}
    assert expected == attachments
