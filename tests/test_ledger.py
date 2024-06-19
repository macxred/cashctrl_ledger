"""
Unit tests for ledger accessors, mutators and mirroring.
"""

from io import StringIO
from typing import List
import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger, df_to_consistent_str, nest
from pyledger import StandaloneLedger
from requests.exceptions import RequestException


LEDGER_CSV = """
    id,   date, account, counter_account, currency, amount,      vat_code, text,                             document
    1, 2024-05-24, 2270,            2210,      CHF,    100, Test_VAT_code, pytest single transaction 1,      /file1.txt
    2, 2024-05-24, 2210,                ,      USD,   -100, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2, 2024-05-24, 2270,                ,      USD,    100, Test_VAT_code, pytest collective txn 1 - line 2, /subdir/file2.txt
    3, 2024-04-24, 2210,                ,      EUR,   -200, Test_VAT_code, pytest collective txn 2 - line 1, /document-col-alt.pdf
    3, 2024-04-24, 2270,                ,      EUR,    200, Test_VAT_code, pytest collective txn 2 - line 2, /document-col-alt.pdf
    4, 2024-05-24, 2270,            2210,      CHF,    300, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
"""
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)


@pytest.fixture(scope="module")
def add_vat_code():
    # Creates VAT code
    cashctrl = CashCtrlLedger()
    initial_ledger = cashctrl.ledger().reset_index(drop=True)
    cashctrl.add_vat_code(
        code="Test_VAT_code",
        text='VAT 2%',
        account=2200,
        rate=0.02,
        inclusive=True,
    )

    yield

    # Restore initial state
    cashctrl.mirror_ledger(target=initial_ledger, delete=True)
    cashctrl.delete_vat_code(code="Test_VAT_code")

def txn_to_str(df: pd.DataFrame) -> List[str]:
    df = nest(df, columns=[col for col in df.columns if not col in ['id', 'date']], key='txn')
    df = df.drop(columns=['id'])
    result = [f'{str(date)},{df_to_consistent_str(txn)}' for date, txn in zip(df['date'], df['txn'])]
    return result.sort()

def test_ledger_accessor_mutators_single_transaction(add_vat_code):
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.iloc[[0]]
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update the ledger entry
    target = LEDGER_ENTRIES.iloc[[5]].copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # TODO: CashCtrl doesn`t allow to convert a single transaction into collective
    # transaction if the single transaction has a taxId assigned. See cashctrl#27.
    # As a workaround, we reset the taxId manually before update with a collective transaction
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)

    # Test replace with an collective ledger entry
    target = LEDGER_ENTRIES.iloc[[1, 2]].copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the created ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert id not in remote['id']

def test_ledger_accessor_mutators_single_transaction_without_VAT():
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry without VAT code
    target = LEDGER_ENTRIES.iloc[[5]].copy()
    target['vat_code'] = None
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update the ledger entry
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert id not in remote['id']

def test_ledger_accessor_mutators_collective_transaction(add_vat_code):
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.iloc[[1, 2]]
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update the ledger entry
    target = LEDGER_ENTRIES.iloc[[3, 4]].copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # Test replace with an individual ledger entry
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert id not in remote['id']

def test_ledger_accessor_mutators_collective_transaction_without_vat():
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.iloc[[1, 2]].copy()
    target['vat_code'] = None
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update the ledger entry
    target = LEDGER_ENTRIES.iloc[[3, 4]].copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)].reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert id not in remote['id']

def test_add_ledger_with_non_existing_vat():
    # Adding a ledger entry with non existing VAT code should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl.add_ledger_entry(target)

def test_add_ledger_with_non_existing_account():
    # Adding a ledger entry with non existing account should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['account'] = 33333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl.add_ledger_entry(target)

def test_add_ledger_with_non_existing_currency():
    # Adding a ledger entry with non existing currency code should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['currency'] = 'Non_Existent_Currency'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl.add_ledger_entry(target)

def test_update_ledger_with_illegal_attributes(add_vat_code):
    cashctrl = CashCtrlLedger()
    id = cashctrl.add_ledger_entry(LEDGER_ENTRIES.iloc[[0]])

    # Updating a ledger with non existent VAT code should raise an error
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['vat_code'] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl.update_ledger_entry(target)

    # Updating a ledger with non existent account code should raise an error
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['account'].iat[0] = 333333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl.update_ledger_entry(target)

    # Updating a ledger with non existent currency code should raise an error
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['currency'].iat[0] = 'Non_Existent_Currency'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl.update_ledger_entry(target)

    # Delete the ledger entry created above
    cashctrl.delete_ledger_entry(id)

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger():
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = 999999
    with pytest.raises(RequestException):
        cashctrl.update_ledger_entry(target)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl.delete_ledger_entry(ids='non-existent')

def test_mirror_ledger(add_vat_code):
    cashctrl = CashCtrlLedger()

    # Mirror with one single and one collective transaction
    target = LEDGER_ENTRIES.iloc[[0, 1, 2]]
    cashctrl.mirror_ledger(target=target, delete=True)
    expected = StandaloneLedger.standardize_ledger(target)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with duplicate transactions and delete=False
    target = pd.concat([
        LEDGER_ENTRIES.iloc[[0]],
        LEDGER_ENTRIES.iloc[[0]].assign(id=5),
        LEDGER_ENTRIES.iloc[[1, 2]].assign(id=6),
        LEDGER_ENTRIES.iloc[[1, 2]]
    ])
    cashctrl.mirror_ledger(target=target)
    expected = StandaloneLedger.standardize_ledger(target)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with alternative transactions and delete=False
    target = LEDGER_ENTRIES.iloc[[5, 3, 4]]
    cashctrl.mirror_ledger(target=target, delete=False)
    expected = pd.concat([mirrored, StandaloneLedger.standardize_ledger(target)])
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror existing transactions with delete=False has no impact
    target = LEDGER_ENTRIES.iloc[[0, 1, 2]]
    cashctrl.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with delete=True
    target = LEDGER_ENTRIES.iloc[[0, 1, 2]]
    cashctrl.mirror_ledger(target=target)
    mirrored = cashctrl.ledger()
    expected = StandaloneLedger.standardize_ledger(target)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror an empty target state
    cashctrl.mirror_ledger(target=pd.DataFrame({}), delete=True)
    assert cashctrl.ledger().empty
