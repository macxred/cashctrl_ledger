from io import StringIO
from typing import List
import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger, df_to_consistent_str, nest
from pyledger import StandaloneLedger
from requests.exceptions import RequestException

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

@pytest.fixture
def ledger_entries():
    LEDGER_CSV = """
        id, date,    account, counter_account, currency, amount,      vat_code, text,                             document
         1, 2024-05-24, 2270,            2210,      CHF,    100, Test_VAT_code, pytest single transaction 1,      /file1.txt
         2, 2024-05-24, 2210,                ,      USD,   -100, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
         2, 2024-05-24, 2270,                ,      USD,    100, Test_VAT_code, pytest collective txn 1 - line 2, /subdir/file2.txt
         3, 2024-04-24, 2210,                ,      USD,   -200, Test_VAT_code, pytest collective txn 2 - line 1, /document-col-alt.pdf
         3, 2024-04-24, 2270,                ,      USD,    200, Test_VAT_code, pytest collective txn 2 - line 2, /document-col-alt.pdf
         4, 2024-05-24, 2270,            2210,      CHF,    100, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
    """
    return pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)

def txn_to_str(df: pd.DataFrame) -> List[str]:
    df = nest(df, columns=[col for col in df.columns if not col in ['id', 'date']], key='txn')
    df = df.drop(columns=['id'])
    result = [f'{str(date)},{df_to_consistent_str(txn)}' for date, txn in zip(df['date'], df['txn'])]
    return result.sort()

def test_ledger_accessor_mutators_single_transaction(add_vat_code, ledger_entries):
    cashctrl_ledger = CashCtrlLedger()

    # Test adding a ledger entry
    target = ledger_entries.iloc[[0]]
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    cashctrl_ledger.add_ledger_entry(target)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test delete the created ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

    # Test adding a ledger entry without VAT code
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['vat_code'] = None
    new_entry['text'] = 'Ledger entry without VAT'
    cashctrl_ledger.add_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['amount'].iat[0] = 300
    new_entry['id'] = created['id'].iat[0]
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # TODO: CashCtrl doesn`t allow to convert a single transaction into collective
    # transaction if the single transaction has a taxId assigned. Tracked as cashctrl_ledger#27.
    # As a workaround, we reset the taxId manually before update with a collective transaction
    new_entry['vat_code'] = None
    cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Test replace with an collective ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[1, 2]].copy()
    new_entry['id'] = created['id'].iat[0]
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated.drop(columns=['id']).reset_index(drop=True), expected.drop(columns=['id']).reset_index(drop=True), check_column_type=False)

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

def test_ledger_accessor_mutators_collective_transaction(add_vat_code, ledger_entries):
    cashctrl_ledger = CashCtrlLedger()

    # Test adding a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    target = ledger_entries.iloc[[1, 2]]
    cashctrl_ledger.add_ledger_entry(target)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target)
    pd.testing.assert_frame_equal(created.drop(columns=['id']).reset_index(drop=True), expected.drop(columns=['id']).reset_index(drop=True))

    # Test delete the created ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

    # Test adding a ledger entry without VAT
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[1, 2]].copy()
    new_entry['vat_code'] = None
    cashctrl_ledger.add_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(created.drop(columns=['id']).reset_index(drop=True), expected.drop(columns=['id']).reset_index(drop=True))

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[1, 2]].copy()
    new_entry['amount'].iat[0] = 300
    new_entry['amount'].iat[1] = -300
    new_entry['id'] = created['id'].iat[0]
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated.reset_index(drop=True), expected.reset_index(drop=True))

    # Test replace with an individual ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['vat_code'] = None
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated.reset_index(drop=True), expected.reset_index(drop=True))

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

# Tests for addition logic edge cases
def test_add_ledger_with_non_existent_vat(ledger_entries):
    cashctrl_ledger = CashCtrlLedger()

    # Adding a ledger with non existent VAT code should raise an error
    entry = ledger_entries.iloc[[0]].copy()
    entry['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl_ledger.add_ledger_entry(entry=entry)

    # Adding a ledger with non existent account code should raise an error
    entry = ledger_entries.iloc[[0]].copy()
    entry['account'].iat[0] = 33333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl_ledger.add_ledger_entry(entry=entry)

    # Adding a ledger with non existent currency code should raise an error
    entry = ledger_entries.iloc[[0]].copy()
    entry['currency'].iat[0] = 'Non_Existent_Currency'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl_ledger.add_ledger_entry(entry=entry)

# Tests for updating logic edge cases
def test_update_ledger_with_edge_cases(add_vat_code, ledger_entries):
    cashctrl_ledger = CashCtrlLedger()

    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    cashctrl_ledger.add_ledger_entry(ledger_entries.iloc[[0]])
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)

    # Updating a ledger with non existent VAT code should raise an error
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent account code should raise an error
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['account'].iat[0] = 333333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent currency code should raise an error
    new_entry = ledger_entries.iloc[[0]].copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['currency'].iat[0] = 'CURRENCY'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Delete the ledger entry created above
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger(ledger_entries):
    cashctrl_ledger = CashCtrlLedger()
    entry = ledger_entries.iloc[[0]].copy()
    entry['id'] = 999999
    with pytest.raises(RequestException):
        cashctrl_ledger.update_ledger_entry(entry=entry)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl_ledger.delete_ledger_entry(ids='non-existent')

def test_mirror_ledger(add_vat_code, ledger_entries):
    cashctrl_ledger = CashCtrlLedger()

    # Mirror with one single and one collective transaction
    target = ledger_entries.iloc[[0, 1, 2]].copy()
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(target) == txn_to_str(mirrored)

    # Mirror with duplicate transactions and delete=False
    new_individual_transaction = ledger_entries.iloc[[0]].copy()
    new_collective_transaction = ledger_entries.iloc[[1, 2]].copy()
    new_individual_transaction['id'] = 5
    new_collective_transaction['id'] = 6
    target = pd.concat([
        ledger_entries.iloc[[0]],
        new_individual_transaction,
        ledger_entries.iloc[[1, 2]],
        new_collective_transaction,
    ])
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with alternative transactions and delete=False
    target = ledger_entries.iloc[[5, 3, 4]]
    expected = pd.concat([mirrored, StandaloneLedger.standardize_ledger(target)])
    cashctrl_ledger.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with delete=False
    target = ledger_entries.iloc[[0, 1, 2]]
    cashctrl_ledger.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with delete=True
    target = ledger_entries.iloc[[0, 1, 2]]
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(target)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror an empty target state
    cashctrl_ledger.mirror_ledger(target=pd.DataFrame({}), delete=True)
    assert cashctrl_ledger.ledger().empty
