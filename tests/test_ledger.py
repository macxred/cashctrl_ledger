from typing import List
import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger, df_to_consistent_str, nest
from pyledger import StandaloneLedger
from requests.exceptions import RequestException

@pytest.fixture(scope="session")
def add_vat_code():
    # Creates VAT code
    cashctrl_ledger = CashCtrlLedger()
    cashctrl_ledger.add_vat_code(
        code="Test_VAT_code",
        text='VAT 2%',
        account=2200,
        rate=0.02,
        inclusive=True,
    )

    yield

    # Deletes VAT code
    cashctrl_ledger.delete_vat_code(code="Test_VAT_code")

individual_transaction = pd.DataFrame({
    'id': ['1'],
    'date': ['2024-05-24'],
    'account': [2270],
    'counter_account': [2210],
    'amount': [100],
    'currency': ['USD'],
    'text': ['pytest added ledger112'],
    'vat_code': ['Test_VAT_code'],
})
collective_transaction = pd.DataFrame({
    'id': ['2', '2'],
    'date': ['2024-05-24', '2024-05-24'],
    'account': [2210, 2270],
    'amount': [-100, 100],
    'currency': ['USD', 'USD'],
    'text': ['pytest added ledger111', 'pytest added ledger222'],
    'vat_code': ['Test_VAT_code', 'Test_VAT_code']
})
alt_collective_transaction = pd.DataFrame({
    'id': ['3', '3'],
    'date': ['2024-04-24', '2024-04-24'],
    'account': [2210, 2270],
    'amount': [-200, 200],
    'currency': ['USD', 'USD'],
    'text': ['pytest added alt ledger 1', 'pytest added alt ledger 2'],
    'vat_code': ['Test_VAT_code', 'Test_VAT_code']
})
alt_individual_transaction = pd.DataFrame({
    'id': ['4'],
    'date': ['2024-04-24'],
    'account': [2270],
    'counter_account': [2210],
    'amount': [500],
    'currency': ['USD'],
    'text': ['pytest added alt single ledger'],
    'vat_code': ['Test_VAT_code'],
})

def txn_to_str(df: pd.DataFrame) -> List[str]:
    df = nest(df, columns=[col for col in df.columns if not col in ['id', 'date']], key='txn')
    df = df.drop(columns=['id']).reset_index(drop=True)
    result = [f'{str(date)},{df_to_consistent_str(txn)}' for date, txn in zip(df['date'], df['txn'])]
    return result.sort()

def test_ledger_accessor_mutators_single_transaction(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()

    # Test adding a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    cashctrl_ledger.add_ledger_entry(entry=individual_transaction)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(individual_transaction)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test delete the created ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

    # Test adding a ledger entry without VAT code
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = individual_transaction.copy()
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
    new_entry = individual_transaction.copy()
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

    # Test replace with an individual ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = collective_transaction.copy()
    new_entry['id'] = created['id'].iat[0]
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

def test_ledger_accessor_mutators_collective_transaction(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()

    # Test adding a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    cashctrl_ledger.add_ledger_entry(entry=collective_transaction)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(collective_transaction)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test delete the created ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

    # Test adding a ledger entry without VAT
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = collective_transaction.copy()
    new_entry['vat_code'] = None
    cashctrl_ledger.add_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), expected.drop(columns=['id']))

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = collective_transaction.copy()
    new_entry['amount'].iat[0] = 300
    new_entry['amount'].iat[1] = -300
    new_entry['id'] = created['id'].iat[0]
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test replace with an individual ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = individual_transaction.copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['vat_code'] = None
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

# Tests for addition logic edge cases
def test_add_ledger_with_non_existent_vat():
    cashctrl_ledger = CashCtrlLedger()

    # Adding a ledger with non existent VAT code should raise an error
    entry = individual_transaction.copy()
    entry['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=entry)

    # Adding a ledger with non existent account code should raise an error
    entry = individual_transaction.copy()
    entry['account'].iat[0] = 33333
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=entry)

    # Adding a ledger with non existent currency code should raise an error
    entry = individual_transaction.copy()
    entry['currency'].iat[0] = 'Non_Existent_Currency'
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=entry)

# Tests for updating logic edge cases
def test_update_ledger_with_edge_cases(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()

    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    cashctrl_ledger.add_ledger_entry(entry=individual_transaction)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)

    # Updating a ledger with non existent VAT code should raise an error
    new_entry = individual_transaction.copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent account code should raise an error
    new_entry = individual_transaction.copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['account'].iat[0] = 333333
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent currency code should raise an error
    new_entry = individual_transaction.copy()
    new_entry['id'] = created['id'].iat[0]
    new_entry['currency'].iat[0] = 'CURRENCY'
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Delete the ledger entry created above
    cashctrl_ledger.delete_ledger_entry(ids=created['id'].iat[0])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created['id'].iat[0] not in ledger['id']

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    entry = individual_transaction.copy()
    entry['id'].iat[0] = 999999
    with pytest.raises(RequestException):
        cashctrl_ledger.update_ledger_entry(entry=entry)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl_ledger.delete_ledger_entry(ids='non-existent')

def test_mirror_ledger(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial = cashctrl_ledger.ledger().reset_index(drop=True)

    # Mirror with one single and one collective transaction
    target = pd.concat([individual_transaction, collective_transaction])
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(target) == txn_to_str(mirrored)

    # Mirror with duplicate transactions and delete=False
    new_individual_transaction = individual_transaction.copy()
    new_collective_transaction = collective_transaction.copy()
    new_individual_transaction['id'].iat[0] = 5
    new_collective_transaction['id'].iat[0] = 6
    new_collective_transaction['id'].iat[1] = 6
    target = pd.concat([
        individual_transaction,
        new_individual_transaction,
        collective_transaction,
        new_collective_transaction,
    ])
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(target) == txn_to_str(mirrored)

    # Mirror with alternative transactions and delete=False
    remote = cashctrl_ledger.ledger().reset_index(drop=True)
    target = pd.concat([alt_individual_transaction, alt_collective_transaction])
    remote = pd.concat([remote, target])
    cashctrl_ledger.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(remote) == txn_to_str(mirrored)

    # Mirror with delete=False
    remote = cashctrl_ledger.ledger().reset_index(drop=True)
    target = pd.concat([individual_transaction, collective_transaction])
    cashctrl_ledger.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(target) == txn_to_str(mirrored)

    # Mirror with delete=True
    target = pd.concat([individual_transaction, collective_transaction])
    cashctrl_ledger.mirror_ledger(target=target)
    mirrored = cashctrl_ledger.ledger()
    assert txn_to_str(target) == txn_to_str(mirrored)

    # Mirror an empty target state
    cashctrl_ledger.mirror_ledger(target=pd.DataFrame({}), delete=True)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    assert mirrored.empty

    # Restore initial state
    cashctrl_ledger.mirror_ledger(target=initial, delete=True)
    mirrored = cashctrl_ledger.ledger().reset_index(drop=True)
    assert txn_to_str(initial) == txn_to_str(mirrored)