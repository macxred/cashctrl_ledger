import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
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
    'date': ['2024-05-24'],
    'account': [2270],
    'counter_account': [2210],
    'amount': [100],
    'currency': ['USD'],
    'text': ['pytest added ledger112'],
    'vat_code': ['Test_VAT_code'],
})

collective_transaction = pd.DataFrame({
    'date': ['2024-05-24', '2024-05-24'],
    'account': [2210, 2270],
    'amount': [-100, 100],
    'currency': ['USD', 'USD'],
    'text': ['pytest added ledger111', 'pytest added ledger222'],
    'vat_code': ['Test_VAT_code', 'Test_VAT_code']
})

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
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

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
    new_entry.at[0, 'amount'] = 300
    new_entry['id'] = created.at[0, 'id']
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # TODO: CashCtrl doesn`t allow to convert a single transaction into collective
    # if the single transaction have an taxId, should reset it manually before update
    new_entry['vat_code'] = None
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    # Test replace with an individual ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = collective_transaction.copy()
    new_entry['id'] = created.at[0, 'id']
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

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
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

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
    new_entry.at[0, 'amount'] = 300
    new_entry.at[1, 'amount'] = -300
    new_entry['id'] = created.at[0, 'id']
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test replace with an individual ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    new_entry = individual_transaction.copy()
    new_entry['id'] = created.at[0, 'id']
    new_entry['vat_code'] = None
    cashctrl_ledger.update_ledger_entry(entry=new_entry)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    expected = StandaloneLedger.standardize_ledger(new_entry)
    pd.testing.assert_frame_equal(updated, expected)

    # Test delete the updated ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

# Tests for addition logic edge cases
def test_add_ledger_with_non_existent_vat(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)

    # Adding a ledger with non existent VAT code shouldn`t raise an error
    entry = individual_transaction.copy()
    entry.at[0, 'vat_code'] = 'Test_Non_Existent_VAT_code'
    cashctrl_ledger.add_ledger_entry(entry=entry)

    # Delete the ledger entry created above
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

    # Adding a ledger with non existent account code should raise an error
    entry = individual_transaction.copy()
    entry.at[0, 'account'] = 33333
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=entry)

    # Adding a ledger with non existent currency code shouldn`t raise an error
    entry = individual_transaction.copy()
    entry.at[0, 'currency'] = 'currency'
    cashctrl_ledger.add_ledger_entry(entry=entry)

    # Delete the ledger entry created above
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

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
    new_entry['id'] = created.at[0, 'id']
    new_entry.at[0, 'vat_code'] = 'Test_Non_Existent_VAT_code'
    cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent account code should raise an error
    new_entry = individual_transaction.copy()
    new_entry['id'] = created.at[0, 'id']
    new_entry.at[0, 'account'] = 333333
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Updating a ledger with non existent currency code should raise an error
    new_entry = individual_transaction.copy()
    new_entry['id'] = created.at[0, 'id']
    new_entry.at[0, 'currency'] = 'CURRENCY'
    cashctrl_ledger.update_ledger_entry(entry=new_entry)

    # Delete the ledger entry created above
    cashctrl_ledger.delete_ledger_entry(ids=created.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert created.at[0, 'id'] not in ledger['id']

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    entry = individual_transaction.copy()
    entry.at[0, 'id'] = 999999
    with pytest.raises(RequestException):
        cashctrl_ledger.update_ledger_entry(entry=entry)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl_ledger.delete_ledger_entry(ids='non-existent')
