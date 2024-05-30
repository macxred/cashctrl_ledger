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

@pytest.fixture
def single_transaction() -> pd.DataFrame:
    return StandaloneLedger.standardize_ledger(pd.DataFrame({
        'date': '2024-05-24',
        'account': [2270],
        'counter_account': [2210],
        'amount': [100],
        'currency': ['USD'],
        'text': ['pytest added ledger112'],
        'vat_code': ['Test_VAT_code'],
    })).drop(columns=['id'])

@pytest.fixture
def collective_transaction() -> pd.DataFrame:
    return StandaloneLedger.standardize_ledger(pd.DataFrame({
        'date': ['2024-05-24', '2024-05-24'],
        'account': [2210, 2270],
        'amount': [-100, 100],
        'currency': ['USD', 'USD'],
        'text': ['pytest added ledger111', 'pytest added ledger222'],
        'vat_code': ['Test_VAT_code', 'Test_VAT_code'],
    })).drop(columns=['id'])

@pytest.fixture
def add_transaction() -> pd.DataFrame:
    def _create_transaction(transaction: pd.DataFrame) -> pd.DataFrame:
        cashctrl_ledger = CashCtrlLedger()
        initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
        cashctrl_ledger.add_ledger_entry(entry=transaction)
        updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
        outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
        return outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    return _create_transaction

def test_ledger_accessor_mutators_single_transaction(add_vat_code, single_transaction):
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)

    # Test adding a ledger entry
    cashctrl_ledger.add_ledger_entry(entry=single_transaction)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), single_transaction)

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    created.at[0, 'amount'] = 300
    cashctrl_ledger.update_ledger_entry(entry=created)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, created)

    # Test delete a ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=updated.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert updated.at[0, 'id'] not in ledger['id']

def test_ledger_accessor_mutators_collective_transaction(add_vat_code, collective_transaction):
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)

    # Test adding a ledger entry
    cashctrl_ledger.add_ledger_entry(entry=collective_transaction)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), collective_transaction)

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    created.at[0, 'amount'] = 300
    created.at[1, 'amount'] = -300
    cashctrl_ledger.update_ledger_entry(entry=created)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, created)

    # Test delete a ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=updated.at[0, 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert updated.at[0, 'id'] not in ledger['id']

# Tests for addition logic edge cases
def test_add_ledger_with_non_existent_vat(single_transaction):
    cashctrl_ledger = CashCtrlLedger()

    # Adding a ledger with non existent VAT code should raise an error
    single_transaction.at[0, 'vat_code'] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=single_transaction)

    # Adding a ledger with non existent account code should raise an error
    single_transaction.at[0, 'account'] = 33333
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=single_transaction)

    # Adding a ledger with non existent currency code should raise an error
    single_transaction.at[0, 'currency'] = 'currency'
    with pytest.raises(KeyError):
        cashctrl_ledger.add_ledger_entry(entry=single_transaction)

# Tests for updating logic edge cases
def test_update_ledger_with_edge_cases(add_vat_code, single_transaction, add_transaction):
    cashctrl_ledger = CashCtrlLedger()
    transaction = add_transaction(single_transaction)

    # Updating a ledger with non existent VAT code should raise an error
    transaction.at[0, 'vat_code'] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=transaction)

    # Updating a ledger with non existent account code should raise an error
    transaction.at[0, 'account'] = 333333
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=transaction)

    # Updating a ledger with non existent currency code should raise an error
    transaction.at[0, 'currency'] = 'CURRENCY'
    with pytest.raises(KeyError):
        cashctrl_ledger.update_ledger_entry(entry=transaction)

    cashctrl_ledger.delete_ledger_entry(ids=transaction.at[0, 'id'])

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger_(single_transaction):
    cashctrl_ledger = CashCtrlLedger()
    single_transaction['id'] = 'non_existent_id'
    with pytest.raises(RequestException):
        cashctrl_ledger.update_ledger_entry(entry=single_transaction)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl_ledger = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl_ledger.delete_ledger_entry(ids='non-existent')

# Updating a single entry with a collective ledger
def test_update_single_ledger_with_collective(add_vat_code, single_transaction, collective_transaction, add_transaction):
    cashctrl_ledger = CashCtrlLedger()
    transaction = add_transaction(single_transaction)
    collective_transaction.at[0, 'id'] = transaction.at[0, 'id']
    cashctrl_ledger.update_ledger_entry(entry=collective_transaction)
    cashctrl_ledger.delete_ledger_entry(ids=transaction.at[0, 'id'])

# Updating a single entry with a single ledger
def test_update_collective_ledger_with_single(add_vat_code, single_transaction, collective_transaction, add_transaction):
    cashctrl_ledger = CashCtrlLedger()
    transaction = add_transaction(collective_transaction)
    single_transaction.at[0, 'id'] = transaction.at[0, 'id']
    cashctrl_ledger.update_ledger_entry(entry=single_transaction)
    cashctrl_ledger.delete_ledger_entry(ids=transaction.at[0, 'id'])
