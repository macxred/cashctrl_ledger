import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger
from pyledger import StandaloneLedger

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

def test_ledger_accessor_mutators_single_transaction(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)

    # Test adding a ledger entry
    target = StandaloneLedger.standardize_ledger(pd.DataFrame({
        'date': '2024-05-24',
        'account': [2270],
        'counter_account': [2210],
        'amount': [100],
        'currency': ['USD'],
        'text': ['pytest added ledger112'],
        'vat_code': ['Test_VAT_code'],
    })).drop(columns=['id'])

    # Test adding a ledger entry
    cashctrl_ledger.add_ledger_entry(date='2024-05-24', target=target)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), target)

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    created.loc[created.index[0], 'amount'] = 300
    cashctrl_ledger.update_ledger_entry(
        id=created.loc[created.index[0], 'id'],
        date='2024-05-24',
        target=created,
    )
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, created)

    # Test delete a ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=updated.loc[updated.index[0], 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert updated.loc[updated.index[0], 'id'] not in ledger['id']

def test_ledger_accessor_mutators_collective_transaction(add_vat_code):
    cashctrl_ledger = CashCtrlLedger()
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)

    # Test adding a ledger entry
    target = StandaloneLedger.standardize_ledger(pd.DataFrame({
        'date': ['2024-05-24', '2024-05-24'],
        'account': [2210, 2270],
        'amount': [-100, 100],
        'currency': ['USD', 'USD'],
        'text': ['pytest added ledger111', 'pytest added ledger222'],
        'vat_code': ['Test_VAT_code', 'Test_VAT_code'],
    })).drop(columns=['id'])

    # Test adding a ledger entry
    cashctrl_ledger.add_ledger_entry(date='2024-05-24', target=target)
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    created = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(created.drop(columns=['id']), target)

    # Test update a ledger entry
    initial_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    created.loc[created.index[0], 'amount'] = 300
    created.loc[created.index[1], 'amount'] = -300
    cashctrl_ledger.update_ledger_entry(
        id=created.loc[created.index[0], 'id'],
        date='2024-05-24',
        target=created,
    )
    updated_ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    outer_join = pd.merge(initial_ledger, updated_ledger, how='outer', indicator=True)
    updated = outer_join[outer_join['_merge'] == "right_only"].drop('_merge', axis = 1).reset_index(drop=True)
    pd.testing.assert_frame_equal(updated, created)

    # Test delete a ledger entry
    cashctrl_ledger.delete_ledger_entry(ids=updated.loc[updated.index[0], 'id'])
    ledger = cashctrl_ledger.ledger().reset_index(drop=True)
    assert updated.loc[updated.index[0], 'id'] not in ledger['id']
