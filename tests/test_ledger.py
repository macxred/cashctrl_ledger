"""
Unit tests for ledger accessors, mutators and mirroring.
"""

from io import StringIO
from typing import List
import pytest
import pandas as pd
from cashctrl_ledger import CashCtrlLedger, df_to_consistent_str, nest, assert_frame_equal
from pyledger import StandaloneLedger
from requests.exceptions import RequestException


ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10021,      EUR,         , Test EUR Bank Account
    /Assets, 10022,      USD,         , Test USD Bank Account
    /Assets, 10023,      CHF,         , Test CHF Bank Account
    /Assets, 19991,      EUR,         , Transitory Account EUR
    /Assets, 19992,      USD,         , Transitory Account USD
    /Assets, 19993,      CHF,         , Transitory Account CHF
    /Assets, 19999,      CHF,         , Transitory Account CHF
    /Assets, 22000,      CHF,         , Input Tax
"""

VAT_CSV = """
    id,             rate, account, inclusive, text
    Test_VAT_code,  0.02,   22000,      True, Input Tax 2%
"""

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10023,           19993,      CHF,     100.00,                     , Test_VAT_code, pytest single transaction 1,      /file1.txt
    2,  2024-05-24, 10022,                ,      USD,    -100.00,               -88.88, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,       1.00,                 0.89, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,      99.00,                87.99, Test_VAT_code, pytest collective txn 1 - line 1,
    3,  2024-04-24,      ,           10021,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 1, /document-col-alt.pdf
    3,  2024-04-24, 10021,                ,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 2, /document-col-alt.pdf
    4,  2024-05-24, 10022,           19992,      USD,     300.00,               450.45, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
    5,  2024-04-04, 19993,                ,      CHF, -125000.00,           -125000.00,              , Convert -125'000 CHF to USD @ 1.10511,
    5,  2024-04-04, 19992,                ,      USD,  138138.75,            125000.00,              , Convert -125'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 19993,                ,      CHF, -250000.00,                     ,              , Convert -250'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 19992,                ,      USD,  276277.50,            250000.00,              , Convert -250'000 CHF to USD @ 1.10511,
        # Transaction raised RequestException: API call failed. Total debit (125,000.00) and total credit (125,000.00) must be equal.
    7,  2024-01-16,      ,           19991,      EUR,  125000.00,            125362.50,              , Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
    7,  2024-01-16, 19993,                ,      CHF,  125362.50,            125362.50,              , Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
        # Transactions with negative amount
    8,  2024-05-24, 10021,           19991,      EUR,     -10.00,                -9.00,              , Individual transaction with negative amount,
        # Collective transaction with credit and debit account in single line item
    9,  2024-05-24, 10023,           19993,      CHF,     100.00,                     ,              , Collective transaction - leg with debit and credit account,
    9,  2024-05-24, 10021,                ,      EUR,      20.00,                19.00,              , Collective transaction - leg with credit account,
    9,  2024-05-24,      ,           19991,      EUR,      20.00,                19.00,              , Collective transaction - leg with debit account,
        # Transactions with zero base currency
    10, 2024-05-24, 10023,           19993,      CHF,       0.00,                     ,              , Individual transaction with zero amount,
    11, 2024-05-24, 10023,                ,      CHF,     100.00,                     , Test_VAT_code, Collective transaction with zero amount,
    11, 2024-05-24, 19993,                ,      CHF,    -100.00,                     ,              , Collective transaction with zero amount,
    11, 2024-05-24, 19993,                ,      CHF,       0.00,                     ,              , Collective transaction with zero amount,
    12, 2024-03-02,      ,           19991,      EUR,  600000.00,            599580.00,              , Convert 600k EUR to CHF @ 0.9993,
    12, 2024-03-02, 19993,                ,      CHF,  599580.00,            599580.00,              , Convert 600k EUR to CHF @ 0.9993,
        # FX gain/loss: transactions in base currency with zero foreign currency amount
    13, 2024-06-26, 10022,           19993,      CHF,     999.00,                     ,              , Foreign currency adjustment
    14, 2024-06-26, 10021,                ,      EUR,       0.00,                 5.55,              , Foreign currency adjustment
    14, 2024-06-26,      ,           19993,      CHF,       5.55,                     ,              , Foreign currency adjustment
        # Transactions with two non-base currencies
    15, 2024-06-26,      ,           10022,      USD,  100000.00,             90000.00,              , Convert 100k USD to EUR @ 0.9375,
    15, 2024-06-26, 10021,                ,      EUR,   93750.00,             90000.00,              , Convert 100k USD to EUR @ 0.9375,
    16, 2024-06-26,      ,           10022,      USD,  200000.00,            180000.00,              , Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 10021,                ,      EUR,   93750.00,             90000.00,              , Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 10023,                ,      CHF,   90000.00,             90000.00,              , Convert 200k USD to EUR and CHF,
        # Foreign currency transaction exceeding precision for exchange rates in CashCtrl
    17, 2024-06-26, 10022,           19992,      USD,90000000.00,          81111111.11,              , Value 90 Mio USD @ 0.9012345679 with 10 digits precision,
    18, 2024-06-26,      ,           19992,      USD, 9500000.00,           7888888.88,              , Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
    18, 2024-06-26, 10023,                ,      CHF, 7888888.88,                     ,              , Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
"""
STRIPPED_CSV = '\n'.join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
TEST_VAT_CODE = pd.read_csv(StringIO(VAT_CSV), skipinitialspace=True)


@pytest.fixture(scope="module")
def set_up_vat_and_account():
    cashctrl = CashCtrlLedger()
    cashctrl.transitory_account = 19999

    # Fetch original state
    initial_vat_codes = cashctrl.vat_codes().reset_index()
    initial_account_chart = cashctrl.account_chart().reset_index()
    initial_ledger = cashctrl.ledger()

    # Create test accounts and VAT code
    cashctrl.mirror_account_chart(TEST_ACCOUNTS, delete=False)
    cashctrl.mirror_vat_codes(TEST_VAT_CODE, delete=False)

    yield

    # Restore initial state
    cashctrl.mirror_ledger(initial_ledger, delete=True)
    cashctrl.mirror_vat_codes(initial_vat_codes, delete=True)
    cashctrl.mirror_account_chart(initial_account_chart, delete=True)

def txn_to_str(df: pd.DataFrame) -> List[str]:
    df = nest(df, columns=[col for col in df.columns if not col in ['id', 'date']], key='txn')
    df = df.drop(columns=['id'])
    result = [f'{str(date)},{df_to_consistent_str(txn)}' for date, txn in zip(df['date'], df['txn'])]
    result.sort()
    return result

@pytest.mark.parametrize("ledger_id", set(LEDGER_ENTRIES['id'].unique()).difference([15, 16, 17, 18]))
def test_add_ledger_entry(set_up_vat_and_account, ledger_id):
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.query('id == @ledger_id')
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(created, expected, ignore_index=True, ignore_columns=['id'], check_exact=True)

def test_ledger_accessor_mutators_single_transaction(set_up_vat_and_account):
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.query('id == 1')
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(created, expected, ignore_index=True, ignore_columns=['id'])

    # Test update the ledger entry
    target = LEDGER_ENTRIES.query('id == 4').copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # TODO: CashCtrl doesn`t allow to convert a single transaction into collective
    # transaction if the single transaction has a taxId assigned. See cashctrl#27.
    # As a workaround, we reset the taxId manually before update with a collective transaction
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)

    # Test replace with an collective ledger entry
    target = LEDGER_ENTRIES.query('id == 2').copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # Test delete the created ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert all(remote['id'] != str(id)), f"Ledger entry {id} was not deleted"

def test_ledger_accessor_mutators_single_transaction_without_VAT():
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry without VAT code
    target = LEDGER_ENTRIES.query('id == 4').copy()
    target['vat_code'] = None
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(created, expected, ignore_index=True, ignore_columns=['id'])

    # Test update the ledger entry
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert all(remote['id'] != str(id)), f"Ledger entry {id} was not deleted"

def test_ledger_accessor_mutators_collective_transaction(set_up_vat_and_account):
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.query('id == 2')
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(created, expected, ignore_index=True, ignore_columns=['id'])

    # Test update the ledger entry
    target = LEDGER_ENTRIES.query('id == 3').copy()
    target['id'] = id
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # Test replace with an individual ledger entry
    target = LEDGER_ENTRIES.iloc[[0]].copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert all(remote['id'] != str(id)), f"Ledger entry {id} was not deleted"

def test_ledger_accessor_mutators_collective_transaction_without_vat():
    cashctrl = CashCtrlLedger()

    # Test adding a ledger entry
    target = LEDGER_ENTRIES.query('id == 2').copy()
    target['vat_code'] = None
    id = cashctrl.add_ledger_entry(target)
    remote = cashctrl.ledger()
    created = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(created, expected, ignore_index=True, ignore_columns=['id'])

    # Test update the ledger entry
    target = LEDGER_ENTRIES.query('id == 3').copy()
    target['id'] = id
    target['vat_code'] = None
    cashctrl.update_ledger_entry(target)
    remote = cashctrl.ledger()
    updated = remote.loc[remote['id'] == str(id)]
    expected = cashctrl.standardize_ledger(target)
    assert_frame_equal(updated, expected, ignore_index=True)

    # Test delete the updated ledger entry
    cashctrl.delete_ledger_entry(id)
    remote = cashctrl.ledger()
    assert all(remote['id'] != str(id)), f"Ledger entry {id} was not deleted"

def test_add_ledger_with_non_existing_vat():
    # Adding a ledger entry with non existing VAT code should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['vat_code'].iat[0] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl.add_ledger_entry(target)

def test_add_ledger_with_non_existing_account():
    # Adding a ledger entry with non existing account should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['account'] = 33333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl.add_ledger_entry(target)

def test_add_ledger_with_non_existing_currency():
    # Adding a ledger entry with non existing currency code should raise an error
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['currency'] = 'Non_Existent_Currency'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl.add_ledger_entry(target)

@pytest.mark.parametrize("id", [15, 16])
def test_adding_transaction_with_two_non_base_currencies_fails(set_up_vat_and_account, id):
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES[LEDGER_ENTRIES['id'] == id]
    expected = "CashCtrl allows only the base currency plus a single foreign currency"
    with pytest.raises(ValueError, match=expected):
        cashctrl.add_ledger_entry(target)

def test_update_ledger_with_illegal_attributes(set_up_vat_and_account):
    cashctrl = CashCtrlLedger()
    id = cashctrl.add_ledger_entry(LEDGER_ENTRIES.query('id == 1'))

    # Updating a ledger with non existent VAT code should raise an error
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['id'] = id
    target['vat_code'] = 'Test_Non_Existent_VAT_code'
    with pytest.raises(ValueError, match='No id found for tax code'):
        cashctrl.update_ledger_entry(target)

    # Updating a ledger with non existent account code should raise an error
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['id'] = id
    target['account'].iat[0] = 333333
    with pytest.raises(ValueError, match='No id found for account'):
        cashctrl.update_ledger_entry(target)

    # Updating a ledger with non existent currency code should raise an error
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['id'] = id
    target['currency'].iat[0] = 'Non_Existent_Currency'
    with pytest.raises(ValueError, match='No id found for currency'):
        cashctrl.update_ledger_entry(target)

    # Delete the ledger entry created above
    cashctrl.delete_ledger_entry(id)

# Updating a non-existent ledger should raise an error
def test_update_non_existent_ledger():
    cashctrl = CashCtrlLedger()
    target = LEDGER_ENTRIES.query('id == 1').copy()
    target['id'] = 999999
    with pytest.raises(RequestException):
        cashctrl.update_ledger_entry(target)

# Deleting a non-existent ledger should raise an error
def test_delete_non_existent_ledger():
    cashctrl = CashCtrlLedger()
    with pytest.raises(RequestException):
        cashctrl.delete_ledger_entry(ids='non-existent')

def test_split_multi_currency_transactions():
    cashctrl = CashCtrlLedger()
    transitory_account = 19993
    txn = cashctrl.standardize_ledger(LEDGER_ENTRIES.query('id == 15'))
    spit_txn = cashctrl.split_multi_currency_transactions(txn, transitory_account=transitory_account)
    is_base_currency = spit_txn['currency'] == cashctrl.base_currency
    spit_txn.loc[is_base_currency, 'base_currency_amount'] = spit_txn.loc[is_base_currency, 'amount']
    assert len(spit_txn) == len(txn) + 2, "Expecting two new lines when transaction is split"
    assert sum(spit_txn['account'] == transitory_account) == 2, (
        "Expecting two transactions on transitory account")
    assert all(spit_txn.groupby('id')['base_currency_amount'].sum() == 0), (
        "Expecting split transactions to be balanced")
    assert spit_txn.query("account == @transitory_account")['base_currency_amount'].sum() == 0, (
        "Expecting transitory account to be balanced")

def test_split_several_multi_currency_transactions():
    cashctrl = CashCtrlLedger()
    transitory_account = 19993
    txn = cashctrl.standardize_ledger(LEDGER_ENTRIES.query('id.isin([15, 16])'))
    spit_txn = cashctrl.split_multi_currency_transactions(txn, transitory_account=transitory_account)
    is_base_currency = spit_txn['currency'] == cashctrl.base_currency
    spit_txn.loc[is_base_currency, 'base_currency_amount'] = spit_txn.loc[is_base_currency, 'amount']
    id_currency_pairs = (txn['id'] + txn['currency']).nunique()
    assert len(spit_txn) == len(txn) + id_currency_pairs, (
        "Expecting one new line per currency and transaction")
    assert sum(spit_txn['account'] == transitory_account) == id_currency_pairs, (
        "Expecting one transaction on transitory account per id and currency")
    assert all(spit_txn.groupby('id')['base_currency_amount'].sum() == 0), (
        "Expecting split transactions to be balanced")
    assert spit_txn.query("account == @transitory_account")['base_currency_amount'].sum() == 0, (
        "Expecting transitory account to be balanced")

def test_mirror_ledger(set_up_vat_and_account):
    cashctrl = CashCtrlLedger()
    cashctrl.transitory_account = 19999

    # Mirror with one single and one collective transaction
    target = LEDGER_ENTRIES.query('id in [1, 2]')
    cashctrl.mirror_ledger(target=target, delete=True)
    expected = cashctrl.standardize_ledger(target)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with duplicate transactions and delete=False
    target = pd.concat([
        LEDGER_ENTRIES.query('id == 1'),
        LEDGER_ENTRIES.query('id == 1').assign(id=5),
        LEDGER_ENTRIES.query('id == 2').assign(id=6),
        LEDGER_ENTRIES.query('id == 2')
    ])
    cashctrl.mirror_ledger(target=target)
    expected = cashctrl.standardize_ledger(target)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with complex transactions and delete=False
    target = LEDGER_ENTRIES.query('id in [15, 16, 17, 18]')
    cashctrl.mirror_ledger(target=target, delete=False)
    expected = cashctrl.standardize_ledger(target)
    expected = cashctrl.sanitize_ledger(expected)
    expected = pd.concat([mirrored, expected])
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror existing transactions with delete=False has no impact
    target = LEDGER_ENTRIES.query('id in [1, 2]')
    cashctrl.mirror_ledger(target=target, delete=False)
    mirrored = cashctrl.ledger()
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror with delete=True
    target = LEDGER_ENTRIES.query('id in [1, 2]')
    cashctrl.mirror_ledger(target=target)
    mirrored = cashctrl.ledger()
    expected = cashctrl.standardize_ledger(target)
    assert txn_to_str(mirrored) == txn_to_str(expected)

    # Mirror an empty target state
    cashctrl.mirror_ledger(target=pd.DataFrame({}), delete=True)
    assert cashctrl.ledger().empty
