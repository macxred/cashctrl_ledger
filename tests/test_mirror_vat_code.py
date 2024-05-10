"""
This module defines tests for mirroring VAT codes using the CashCtrlLedger class.
It uses pytest fixtures to set up test data and mock dependencies, ensuring that
the VAT code synchronization logic correctly handles various scenarios including creating, updating,
and deleting VAT codes based on different states of local and remote data.
"""

import pandas as pd
from unittest.mock import MagicMock
import pytest

from cashctrl_ledger import CashCtrlLedger

@pytest.fixture
def setup_cashctrl_ledger():
    """
    Creates a CashCtrlLedger instance with mocked methods to simulate
    interactions with the VAT code management API.
    """
    ledger = CashCtrlLedger()
    ledger.get_vat_codes = MagicMock()
    ledger.delete_vat_code = MagicMock()
    ledger.client.list_accounts = MagicMock()
    ledger.add_vat_code = MagicMock()
    ledger.update_vat_code = MagicMock()
    return ledger

@pytest.fixture
def remote_vat_codes():
    """
    Provides a DataFrame representing a mocked list of remote VAT codes.
    """
    data = {
        'api_id': ['1', '2', '16'],
        'id': ['test_api_update', 'test_api', 'Exempt'],
        'rate': [22.0, 33.0, 0.0],
        'accountId': ['1', '2', '3'],
        'inclusive': [True, True, True],
        'text': ['test_api_update', 'test_api', 'Exempt from VAT']
    }
    return pd.DataFrame(data)

@pytest.fixture
def target_state():
    """
    Provides a DataFrame representing the target state of VAT codes to be achieved
    on the remote system.
    """
    data = {
        'id': ['99'],
        'account': ['1100'],
        'rate': ['10'],
        'inclusive': [True],
        'text': ['Test target state']
    }
    return pd.DataFrame(data)

@pytest.fixture
def account_data():
    """
    Provides a DataFrame representing mocked account data relevant to VAT processing.
    """
    data = {
        'id': ['1', '2', '3'],
        'number': [1100, 9900 ,1100],
        'name': ['Account1', 'Account2', 'Account3'],
    }
    return pd.DataFrame(data)

def test_local_and_remote_empty(setup_cashctrl_ledger):
    """
    Tests that no operations are performed when both local and remote VAT code lists are empty.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = pd.DataFrame()
    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame())
    setup_cashctrl_ledger.delete_vat_code.assert_not_called()
    setup_cashctrl_ledger.add_vat_code.assert_not_called()
    setup_cashctrl_ledger.update_vat_code.assert_not_called()

def test_only_local_empty_should_delete_all_remote(setup_cashctrl_ledger, remote_vat_codes):
    """
    Tests that all remote VAT codes are deleted if the local list is empty and deletion is enabled.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame(), delete=True)
    setup_cashctrl_ledger.add_vat_code.assert_not_called()
    setup_cashctrl_ledger.update_vat_code.assert_not_called()
    setup_cashctrl_ledger.delete_vat_code.assert_called_with('1,2,16')

def test_only_local_empty_should_leave_remote(setup_cashctrl_ledger, remote_vat_codes):
    """
    Tests that no remote VAT codes are deleted if the local list is empty and deletion is disabled.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame(), delete=False)
    setup_cashctrl_ledger.add_vat_code.assert_not_called()
    setup_cashctrl_ledger.update_vat_code.assert_not_called()
    setup_cashctrl_ledger.delete_vat_code.assert_not_called()

def test_should_create_on_remote(setup_cashctrl_ledger, remote_vat_codes, target_state, account_data):
    """
    Tests that new VAT codes specified in the target state are created on the remote system.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.client.list_accounts.return_value = account_data
    setup_cashctrl_ledger.mirror_vat_codes(target_state, delete=False)
    setup_cashctrl_ledger.update_vat_code.assert_not_called()
    setup_cashctrl_ledger.delete_vat_code.assert_not_called()
    setup_cashctrl_ledger.add_vat_code.assert_called_with(code='99', rate=10.0, account='1', inclusive=True, text='Test target state')

def test_should_update_on_remote(setup_cashctrl_ledger, remote_vat_codes, account_data):
    """
    Tests that existing VAT codes on the remote system are updated to match the target state.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.client.list_accounts.return_value = account_data

    data = {
        'id': ['Exempt'],
        'account': ['1100'],
        'rate': ['10'],
        'inclusive': [True],
        'text': ['Test target state']
    }

    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame(data), delete=False)
    setup_cashctrl_ledger.add_vat_code.assert_not_called()
    setup_cashctrl_ledger.delete_vat_code.assert_not_called()
    setup_cashctrl_ledger.update_vat_code.assert_called_with(code='16', rate=10.0, account='1', inclusive=True, text='Test target state', name='Exempt')

def test_should_update_and_create_on_remote(setup_cashctrl_ledger, remote_vat_codes, account_data):
    """
    Tests that both updating existing remote VAT codes and creating new ones work as intended.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.client.list_accounts.return_value = account_data

    data = {
        'id': ['Exempt', '99'],
        'account': ['1100', '1100'],
        'rate': ['10', '10'],
        'inclusive': [True, True],
        'text': ['Test target state', 'Test target state']
    }

    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame(data), delete=False)
    setup_cashctrl_ledger.delete_vat_code.assert_not_called()
    setup_cashctrl_ledger.add_vat_code.assert_called_with(code='99', rate=10.0, account='3', inclusive=True, text='Test target state')
    setup_cashctrl_ledger.update_vat_code.assert_called_with(code='16', rate=10.0, account='1', inclusive=True, text='Test target state', name='Exempt')

def test_should_update_and_create_and_delete_on_remote(setup_cashctrl_ledger, remote_vat_codes, account_data):
    """
    Tests that the remote VAT codes are correctly updated, created, and deleted based on the target state.
    """
    setup_cashctrl_ledger.get_vat_codes.return_value = remote_vat_codes
    setup_cashctrl_ledger.client.list_accounts.return_value = account_data

    data = {
        'id': ['Exempt', '99'],
        'account': ['1100', '1100'],
        'rate': ['10', '10'],
        'inclusive': [True, True],
        'text': ['Test target state', 'Test target state']
    }

    setup_cashctrl_ledger.mirror_vat_codes(pd.DataFrame(data), delete=True)
    setup_cashctrl_ledger.delete_vat_code.assert_called_with('1,2')
    setup_cashctrl_ledger.add_vat_code.assert_called_with(code='99', rate=10.0, account='3', inclusive=True, text='Test target state')
    setup_cashctrl_ledger.update_vat_code.assert_called_with(code='16', rate=10.0, account='1', inclusive=True, text='Test target state', name='Exempt')
