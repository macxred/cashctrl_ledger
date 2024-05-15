"""
Module to sync ledger system onto CashCtrl.
"""

import pandas as pd
from cashctrl_api import CashCtrlClient
from pyledger import LedgerEngine, StandaloneLedger

class CashCtrlLedger(LedgerEngine):
    """
    Class that give you an ability to sync ledger system onto CashCtrl

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    def __init__(self, client: CashCtrlClient | None = None):
        super().__init__()
        self._client = CashCtrlClient() if client is None else client

    def vat_codes(self) -> pd.DataFrame:
        """
        Retrieves VAT codes from the remote CashCtrl account and converts to standard
        pyledger format.

        Returns:
            pd.DataFrame: A DataFrame with pyledger.VAT_CODE column schema.
        """
        tax_rates = self._client.list_tax_rates()
        accounts = self._client.list_accounts()
        account_map = accounts.set_index('id')['number'].to_dict()
        if not tax_rates['accountId'].isin(account_map).all():
            raise ValueError("Unknown 'accountId' in CashCtrl tax rates.")
        result = pd.DataFrame({
            'id': tax_rates['name'],
            'text': tax_rates['documentName'],
            'account': tax_rates['accountId'].map(account_map),
            'rate': tax_rates['percentage'] / 100,
            'inclusive': ~ tax_rates['isGrossCalcType'],
        })

        return StandaloneLedger.standardize_vat_codes(result)
    
    def mirror_vat_codes(self, target_state: pd.DataFrame, delete: bool = True):
        """
        Not implemented yet
        """
        raise NotImplementedError

    def _single_account_balance():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def account_chart():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def add_account():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def add_ledger_entry():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def add_price():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def add_vat_code(
        self, code: str, rate: float, account: str, inclusive: bool = True,
        text: str = ""
    ):
        """
        Adds a new VAT code to the CashCtrl account.

        Parameters:
            code (str): The VAT code to be added.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool): Determines whether the VAT is calculated as 'NET'
                            (True, default) or 'GROSS' (False).
            text (str): Additional text or description associated with the VAT code.
        """
        accounts = self._client.list_accounts()
        account_map = accounts.set_index('number')['id'].to_dict()
        if account not in account_map:
            raise ValueError(f"Account '{account}' does not exist.")
        payload = {
            "name": code,
            "percentage": rate*100,
            "accountId": account_map[account],
            "calcType": "NET" if inclusive else "GROSS",
            "documentName": text,
        }
        self._client.post("tax/create.json", data=payload)
        
    def update_vat_code(
        self, code: str, rate: float, account: str,
        inclusive: bool = True, text: str = ""
    ):
        """
        Updates an existing VAT code in the CashCtrl account with new parameters.

        Parameters:
            code (str): The VAT code to be updated.
            rate (float): The VAT rate, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool): Determines whether the VAT is calculated as 'NET'
                            (True, default) or 'GROSS' (False).
            text (str): Additional text or description associated with the VAT code.
        """
        # Find remote account id
        accounts = self._client.list_accounts()
        account_map = accounts.set_index('number')['id'].to_dict()
        if account not in account_map:
            raise ValueError(f"Account '{account}' does not exist.")

        # Find remote tax id
        remote_vats = self._client.list_tax_rates()
        remote_vat = remote_vats.loc[remote_vats['name'] == code]
        if len(remote_vat) < 1:
            raise ValueError(f"There is no VAT code '{code}'.")
        elif len(remote_vat) > 1:
            raise ValueError(f"VAT code '{code}' is duplicated.")

        # Update remote tax record
        payload = {
            "id": remote_vat['id'].item(),
            "percentage": rate*100,
            "accountId": account_map[account],
            "calcType": "NET" if inclusive else "GROSS",
            "name": code,
            "documentName": text,
        }
        self._client.post("tax/update.json", data=payload)

    def base_currency():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def delete_account():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def delete_ledger_entry():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def delete_price():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def delete_vat_code(self, code: str, allow_missing: bool = False):
        """
        Deletes a VAT code from the remote CashCtrl account.

        Parameters:
            code (str): The VAT code name to be deleted.
            allow_missing (bool): If True, no error is raised if the VAT
                code is not found; if False, raises ValueError.
        """
        tax_rates = self._client.list_tax_rates()
        to_delete = tax_rates.loc[tax_rates['name'] == code, 'id']

        if len(to_delete) > 0:
            delete_ids = ",".join(to_delete.astype(str))
            self._client.post('tax/delete.json', {'ids': delete_ids})
        elif not allow_missing:
            raise ValueError(f"There is no VAT code '{code}'.")

    def ledger():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def ledger_entry():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def modify_account():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def modify_ledger_entry():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def precision():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def price():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def price_history():
        """
        Not implemented yet
        """
        raise NotImplementedError 
