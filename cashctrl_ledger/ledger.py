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
            pd.DataFrame: A DataFrame from the remote system with pyledger.VAT_CODE 
                        column schema.
        """
        vat_codes = self._client.list_tax_rates()
        accounts = self._client.list_accounts()

        columns_mapper = {
            'name': 'id',
            'documentName': 'text',
            'accountId': 'account',
            'percentage': 'rate',
            'isGrossCalcType': 'inclusive',
        }

        vat_codes_mapped = vat_codes[list(columns_mapper.keys())].rename(columns=columns_mapper)
        vat_codes_mapped['inclusive'] = ~vat_codes_mapped['inclusive']
        vat_codes_mapped = pd.merge(vat_codes_mapped, accounts[['id', 'number']], left_on='account', right_on='id', how='left')
        vat_codes_mapped.drop(columns=['account', 'id_y'], inplace=True)
        vat_codes_mapped.rename(columns={'number': 'account', 'id_x': 'id'}, inplace=True)

        return StandaloneLedger.standardize_vat_codes(vat_codes_mapped)
    
    def mirror_vat_codes(self, target_state: pd.DataFrame, delete: bool = True):
        """
        Aligns VAT rates on the remote CashCtrl account with the desired state provided 
        as a DataFrame. This method standardizes VAT codes, matches them with existing 
        remote VAT codes, and performs necessary deletions, creations, and updates to 
        align with the target state.

        Parameters:
            target_state (pd.DataFrame): DataFrame containing desired VAT rates in 
                                        the format required by the system, typically 
                                        including fields like 'rate', 'account', and 
                                        'inclusive'.
            delete (bool, optional): If True, deletes VAT codes on the remote account 
                                    that are not present in the target_state DataFrame.
                                    Defaults to True.

        Returns:
            None: This method does not return any value but performs operations on the 
                remote system and handles exceptions internally.

        Raises:
            Exception: If there is an issue during any operation (deletion, creation, update).
        """
        current_remote_vat_codes = self.get_vat_codes()

        if (len(target_state) == 0 and len(current_remote_vat_codes) == 0):
            print("Nothing to change, both dataFrames are empty")
            return

        if (len(target_state) == 0 and delete):
            delete_ids = ",".join(current_remote_vat_codes['api_id'].astype(str))
            self.delete_vat_code(delete_ids)
            return
        elif (len(target_state) == 0 and not delete):
            delete_ids = ",".join(current_remote_vat_codes['api_id'].astype(str))
            print(f"These VATs should be deleted: {delete_ids}")
            return

        # Standardize VAT codes from input o match expected format
        standardized_vat_codes = StandaloneLedger.standardize_vat_codes(target_state)

        # Retrieve and merge account data from the remote system with VAT codes
        account_data = self._client.list_accounts()
        vat_with_accounts = current_remote_vat_codes.merge(
            account_data, left_on='accountId', right_on='id', how='left'
        )

        current_remote_vat_codes['account_number'] = vat_with_accounts['number']
        current_remote_vat_codes.reset_index(drop=True, inplace=True)
        current_remote_vat_codes.set_index('id', inplace=True)

        # Rename account id for clarity in merging
        account_data.rename(columns={'id': 'account_id'}, inplace=True)
        standardized_vat_codes.reset_index(drop=False, inplace=True)

        vat_codes_with_account_ids = standardized_vat_codes.merge(
            account_data, left_on='account', right_on='number', how='left'
        )
        standardized_vat_codes['account_id'] = vat_codes_with_account_ids['account_id']
        standardized_vat_codes.set_index('id', inplace=True)

        # Identify unique and duplicate VAT codes in remote data
        unique_entries_mask = ~current_remote_vat_codes.index.duplicated(keep='first')
        unique_vat_codes = current_remote_vat_codes[unique_entries_mask]
        duplicate_vat_codes = current_remote_vat_codes[~unique_entries_mask]

        # Determine missing and new VAT codes
        missing_in_target = ~unique_vat_codes.index.isin(standardized_vat_codes.index)
        deletable_vat_codes = unique_vat_codes[missing_in_target]
        vats_to_delete = pd.concat([duplicate_vat_codes, deletable_vat_codes])

        missing_in_remote = ~standardized_vat_codes.index.isin(unique_vat_codes.index)
        vats_to_create = standardized_vat_codes[missing_in_remote]

        existing_in_both = standardized_vat_codes.index.isin(unique_vat_codes.index)
        vats_to_update = standardized_vat_codes[existing_in_both]
        vats_to_update = vats_to_update.merge(
            unique_vat_codes[['api_id']], left_index=True, right_index=True, how='left'
        )

        delete_ids = ",".join(vats_to_delete['api_id'].astype(str))

        # Execute deletion, creation, and update operations
        if delete:
            try:
                self.delete_vat_code(delete_ids)
            except Exception as e:
                print(f"An error occurred deleting VAT codes: {e}")
        else:
            print("Warning! You have duplicated VAT codes.")

        for index, row in vats_to_create.iterrows():
            try:
                self.add_vat_code(
                    code=index, rate=row['rate'], account=row['account_id'],
                    inclusive=row['inclusive'], text=row['text']
                )
            except Exception as e:
                print(f"An error occurred creating VAT: {e}")

        for index, row in vats_to_update.iterrows():
            try:
                self.update_vat_code(
                    code=row['api_id'], rate=row['rate'], account=row['account_id'],
                    inclusive=row['inclusive'], text=row['text'], name=index
                )
            except Exception as e:
                print(f"An error occurred updating VAT: {e}")

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
            rate (float): The percentage rate of the VAT, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool): Determines whether the VAT is calculated as 'NET' 
                            (True, default) or 'GROSS' (False).
            text (str): Additional text or description associated with the VAT code.

        Raises:
            ValueError: If any of the inputs are invalid (wrong type or out of allowed range).
            Exception: If there is an issue with the server connection or the API request.
        """
        accounts = self._client.list_accounts()
        vat_account = accounts.loc[accounts['number'] == account].iloc[0]

        if not vat_account.empty:
            account = vat_account['id']
        else:
            account = None

        payload = {
            "name": code,
            "percentage": rate*100,
            "accountId": account,
            "calcType": "NET" if inclusive else "GROSS",
            "documentName": text,
        }

        if not isinstance(payload['name'], str) or len(payload['name']) > 50:
            raise ValueError(
                "Invalid code. It must be a string with a maximum of 50 characters."
            )

        if not isinstance(payload['percentage'], (int, float)) \
                or not (0.0 <= payload['percentage'] <= 100.0):
            raise ValueError(
                "Invalid rate. It must be a number between 0 and 1"
            )

        if not isinstance(payload['documentName'], str) \
                or len(payload['documentName']) > 50:
            raise ValueError(
                "Invalid text. It must be a string with a maximum of 50 characters."
            )

        self._client.post("tax/create.json", data=payload)
        
    def update_vat_code(
        self, code: str, rate: float, account: str,
        inclusive: bool = True, text: str = ""
    ):
        """
        Updates an existing VAT code in the CashCtrl account with new parameters. 

        Parameters:
            code (str): The VAT code to be updated.
            rate (float): The new percentage rate of the VAT, must be between 0 and 1.
            account (str): The account identifier to which the VAT is applied.
            inclusive (bool): Determines whether the VAT is calculated as 'NET' 
                            (True, default) or 'GROSS' (False).
            text (str): Additional text or description associated with the VAT code,
                        defaults to empty if not provided.

        Raises:
            ValueError: If any of the inputs are invalid (wrong type or out of allowed range).
        """

        accounts = self._client.list_accounts()
        vat_account = accounts.loc[accounts['number'] == account]

        if not vat_account.empty:
            account = vat_account['id'].iloc[0]
        else:
            account = None

        remote_vats = self._client.list_tax_rates()
        remote_vat = remote_vats.loc[remote_vats['name'] == code]
        remote_vat_id = remote_vat['id'].iloc[0] if not remote_vat.empty else None

        payload = {
            "id": remote_vat_id,
            "percentage": rate*100,
            "accountId": account,
            "calcType": "NET" if inclusive else "GROSS",
            "name": code,
            "documentName": text,
        }


        if not isinstance(payload['name'], str) or len(payload['name']) > 50:
            raise ValueError(
                "Invalid code. It must be a string with a maximum of 50 characters."
            )

        if not isinstance(payload['percentage'], (int, float)) \
                or not (0.0 <= payload['percentage'] <= 100.0):
            raise ValueError(
                "Invalid rate. It must be a number between 0 and 1"
            )

        if not isinstance(payload['documentName'], str) \
                or len(payload['documentName']) > 50:
            raise ValueError(
                "Invalid text. It must be a string with a maximum of 50 characters."
            )

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

    def delete_vat_code(self, code: str):
        """
        Deletes a VAT code from the remote CashCtrl account.

        Parameters:
        ----------
        code : str
            The VAT code name to be deleted.
        """

        remote_vats = self._client.list_tax_rates()
        remote_vat_to_delete = remote_vats.loc[remote_vats['name'] == code]
        delete_ids = ",".join(remote_vat_to_delete['id'].astype(str)) if not remote_vat_to_delete.empty else None

        self._client.post('tax/delete.json', {'ids': delete_ids})

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
