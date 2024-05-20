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

        duplicates = set(result.loc[result['id'].duplicated(), 'id'])
        if duplicates:
            raise ValueError(
                f"Duplicated VAT codes in the remote system: '{', '.join(map(str, duplicates))}'"
            )
        return StandaloneLedger.standardize_vat_codes(result)

    def mirror_vat_codes(self, target_state: pd.DataFrame, delete: bool = True):
        """
        Aligns VAT rates on the remote CashCtrl account with the desired state provided as a DataFrame.

        Parameters:
            target_state (pd.DataFrame): DataFrame containing VAT rates in the pyledger.vat_codes format.
            delete (bool, optional): If True, deletes VAT codes on the remote account that are not present in target_state.
        """
        target_df = StandaloneLedger.standardize_vat_codes(target_state).reset_index()
        current_state = self.vat_codes().reset_index()

        # Delete superfluous VAT codes on remote
        if delete:
            for idx in set(current_state['id']).difference(set(target_df['id'])):
                self.delete_vat_code(code=idx)

        # Create new VAT codes on remote
        ids = set(target_df['id']).difference(set(current_state['id']))
        to_add = target_df.loc[target_df['id'].isin(ids)]
        for row in to_add.to_dict('records'):
            self.add_vat_code(
                code=row['id'],
                text=row['text'],
                account=row['account'],
                rate=row['rate'],
                inclusive=row['inclusive'],
            )

        # Update modified VAT cods on remote
        both = set(target_df['id']).intersection(set(current_state['id']))
        l = target_df.loc[target_df['id'].isin(both)]
        r = current_state.loc[current_state['id'].isin(both)]
        merged = pd.merge(l, r, how="outer", indicator=True)
        to_update = merged[merged['_merge'] == 'left_only']
        for row in to_update.to_dict('records'):
            self.update_vat_code(
                code=row['id'],
                text=row['text'],
                account=row['account'],
                rate=row['rate'],
                inclusive=row['inclusive'],
            )

    def _single_account_balance():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def account_chart(self) -> pd.DataFrame:
        """
        Retrieves the account chart from a remote CashCtrl instance, formatted to the pyledger schema.
        """
        accounts = self._client.list_accounts()
        result = pd.DataFrame({
            'account': accounts['number'],
            'currency': accounts['currencyCode'],
            'text': accounts['name'],
            'vat_code': accounts['taxName'],
            'group': accounts['path'],
        })
        return StandaloneLedger.standardize_account_chart(result)

    def mirror_account_chart(self, target: pd.DataFrame, delete: bool = True):
        """
        Synchronizes remote CashCtrl accounts with a desired target state provided as a DataFrame.
        Args:
            target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
            delete (bool, optional): If True, deletes accounts on the remote that are not present in the target DataFrame.
        """
        target_df = StandaloneLedger.standardize_account_chart(target).reset_index()
        current_state = self.account_chart().reset_index()

        # Delete superfluous accounts on remote
        if delete:
            for account in set(current_state['account']).difference(set(target_df['account'])):
                self.delete_account(account=account)

        # Create new accounts on remote
        accounts = set(target_df['account']).difference(set(current_state['account']))
        to_add = target_df.loc[target_df['account'].isin(accounts)]

        for row in to_add.to_dict('records'):
            self.add_account(
                account=row['account'],
                currency=row['currency'],
                text=row['text'],
                vat_code=row['vat_code'],
                group=row['group'],
            )

        # Update modified accounts on remote
        both = set(target_df['account']).intersection(set(current_state['account']))
        l = target_df.loc[target_df['account'].isin(both)]
        r = current_state.loc[current_state['account'].isin(both)]
        merged = pd.merge(l, r, how="outer", indicator=True)
        to_update = merged[merged['_merge'] == 'left_only']

        for row in to_update.to_dict('records'):
            self.update_account(
                account=row['account'],
                currency=row['currency'],
                text=row['text'],
                vat_code=row['vat_code'],
                group=row['group'],
            )

    def add_account(self, account: str, currency: str, text: str, group: str, vat_code: str | None = None):
        """
        Adds a new account to the remote CashCtrl instance.

        Parameters:
            account (str): The account number or identifier to be added.
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """

        currencies = pd.DataFrame(self._client.get("currency/list.json")['data'])
        currency_map = currencies.set_index('text')['id'].to_dict()
        if currency not in currency_map:
            raise ValueError(f"Currency '{currency}' does not exist.")
        currency_id = currency_map[currency]

        tax_id = None
        if vat_code is not None:
            tax_data = self._client.list_tax_rates()
            tax_map = tax_data.set_index('text')['id'].to_dict()
            if vat_code not in tax_map:
                raise ValueError(f"VAT code '{vat_code}' does not exist.")
            tax_id = tax_map[vat_code]

        categories = self._client.list_categories('account')
        categories_map = categories.set_index('path')['id'].to_dict()
        if group not in categories_map:
            raise ValueError(f"Group '{group}' does not exist.")
        category_id = categories_map[group]

        payload = {
            "number": account,
            "currencyId": currency_id,
            "name": text,
            "taxId": tax_id,
            "categoryId": category_id,
        }

        self._client.post("account/create.json", data=payload)

    def update_account(self, account: str, currency: str, text: str, group: str, vat_code: str | None = None):
        """
        Updates an existing account in the remote CashCtrl instance.

        Parameters:
            account (str): The account number or identifier to be added.
            currency (str): The currency associated with the account.
            text (str): Additional text or description associated with the account.
            group (str): The category group to which the account belongs.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
        """

        accounts = self._client.list_accounts()
        account_map = accounts.set_index('number')['id'].to_dict()
        if account not in account_map:
            raise ValueError(f"Account '{account}' does not exist.")

        currencies = pd.DataFrame(self._client.get("currency/list.json")['data'])
        currency_map = currencies.set_index('text')['id'].to_dict()
        if currency not in currency_map:
            raise ValueError(f"Currency '{currency}' does not exist.")
        currency_id = currency_map[currency]

        tax_id = None
        if vat_code is not None:
            tax_data = self._client.list_tax_rates()
            tax_map = tax_data.set_index('text')['id'].to_dict()
            if vat_code not in tax_map:
                raise ValueError(f"VAT code '{vat_code}' does not exist.")
            tax_id = tax_map[vat_code]

        categories = self._client.list_categories('account')
        categories_map = categories.set_index('path')['id'].to_dict()
        if group not in categories_map:
            raise ValueError(f"Group '{group}' does not exist.")
        category_id = categories_map[group]


        payload = {
            "id": account_map[account],
            "number": account,
            "currencyId": currency_id,
            "name": text,
            "taxId": tax_id,
            "categoryId": category_id,
        }

        self._client.post("account/update.json", data=payload)

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

    def add_ledger_entry(
        self,
        date: str,
        account: str,
        counter_account: str,
        amount: float,
        currency: str,
        text: str,
        vat_code: str,
        document: str
    ):
        """
        Adds a new ledger entry to the remote CashCtrl instance.

        Parameters:
            date (str): The date ledger entry was created.
            account (str): The account on the credit side.
            counter_account (str): The account on the debit side.
            amount (float): The amount of the book entry.
            currency (str): The currency associated with the account.
            text (str, optional): The description of the book entry.
            vat_code (str, optional): The VAT code to be applied to the account, if any.
            document (str, optional): reference for the book entry.
        """

        accounts = self._client.list_accounts()
        account_map = accounts.set_index('number')['id'].to_dict()
        if account not in account_map or counter_account not in account_map:
            raise ValueError(f"Account '{account}' does not exist.")
        elif counter_account not in account_map:
            raise ValueError(f"Account '{counter_account}' does not exist.")

        currencies = pd.DataFrame(self._client.get("currency/list.json")['data'])
        currency_map = currencies.set_index('text')['id'].to_dict()
        if currency not in currency_map:
            raise ValueError(f"Currency '{currency}' does not exist.")
        currency_id = currency_map[currency]

        tax_id = None
        if vat_code is not None:
            tax_data = self._client.list_tax_rates()
            tax_map = tax_data.set_index('text')['id'].to_dict()
            if vat_code not in tax_map:
                raise ValueError(f"VAT code '{vat_code}' does not exist.")
            tax_id = tax_map[vat_code]

        # Create remote journal record
        payload = {
            "dateAdded": date,
            "amount": amount,
            "debitId": account_map[account],
            "creditId": account_map[counter_account],
            "currencyId": currency_id,
            "title": text,
            "taxId": tax_id,
            "reference": document,
        }

        self._client.post("journal/create.json", data=payload)

    def base_currency():
        """
        Not implemented yet
        """
        raise NotImplementedError

    def delete_account(self, account: str, allow_missing: bool = False):
        """Deletes an account from the remote CashCtrl instance."""
        accounts = self._client.list_accounts()
        to_delete = accounts.loc[accounts['number'] == account, 'id']

        if len(to_delete) > 0:
            delete_ids = ",".join(to_delete.astype(str))
            self._client.post('account/delete.json', {'ids': delete_ids})
        elif not allow_missing:
            raise ValueError(f"There is no Account '{account}'.")

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

    def ledger(self) -> pd.DataFrame:
        """
        Retrieves the account remote ledger from CashCtrl instance formatted to the pyledger schema.

        Returns:
            pd.DataFrame: A DataFrame with enforced data types.
        """
        ledger = self._client.list_journal_entries()
        tax_rates = self._client.list_tax_rates()
        rates_map = tax_rates.set_index('id')['name'].to_dict()
        accounts = self._client.list_accounts()
        account_map = accounts.set_index('id')['number'].to_dict()
        currencies = pd.DataFrame(self._client.get("currency/list.json")['data'])
        currency_map = currencies.set_index('text')['id'].to_dict()

        result = pd.DataFrame({
            'date': ledger['dateAdded'],
            'account': ledger.apply(
                lambda row: account_map[row['creditId']]
                if row['amount'] > 0
                else account_map[row['debitId']], axis=1
            ),
            'counter_account': ledger.apply(
                lambda row: account_map[row['debitId']]
                if row['amount'] > 0
                else account_map[row['creditId']], axis=1
            ),
            'amount': ledger['amount'],
            'currency': ledger['currencyCode'].map(currency_map),
            'text': ledger['title'],
            'vat_code': ledger['taxId'].map(rates_map),
            'document': ledger['reference'],
        })

        return StandaloneLedger.standardize_ledger(result)

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
