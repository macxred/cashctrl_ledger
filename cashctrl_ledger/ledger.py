"""
Module to sync ledger system onto CashCtrl.
"""

import pandas as pd
from pyledger import LedgerEngine
from cashctrl_api import CashCtrlClient

class CashCtrlLedger(LedgerEngine):
    """
    Class that give you an ability to sync ledger system onto CashCtrl

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    def __init__(self, client: CashCtrlClient | None = None):
        super().__init__()
        self.client = CashCtrlClient() if client is None else client

    @property
    def client(self):
        """The getter method that returns the CashCtrlClient instance."""
        return self._client

    @client.setter
    def client(self, value):
        """
        The setter method that sets the value of tne CashCtrlClient 
        instance. Validate value to be a CashCtrlClient instance.
        """
        if isinstance(value, CashCtrlClient):
            self._client = value
        else:
            raise ValueError("Value must be an instance of CashCtrlClient class")

    def get_vat_codes(self) -> pd.DataFrame:
        return pd.DataFrame(self.client.get("tax/list.json")['data'])

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
        self, accountId: int, name: str, percentage: float, percentageFlat: float, 
        calcType: str = "NET", documentName: str = "", isInactive: bool = False
    ):
        """Adds a new VAT code to the CashCtrl account.
        
        Parameters:
        - accountId: int, mandatory, ID of the account which collects taxes.
        - name: str, mandatory, description of the tax rate, max 50 chars.
        - percentage: float, mandatory, tax rate percentage between 0.0 and 100.0.
        - percentageFlat: float, optional, flat tax rate percentage between 0.0 and 100.0.
        - calcType: str, optional, basis of tax rate calculation ('NET' or 'GROSS').
        - documentName: str, optional, name for the tax rate on documents, max 50 chars.
        - isInactive: bool, optional, marks the tax rate as inactive if True.
        """
        # Validate input parameters
        if not isinstance(accountId, int) or accountId <= 0:
            raise ValueError("Invalid accountId. It must be a positive integer.")

        if not isinstance(name, str) or len(name) > 50:
            raise ValueError(
                "Invalid name. It must be a string with a maximum of 50 characters."
            )

        if not isinstance(percentage, (int, float)) or not (0.0 <= percentage <= 100.0):
            raise ValueError(
                "Invalid percentage. It must be a number between 0.0 and 100.0."
            )

        if not isinstance(percentageFlat, (int, float)) or not (0.0 <= percentageFlat <= 100.0):
            raise ValueError(
                "Invalid percentageFlat. It must be a number between 0.0 and 100.0."
            )

        if calcType not in ["NET", "GROSS"]:
            raise ValueError("Invalid calcType. It must be either 'NET' or 'GROSS'.")

        if not isinstance(documentName, str) or len(documentName) > 50:
            raise ValueError(
                "Invalid documentName. It must be a string with a maximum of 50 characters."
            )

        if not isinstance(isInactive, bool):
            raise ValueError("Invalid isInactive flag. It must be a boolean.")

        # Prepare payload for the API request
        payload = {
            "accountId": accountId,
            "name": name,
            "percentage": percentage,
            "percentageFlat": percentageFlat,
            "calcType": calcType,
            "documentName": documentName or name,
            "isInactive": isInactive,
        }

        try:
            self.client.post("tax/create.json", data=payload)
        except Exception as e:
            raise Exception(
                f"An error occurred while posting data to the server: {e}"
            )
        
    def update_vat_code(
        self, accountId: int, id: int, name: str, percentage: float,
        documentName: str = None, isInactive: bool = False,
        calcType: str = "NET", percentageFlat: float = None
    ):
        """
        Updates an existing tax rate with given parameters. If a tax rate is already in use,
        some parameters like 'percentage' and 'percentageFlat' cannot be changed.

        Parameters:
        - accountId (int): ID of the account collecting taxes.
        - id (int): ID of the tax rate to be updated.
        - name (str): Name describing the tax rate, max 50 characters.
        - percentage (float): Tax rate percentage between 0.0 and 100.0.
        - documentName (str): Name displayed on documents, defaults to 'name' if None.
        - isInactive (bool): If True, marks the tax rate as inactive.
        - calcType (str): Basis of tax calculation, 'NET' or 'GROSS', defaults to 'NET'.
        - percentageFlat (float): Flat tax rate percentage, optional.

        Returns:
        - None

        Raises:
        - ValueError: If input values are out of allowed range or missing mandatory fields.
        - Exception: For issues during data posting to the server.
        """

        # Validation
        if len(name) > 50 or (documentName and len(documentName) > 50):
            raise ValueError("Name and documentName must be 50 characters or less.")
        if not (0.0 <= percentage <= 100.0):
            raise ValueError("Percentage must be between 0.0 and 100.0.")
        if percentageFlat is not None and not (0.0 <= percentageFlat <= 100.0):
            raise ValueError("PercentageFlat must be between 0.0 and 100.0.")
        if isInactive and not isinstance(isInactive, bool):
            raise ValueError("isInactive must be a boolean")
        if isInactive and not isinstance(isInactive, bool):
            raise ValueError("isInactive must be a boolean")

        payload = {
            "accountId": accountId,
            "id": id,
            "name": name,
            "percentage": percentage,
            "documentName": documentName or name,
            "isInactive": isInactive,
            "calcType": calcType,
        }

        if percentageFlat is not None:
            payload['percentageFlat'] = percentageFlat

        try:
            self.client.post("tax/update.json", data=payload)
        except Exception as e:
            raise Exception(
                f"An error occurred while posting data to the server: {e}"
            )


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
        Deletes a VAT code from the tax system via a POST request to the API.

        Sends a POST request to the 'tax/delete.json' endpoint to delete the VAT code
        specified by the 'code' parameter. The 'code' should be a string identifier
        for the VAT code.

        Parameters:
        ----------
        code : str
            The identifier for the VAT code to be deleted.

        Raises:
        ------
        Exception
            An exception is raised with a detailed error message if the API request fails.

        Returns:
        -------
        None
            This method does not return any value but will raise an exception if unsuccessful.
        """
        try:
            self.client.post('tax/delete.json', {'ids': code})
        except Exception as e:
            raise Exception(f"An error occurred: {e}")


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

    def vat_codes():
        """
        Not implemented yet
        """
        raise NotImplementedError