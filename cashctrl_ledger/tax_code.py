   def tax_codes(self) -> pd.DataFrame:
        """Retrieves tax codes from the remote CashCtrl account and converts to standard
        pyledger format.

        Returns:
            pd.DataFrame: A DataFrame with pyledger.TAX_CODE column schema.
        """
        tax_rates = self._client.list_tax_rates()
        accounts = self._client.list_accounts()
        account_map = accounts.set_index("id")["number"].to_dict()
        if not tax_rates["accountId"].isin(account_map).all():
            raise ValueError("Unknown 'accountId' in CashCtrl tax rates.")
        result = pd.DataFrame(
            {
                "id": tax_rates["name"],
                "description": tax_rates["documentName"],
                "account": tax_rates["accountId"].map(account_map),
                "rate": tax_rates["percentage"] / 100,
                "is_inclusive": ~tax_rates["isGrossCalcType"],
            }
        )

        duplicates = set(result.loc[result["id"].duplicated(), "id"])
        if duplicates:
            raise ValueError(
                f"Duplicated tax codes in the remote system: '{', '.join(map(str, duplicates))}'"
            )
        return StandaloneLedger.standardize_tax_codes(result)

    def add_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        description: str = "",
        is_inclusive: bool = True,
    ):
        """Adds a new tax code to the CashCtrl account.

        Args:
            id (str): The tax code to be added.
            rate (float): The tax rate, must be between 0 and 1.
            account (str): The account identifier to which the tax is applied.
            is_inclusive (bool, optional): Determines whether the tax is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            description (str, optional): Additional description associated with the tax code.
                                  Defaults to "".
        """
        payload = {
            "name": id,
            "percentage": rate * 100,
            "accountId": self._client.account_to_id(account),
            "documentName": description,
            "calcType": "NET" if is_inclusive else "GROSS",
        }
        self._client.post("tax/create.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def modify_tax_code(
        self,
        id: str,
        rate: float,
        account: str,
        description: str = "",
        is_inclusive: bool = True,
    ):
        """Updates an existing tax code in the CashCtrl account with new parameters.

        Args:
            id (str): The tax code to be updated.
            rate (float): The tax rate, must be between 0 and 1.
            account (str): The account identifier to which the tax is applied.
            is_inclusive (bool, optional): Determines whether the tax is calculated as 'NET'
                                        (True, default) or 'GROSS' (False). Defaults to True.
            description (str, optional): Additional description associated with the tax code.
                                  Defaults to "".
        """
        payload = {
            "id": self._client.tax_code_to_id(id),
            "percentage": rate * 100,
            "accountId": self._client.account_to_id(account),
            "calcType": "NET" if is_inclusive else "GROSS",
            "name": id,
            "documentName": description,
        }
        self._client.post("tax/update.json", data=payload)
        self._client.invalidate_tax_rates_cache()

    def delete_tax_codes(self, codes: List[str] = [], allow_missing: bool = False):
        ids = []
        for code in codes:
            id = self._client.tax_code_to_id(code, allow_missing=allow_missing)
            if id:
                ids.append(str(id))

        if len(ids):
            self._client.post("tax/delete.json", {"ids": ", ".join(ids)})
            self._client.invalidate_tax_rates_cache()
