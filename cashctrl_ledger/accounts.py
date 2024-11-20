def accounts(self) -> pd.DataFrame:
    """Retrieves the accounts from a remote CashCtrl instance,
    formatted to the pyledger schema.

    Returns:
        pd.DataFrame: A DataFrame with the accounts in pyledger format.
    """
    accounts = self._client.list_accounts()
    result = pd.DataFrame(
        {
            "account": accounts["number"],
            "currency": accounts["currencyCode"],
            "description": accounts["name"],
            "tax_code": accounts["taxName"],
            "group": accounts["path"],
        }
    )
    return self.standardize_accounts(result)

def add_account(
    self,
    account: str,
    currency: str,
    description: str,
    group: str,
    tax_code: Union[str, None] = None,
):
    """Adds a new account to the remote CashCtrl instance.

    Args:
        account (str): The account number or identifier to be added.
        currency (str): The currency associated with the account.
        description (str): Description associated with the account.
        group (str): The category group to which the account belongs.
        tax_code (str, optional): The tax code to be applied to the account, if any.
    """
    payload = {
        "number": account,
        "currencyId": self._client.currency_to_id(currency),
        "name": description,
        "taxId": None
        if pd.isna(tax_code)
        else self._client.tax_code_to_id(tax_code),
        "categoryId": self._client.account_category_to_id(group),
    }
    self._client.post("account/create.json", data=payload)
    self._client.invalidate_accounts_cache()

def modify_account(
    self,
    account: str,
    currency: str,
    description: str,
    group: str,
    tax_code: Union[str, None] = None,
):
    """Updates an existing account in the remote CashCtrl instance.

    Args:
        account (str): The account number or identifier to be added.
        currency (str): The currency associated with the account.
        description (str): Description associated with the account.
        group (str): The category group to which the account belongs.
        tax_code (str, optional): The tax code to be applied to the account, if any.
    """
    payload = {
        "id": self._client.account_to_id(account),
        "number": account,
        "currencyId": self._client.currency_to_id(currency),
        "name": description,
        "taxId": None
        if pd.isna(tax_code)
        else self._client.tax_code_to_id(tax_code),
        "categoryId": self._client.account_category_to_id(group),
    }
    self._client.post("account/update.json", data=payload)
    self._client.invalidate_accounts_cache()

def delete_accounts(self, accounts: List[int] = [], allow_missing: bool = False):
    ids = []
    for account in accounts:
        id = self._client.account_to_id(account, allow_missing)
        if id is not None:
            ids.append(str(id))
    if len(ids):
        self._client.post("account/delete.json", {"ids": ", ".join(ids)})
        self._client.invalidate_accounts_cache()

def mirror_accounts(self, target: pd.DataFrame, delete: bool = False):
    """Synchronizes remote CashCtrl accounts with a desired target state
    provided as a DataFrame.

    Updates existing categories before creating accounts and then invokes
    the parent class method.

    Args:
        target (pd.DataFrame): DataFrame with an account chart in the pyledger format.
        delete (bool, optional): If True, deletes accounts on the remote that are not
                                  present in the target DataFrame.
    """
    target_df = StandaloneLedger.standardize_accounts(target).reset_index()
    current_state = self.accounts().reset_index()

    # Delete superfluous accounts on remote
    if delete:
        self.delete_accounts(
            set(current_state["account"]).difference(set(target_df["account"]))
        )

    # Update account categories
    def get_nodes_list(path: str) -> List[str]:
        parts = path.strip("/").split("/")
        return ["/" + "/".join(parts[:i]) for i in range(1, len(parts) + 1)]

    def account_groups(df: pd.DataFrame) -> Dict[str, str]:
        if df is None or df.empty:
            return {}

        df = df.copy()
        df["nodes"] = [
            pd.DataFrame({"items": get_nodes_list(path)}) for path in df["group"]
        ]
        df = unnest(df, key="nodes")
        return df.groupby("items")["account"].agg("min").to_dict()

    self._client.update_categories(
        resource="account",
        target=account_groups(target),
        delete=delete,
        ignore_account_root_nodes=True,
    )
    super().mirror_accounts(target, delete)

def _single_account_balance(
    self, account: int, date: Union[datetime.date, None] = None
) -> dict:
    """Calculate the balance of a single account in both account currency
    and reporting currency.

    Args:
        account (int): The account number.
        date (datetime.date, optional): The date for the balance. Defaults to None,
            in which case the balance on the last day of the current fiscal period is returned.

    Returns:
        dict: A dictionary with the balance in the account currency and the reporting currency.
    """
    account_id = self._client.account_to_id(account)
    params = {"id": account_id, "date": date}
    response = self._client.request("GET", "account/balance", params=params)
    balance = float(response.text)

    account_currency = self._client.account_to_currency(account)
    if self.reporting_currency == account_currency:
        reporting_currency_balance = balance
    else:
        response = self._client.get(
            "fiscalperiod/exchangediff.json", params={"date": date}
        )
        exchange_diff = pd.DataFrame(response["data"])
        reporting_currency_balance = exchange_diff.loc[
            exchange_diff["accountId"] == account_id, "dcBalance"
        ].item()

    return {account_currency: balance, "reporting_currency": reporting_currency_balance}

