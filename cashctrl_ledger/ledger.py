"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

import datetime
import json
from typing import Union
import zipfile
from cashctrl_api import CachedCashCtrlClient
import pandas as pd
from .tax_code import TaxCode
from .accounts import Account
from pyledger import LedgerEngine
from .constants import SETTINGS_KEYS
from pyledger.constants import TAX_CODE_SCHEMA, ACCOUNT_SCHEMA


class CashCtrlLedger(LedgerEngine):
    """Class that Implements the pyledger interface by connecting
    to the CashCtrl online accounting software.

    See README on https://github.com/macxred/cashctrl_ledger for overview and
    usage examples.
    """

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, client: Union[CachedCashCtrlClient, None] = None):
        super().__init__()
        client = CachedCashCtrlClient() if client is None else client
        self._client = client
        self._tax_codes = TaxCode(client=client, schema=TAX_CODE_SCHEMA)
        self._accounts = Account(client=client, schema=ACCOUNT_SCHEMA)

    # ----------------------------------------------------------------------
    # File operations

    def dump_to_zip(self, archive_path: str):
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('settings.json', json.dumps(self.settings_list()))
            archive.writestr('tax_codes.csv', self.tax_codes.list().to_csv(index=False))
            archive.writestr('accounts.csv', self.accounts.list().to_csv(index=False))

    def restore_from_zip(self, archive_path: str):
        required_files = {'tax_codes.csv', 'accounts.csv', 'settings.json'}

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            settings = json.loads(archive.open('settings.json').read().decode('utf-8'))
            accounts = pd.read_csv(archive.open('accounts.csv'))
            tax_codes = pd.read_csv(archive.open('tax_codes.csv'))
            self.restore(
                settings=settings,
                tax_codes=tax_codes,
                accounts=accounts,
            )

    def restore(
        self,
        settings: dict | None = None,
        tax_codes: pd.DataFrame | None = None,
        accounts: pd.DataFrame | None = None,
    ):
        if accounts is not None:
            self.accounts.mirror(accounts.assign(tax_code=pd.NA), delete=True)
        if tax_codes is not None:
            self.tax_codes.mirror(tax_codes, delete=True)
        if accounts is not None:
            self.accounts.mirror(accounts, delete=True)
        if settings is not None:
            self.settings_modify(settings)
        # TODO: Implement logic for other entities

    def clear(self):
        self.settings_clear()

        # Manually reset accounts tax to none
        accounts = self.accounts.list()
        self.accounts.mirror(accounts.assign(tax_code=pd.NA))
        self.tax_codes.mirror(None, delete=True)
        self.accounts.mirror(None, delete=True)
        # TODO: Implement logic for other entities

    # ----------------------------------------------------------------------
    # Settings

    def settings_list(self) -> dict:
        roundings = self._client.get("rounding/list.json")["data"]
        for rounding in roundings:
            rounding["account"] = self._client.account_from_id(rounding["accountId"])
            rounding.pop("accountId")

        system_settings = self._client.get("setting/read.json")
        cash_ctrl_settings = {
            key: self._client.account_from_id(system_settings[key])
            for key in SETTINGS_KEYS if key in system_settings
        }

        return {
            "REPORTING_CURRENCY": self.reporting_currency,
            "ROUNDING": roundings,
            "CASH_CTRL": cash_ctrl_settings
        }

    def settings_modify(self, settings: dict = {}):
        if "REPORTING_CURRENCY" in settings:
            self.reporting_currency = settings["REPORTING_CURRENCY"]

        if "ROUNDING" in settings:
            for rounding in settings["ROUNDING"]:
                rounding["accountId"] = self._client.account_to_id(rounding["account"])
                self._client.post("rounding/create.json", data=rounding)

        if "CASH_CTRL" in settings:
            system_settings = {
                key: self._client.account_to_id(settings["CASH_CTRL"][key])
                for key in SETTINGS_KEYS if key in settings["CASH_CTRL"]
            }
            self._client.post("setting/update.json", data=system_settings)

    def settings_clear(self):
        empty_settings = {key: "" for key in SETTINGS_KEYS}
        self._client.post("setting/update.json", empty_settings)
        roundings = self._client.get("rounding/list.json")["data"]
        if len(roundings):
            ids = ','.join(str(item['id']) for item in roundings)
            self._client.post("rounding/delete.json", data={"ids": ids})

    # ----------------------------------------------------------------------
    # Accounts

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

    # ----------------------------------------------------------------------
    # Currencies

    @property
    def reporting_currency(self) -> str:
        """Returns the reporting currency of the CashCtrl account.

        Returns:
            str: The reporting currency code.
        """
        currencies = self._client.list_currencies()
        is_reporting_currency = currencies["isDefault"].astype("bool")
        if is_reporting_currency.sum() == 1:
            return currencies.loc[is_reporting_currency, "code"].item()
        elif is_reporting_currency.sum() == 0:
            raise ValueError("No reporting currency set.")
        else:
            raise ValueError("Multiple reporting currencies defined.")

    @reporting_currency.setter
    def reporting_currency(self, currency):
        # TODO: Perform testing of this method after restore() for currencies implemented
        currencies = self._client.list_currencies()
        if currency in set(currencies["code"]):
            target_currency = currencies[currencies["code"] == currency].iloc[0]
            payload = {
                "id": target_currency["id"],
                "code": currency,
                "isDefault": True,
                "description": target_currency["description"],
                "rate": target_currency["rate"]
            }
            self._client.post("currency/update.json", data=payload)
        else:
            payload = {
                "code": currency,
                "isDefault": True,
                "description": "Reporting Currency",
                "rate": 1
            }
            self._client.post("currency/create.json", data=payload)

        self._client.invalidate_currencies_cache()

    def precision(self, ticker: str, date: datetime.date = None) -> float:
        return self._precision.get(ticker, 0.01)

    def price(self, currency: str, date: datetime.date = None) -> float:
        """
        Retrieves the price (exchange rate) of a given currency in terms
        of the reporting currency.

        Args:
            currency (str): The currency code to retrieve the price for.
            date (datetime.date, optional): The date for which the price is
                requested. Defaults to None, which retrieves the latest price.

        Returns:
            float: The exchange rate between the currency and the reporting currency.
        """
        return self._client.get_exchange_rate(
            from_currency=currency,
            to_currency=self.reporting_currency,
            date=date
        )
