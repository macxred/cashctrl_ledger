"""Module that implements the pyledger interface by connecting to the CashCtrl API."""

import datetime
from typing import Union
from cashctrl_api import CachedCashCtrlClient
import pandas as pd
import zipfile
import json
from pyledger import LedgerEngine
from .constants import SETTINGS_KEYS


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
        self._client = CachedCashCtrlClient() if client is None else client

    # ----------------------------------------------------------------------
    # File operations

    def dump_to_zip(self, archive_path: str):
        with zipfile.ZipFile(archive_path, 'w') as archive:
            settings = {"REPORTING_CURRENCY": self.reporting_currency}

            roundings = self._client.get("rounding/list.json")["data"]
            for rounding in roundings:
                rounding["account"] = self._client.account_from_id(rounding["accountId"])
                rounding.pop("accountId")
            settings["DEFAULT_ROUNDINGS"] = roundings

            default_settings = {}
            system_settings = self._client.get("setting/read.json")
            for key in SETTINGS_KEYS:
                if key in system_settings:
                    default_settings[key] = self._client.account_from_id(system_settings[key])
            settings["DEFAULT_SETTINGS"] = default_settings

            archive.writestr('settings.yml', json.dumps(settings))

    def restore_from_zip(self, archive_path: str):
        required_files = {'settings.yml'}

        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive_files = set(archive.namelist())
            missing_files = required_files - archive_files
            if missing_files:
                raise FileNotFoundError(
                    f"Missing required files in the archive: {', '.join(missing_files)}"
                )

            settings = json.loads(archive.open('settings.yml').read().decode('utf-8'))
            self.restore(
                settings=settings,
            )

    def restore(self, settings: dict | None = None):
        self.clear()
        if settings is not None:
            roundings = settings.get("DEFAULT_ROUNDINGS", None)
            reporting_currency = settings.get("REPORTING_CURRENCY", None)
            system_settings = settings.get("DEFAULT_SETTINGS", None)
        else:
            roundings = None
            reporting_currency = None
            system_settings = None

        if reporting_currency is not None:
            self.reporting_currency = reporting_currency
        if system_settings is not None:
            for key in SETTINGS_KEYS:
                if key in system_settings:
                    system_settings[key] = self._client.account_to_id(system_settings[key])
            self._client.post("setting/update.json", data=system_settings)
        if roundings is not None:
            for rounding in roundings:
                rounding["accountId"] = self._client.account_to_id(rounding["account"])
                self._client.post("rounding/create.json", data=rounding)
        # TODO: Implement price history, precision settings,
        # and FX adjustments restoration logic

    def clear(self):
        # Clear default System settings
        empty_settings = {key: "" for key in SETTINGS_KEYS}
        self._client.post("setting/update.json", empty_settings)
        roundings = self._client.get("rounding/list.json")["data"]
        if len(roundings):
            ids = ','.join(str(item['id']) for item in roundings)
            self._client.post("rounding/delete.json", data={"ids": ids})
        # TODO: Implement price history, precision settings, and FX adjustments clearing logic

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
