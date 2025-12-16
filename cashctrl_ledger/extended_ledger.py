"""Extends CashCtrlLedger to handle transactions incompatible with CashCtrl."""

import pandas as pd
from .ledger import CashCtrlLedger


class ExtendedCashCtrlLedger(CashCtrlLedger):
    """Handles CashCtrl limitations: 8-digit FX precision, single currency per transaction.

    Splits multi-currency transactions and creates compensating entries via a transitory
    account to preserve financial accuracy. See docs/CASHCTRL_FX_CONSTRAINTS.md for details.
    """

    _transitory_account = None

    # ----------------------------------------------------------------------
    # Constructor

    def __init__(self, transitory_account: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transitory_account = transitory_account

    def clear(self):
        transitory_account = self._client.account_to_id(
            self._transitory_account, allow_missing=True
        )
        if transitory_account is None:
            self.accounts.add([{
                "account": self._transitory_account,
                "currency": self.reporting_currency,
                "description": "temp transitory account",
                "tax_code": None,
                "group": "/Assets",
            }])
        super().clear()

    # ----------------------------------------------------------------------
    # Accounts

    @property
    def transitory_account(self) -> int:
        """Account for residual amounts when splitting complex transactions."""
        if self._transitory_account is None:
            raise ValueError("transitory_account is not set.")
        if self._transitory_account not in set(
            self._client.list_accounts()["number"]
        ):
            raise ValueError(
                f"The transitory account {self._transitory_account} does not exist."
            )
        account_currency = self._client.account_to_currency(self._transitory_account)
        if account_currency != self.reporting_currency:
            raise ValueError(
                f"The transitory account {self._transitory_account} must be "
                f"denominated in {self.reporting_currency} reporting currency, not "
                f"{account_currency}."
            )
        return self._transitory_account

    @transitory_account.setter
    def transitory_account(self, value: int):
        if value not in set(self._client.list_accounts()["number"]):
            self.accounts.add([{
                "account": value,
                "tax_code": None,
                "group": "/Assets",
                "description": "Transitory account",
                "currency": self.reporting_currency
            }])
        self._transitory_account = value

    # ----------------------------------------------------------------------
    # Journal

    def sanitize_journal(self, journal: pd.DataFrame) -> pd.DataFrame:
        """Prepare journal for CashCtrl by splitting multi-currency transactions
        and adding FX adjustment entries to handle 8-digit precision limits.
        """
        journal = self.journal.standardize(journal)

        if len(journal) == 0:
            return journal

        # Insert missing base currency amounts
        mask = journal['report_amount'].isna()
        journal.loc[mask, 'report_amount'] = self.report_amount(
            amount=journal.loc[mask, 'amount'],
            currency=journal.loc[mask, 'currency'],
            date=journal.loc[mask, 'date']
        )

        # Number of currencies other than reporting currency
        reporting_currency = self.reporting_currency
        n_currency = journal[["id", "currency"]][journal["currency"] != reporting_currency]
        n_currency = n_currency.groupby("id")["currency"].nunique()

        # Split entries with multiple currencies into separate entries for each currency
        ids = n_currency.index[n_currency > 1]
        if len(ids) > 0:
            multi_currency = self.journal.standardize(journal[journal["id"].isin(ids)])
            multi_currency = self.split_multi_currency_transactions(multi_currency)
            others = journal[~journal["id"].isin(ids)]
            df = pd.concat([others, multi_currency], ignore_index=True)
        else:
            df = journal

        # Ensure foreign currencies can be mapped, correct with FX adjustments otherwise
        transitory_account = self.transitory_account
        result = []
        for _, txn in df.groupby("id"):
            new_txn = self._add_fx_adjustment(
                txn, transitory_account=transitory_account, reporting_currency=reporting_currency
            )
            result.append(self.journal.standardize(new_txn))
        if len(result) > 0:
            result = pd.concat(result)
        else:
            result = df

        # Drop internal fx_rate column - not part of journal schema
        if "fx_rate" in result.columns:
            result = result.drop(columns=["fx_rate"])

        return result

    def split_multi_currency_transactions(self, journal: pd.DataFrame,
                                          transitory_account: int | None = None) -> pd.DataFrame:
        """Split multi-currency transactions into one transaction per currency.

        CashCtrl allows only one foreign currency per collective transaction. This splits
        them and uses transitory_account for clearing entries that balance to zero overall.
        """
        reporting_currency = self.reporting_currency
        is_reporting_currency = journal["currency"] == reporting_currency
        journal.loc[is_reporting_currency, "report_amount"] = journal.loc[
            is_reporting_currency, "amount"
        ]

        if any(journal["report_amount"].isna()):
            raise ValueError("Reporting currency amount missing for some items.")
        if transitory_account is None:
            transitory_account = self.transitory_account

        result = []
        for (id, currency), group in journal.groupby(["id", "currency"]):
            sub_id = f"{id}:{currency}"
            result.append(group.assign(id=sub_id))
            balance = self.round_to_precision(group["report_amount"].sum(), reporting_currency)
            if balance != 0:
                clearing_txn = pd.DataFrame(
                    {
                        "id": [sub_id],
                        "description": [
                            "Split multi-currency transaction "
                            "into multiple transactions compatible with CashCtrl."
                        ],
                        "amount": [-1 * balance],
                        "report_amount": [-1 * balance],
                        "currency": [reporting_currency],
                        "account": [transitory_account],
                    }
                )
                result.append(clearing_txn)

        result = pd.concat(result, ignore_index=True)
        return self.journal.standardize(result)

    def _add_fx_adjustment(
        self, entry: pd.DataFrame, transitory_account: int, reporting_currency: str
    ) -> pd.DataFrame:
        """Adjust entries for CashCtrl's 8-digit FX precision and single-rate constraint.

        Creates :rounding entries to compensate for differences between desired and
        actual report_amounts after CashCtrl's recalculation (amount * fx_rate).
        """
        if len(entry) == 1:
            # Individual transaction: one row in the journal data frame
            if (
                entry["amount"].item() == 0
                or entry["currency"].item() == reporting_currency
            ):
                return entry
            else:
                amount = self.round_to_precision(entry["amount"].item(), entry["currency"].item())
                reporting_amount = self.round_to_precision(
                    entry["report_amount"].item(), reporting_currency
                )
                fx_rate = round(reporting_amount / amount, 8)
                balance = reporting_amount - self.round_to_precision(
                    amount * fx_rate, reporting_currency
                )
                entry["fx_rate"] = fx_rate
                if balance == 0.0:
                    return entry
                else:
                    balancing_txn = entry.copy()
                    balancing_txn["id"] = balancing_txn["id"] + ":fx"
                    balancing_txn["currency"] = entry["currency"].item()
                    balancing_txn["amount"] = 0
                    balancing_txn["report_amount"] = balance
                    entry["report_amount"] = (
                        entry["report_amount"] - balance
                    )
                    result = pd.concat(
                        [
                            self.journal.standardize(entry),
                            self.journal.standardize(balancing_txn),
                        ]
                    )
                    result["amount"] = self.round_to_precision(
                        result["amount"], result["currency"]
                    )
                    result["report_amount"] = self.round_to_precision(
                        result["report_amount"], reporting_currency
                    )
                    return self.journal.standardize(result)

        elif len(entry) > 1:
            # Collective transaction: CashCtrl uses single FX rate, recalculates all report_amounts
            currency, fx_rate = self._collective_transaction_currency_and_rate(entry)
            fx_rate = round(fx_rate, 8)
            self._sanitized_fx_rates[entry["id"].iloc[0]] = currency, fx_rate
            if currency == reporting_currency:
                return entry

            # Step 1: Save original desired report_amounts for foreign accounts
            mask = (entry["currency"] == reporting_currency) & entry["report_amount"].isna()
            entry.loc[mask, "report_amount"] = entry.loc[mask, "amount"]
            entry["report_amount"] = self.round_to_precision(
                entry["report_amount"], reporting_currency,
            )
            if entry["report_amount"].isna().any():
                raise ValueError("Reporting currency missing.")

            original_reports = entry[entry["account"] != transitory_account].copy()

            # Step 2: Convert to foreign currency with smart splitting (balances both currencies)
            entry = self._smart_convert_to_foreign(
                entry, fx_rate, currency, transitory_account, reporting_currency
            )

            # Step 3: Calculate what CashCtrl will ACTUALLY store
            entry["report_amount"] = self.round_to_precision(
                entry["amount"] * fx_rate, reporting_currency
            )

            result = self.journal.standardize(entry)

            # Step 4: Calculate rounding corrections for each foreign account
            currency_precision = self.precision_vectorized(
                [reporting_currency], [entry["date"].iloc[0]]
            )[0]
            rounding_entries = []
            accounts_df = self.accounts.list()

            for account in original_reports["account"].unique():
                if pd.isna(account) or account == transitory_account:
                    continue

                orig_mask = original_reports["account"] == account
                original_sum = original_reports.loc[orig_mask, "report_amount"].sum()

                cashctrl_mask = entry["account"] == account
                cashctrl_sum = entry.loc[cashctrl_mask, "report_amount"].sum()

                rounding_error = self.round_to_precision(
                    cashctrl_sum - original_sum, reporting_currency
                )

                if abs(rounding_error) > currency_precision / 2:
                    adjustment = self.round_to_precision(-rounding_error, reporting_currency)
                    rounding_id = entry["id"].iloc[0] + ":rounding"
                    rounding_date = entry["date"].iloc[0]

                    # Get account's native currency to avoid fallback issues
                    account_currency = accounts_df.loc[
                        accounts_df["account"] == account, "currency"
                    ].iloc[0]

                    if account_currency == reporting_currency:
                        # Reporting currency account - use USD directly
                        rounding_currency = reporting_currency
                        rounding_amount = adjustment
                    else:
                        # Foreign currency account - convert adjustment to account's currency
                        # Use the transaction's FX rate
                        rounding_currency = currency
                        foreign_precision = self.precision_vectorized(
                            [currency], [entry["date"].iloc[0]]
                        )[0]
                        rounding_amount = self.round_to_precision(
                            adjustment / fx_rate, currency
                        )
                        # Ensure the converted amount gives correct report_amount
                        if rounding_amount == 0 and adjustment != 0:
                            sign = 1 if adjustment > 0 else -1
                            rounding_amount = sign * foreign_precision

                    if adjustment > 0:
                        acc, contra = account, transitory_account
                    else:
                        acc, contra = transitory_account, account
                        rounding_amount = abs(rounding_amount)

                    rounding_entries.append({
                        "id": rounding_id,
                        "date": rounding_date,
                        "description": "Compensation of CashCtrl rounding differences",
                        "account": acc,
                        "contra": contra,
                        "currency": rounding_currency,
                        "amount": rounding_amount,
                        "report_amount": abs(adjustment),
                    })

            if rounding_entries:
                rounding_df = pd.DataFrame(rounding_entries)
                rounding_df = self.journal.standardize(rounding_df)
                result = pd.concat([result, rounding_df], ignore_index=True)

            # Step 5: Check if TOTAL transaction is balanced, fix transitory if needed
            # This handles cases where smart splitting introduced its own rounding error
            result_multiplier = (
                result["account"].notna().astype(int) - result["contra"].notna().astype(int)
            )
            total_imbalance = self.round_to_precision(
                (result["report_amount"] * result_multiplier).sum(), reporting_currency
            )

            if abs(total_imbalance) > currency_precision / 2:
                # Find a reporting-currency account to balance against
                # We need a reporting-currency account to avoid currency fallback issues
                accounts_df = self.accounts.list()
                reporting_accounts = original_reports.merge(
                    accounts_df[['account', 'currency']],
                    on='account',
                    how='left'
                )
                reporting_accounts = reporting_accounts[
                    reporting_accounts['currency_y'] == reporting_currency
                ]

                if len(reporting_accounts) > 0:
                    balance_account = reporting_accounts["account"].iloc[0]

                    if total_imbalance > 0:
                        # Transaction has too much debit - credit transitory
                        acc, contra = balance_account, transitory_account
                        adjustment = total_imbalance
                    else:
                        # Transaction has too much credit - debit transitory
                        acc, contra = transitory_account, balance_account
                        adjustment = -total_imbalance

                    rounding_id = entry["id"].iloc[0] + ":rounding"
                    rounding_date = entry["date"].iloc[0]
                    total_rounding = pd.DataFrame([{
                        "id": rounding_id,
                        "date": rounding_date,
                        "description": "Compensation of CashCtrl rounding differences",
                        "account": acc,
                        "contra": contra,
                        "currency": reporting_currency,
                        "amount": adjustment,
                        "report_amount": adjustment,
                    }])
                    total_rounding = self.journal.standardize(total_rounding)
                    result = pd.concat([result, total_rounding], ignore_index=True)
                # If no reporting-currency account, skip - transitory will have small residual

            return self.journal.standardize(result)

        else:
            raise ValueError("Expecting at least one `entry` row.")

    def _smart_convert_to_foreign(
        self, entry, fx_rate, currency, transitory_account, reporting_currency
    ):
        """Convert reporting-currency entries to foreign currency with smart splitting.

        Searches for clearing/balancing amounts that make both currencies balance
        after CashCtrl's recalculation (amount * fx_rate).
        """
        multiplier = entry["account"].notna().astype(int) - entry["contra"].notna().astype(int)

        # Identify entries that need conversion (reporting currency entries)
        usd_mask = entry["currency"] == reporting_currency

        if not usd_mask.any():
            # No conversion needed, just add balancing leg
            return self._add_balancing_leg(entry, fx_rate, transitory_account, currency)

        # Calculate total foreign amount in the original entries
        foreign_mask = entry["currency"] == currency
        total_foreign_orig = (entry.loc[foreign_mask, "amount"] * multiplier[foreign_mask]).sum()

        # Convert USD entries to foreign currency (naive conversion first)
        entry.loc[usd_mask, "amount"] = entry.loc[usd_mask, "report_amount"] / fx_rate
        entry.loc[usd_mask, "currency"] = currency
        currency_precision = self.precision_vectorized([currency], [entry["date"].iloc[0]])[0]
        reporting_precision = self.precision_vectorized(
            [reporting_currency], [entry["date"].iloc[0]]
        )[0]
        entry["amount"] = self.round_to_precision(entry["amount"], entry["currency"])

        # USD that CashCtrl will compute for original foreign entries (fixed)
        foreign_usd = self.round_to_precision(
            total_foreign_orig * fx_rate, reporting_currency
        )

        total_foreign_now = (entry["amount"] * multiplier).sum()
        balance_foreign = self.round_to_precision(-total_foreign_now, currency)

        if balance_foreign == 0:
            return entry

        # Search for clearing/balancing split that gives total_usd = 0 after recalculation
        best_split = None
        best_diff = float('inf')

        for balancing in range(int(-50 / currency_precision), int(51 / currency_precision)):
            balancing_amount = balancing * currency_precision
            clearing_amount = balance_foreign - balancing_amount

            clearing_usd = self.round_to_precision(clearing_amount * fx_rate, reporting_currency)
            balancing_usd = self.round_to_precision(balancing_amount * fx_rate, reporting_currency)

            converted_usd = 0
            for idx in entry.index[usd_mask]:
                converted_usd += self.round_to_precision(
                    entry.loc[idx, "amount"] * fx_rate, reporting_currency
                ) * multiplier[idx]

            total_usd = foreign_usd + converted_usd + clearing_usd + balancing_usd
            diff = abs(total_usd)

            if diff < best_diff:
                best_diff = diff
                best_split = (clearing_amount, balancing_amount, clearing_usd, balancing_usd)

            if diff < reporting_precision / 2:
                break

        clearing_amount, balancing_amount, _, _ = best_split

        if abs(clearing_amount) >= currency_precision / 2:
            clearing_leg = entry.head(1).copy()
            clearing_leg["currency"] = currency
            clearing_leg["description"] = "Clearing to transitory"
            clearing_leg["amount"] = clearing_amount
            clearing_leg["account"] = transitory_account
            entry = pd.concat([
                self.journal.standardize(entry),
                self.journal.standardize(clearing_leg),
            ])

        if abs(balancing_amount) >= currency_precision / 2:
            balancing_leg = entry.head(1).copy()
            balancing_leg["currency"] = currency
            balancing_leg["description"] = "Balancing foreign currency amount"
            balancing_leg["amount"] = balancing_amount
            balancing_leg["account"] = transitory_account
            entry = pd.concat([
                self.journal.standardize(entry),
                self.journal.standardize(balancing_leg),
            ])

        return entry

    def _add_balancing_leg(self, entry, fx_rate, account, currency):
        """Add clearing entry to balance foreign currency amounts in collective transactions."""
        multiplier = entry["account"].notna().astype(int) - entry["contra"].notna().astype(int)
        reporting_currency = self.reporting_currency
        balance_foreign = self.round_to_precision(
            -1.0 * (entry["amount"] * multiplier).sum(), currency
        )

        if balance_foreign != 0:
            balancing_leg = entry.head(1).copy()
            balancing_leg["currency"] = currency
            balancing_leg["description"] = "Balancing foreign currency amount"
            balancing_leg["amount"] = balance_foreign
            balancing_leg["account"] = account
            balancing_leg["report_amount"] = self.round_to_precision(
                balance_foreign * fx_rate, reporting_currency
            )
            entry = pd.concat([
                self.journal.standardize(entry),
                self.journal.standardize(balancing_leg),
            ])
        return entry
