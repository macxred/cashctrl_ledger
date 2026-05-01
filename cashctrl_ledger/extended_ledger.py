"""This module extends CashCtrlLedger to handle transactions that can not be
directly represented in CahCtrl.
"""

import math
import pandas as pd
import polars as pl
from pyledger.schema import ensure_polars, to_pandas
from .ledger import CashCtrlLedger


class ExtendedCashCtrlLedger(CashCtrlLedger):
    """
    Extends `CashCtrlLedger` to handle transactions that cannot be directly
    represented due to CashCtrl's limitations.

    CashCtrl's data model imposes constraints, such as restricting FX rates
    to eight-digit precision and limiting collective journal entries to a single
    currency beyond the reporting currency. This class ensures that transactions
    conform to CashCtrl's standards by splitting unrepresentable transactions
    into multiple simpler ones that can be accommodated, while preserving the
    overall financial result.

    To use this class, a special `transitory_account` must be defined in the
    chart of accounts. Residual amounts arising from split transactions are
    recorded in this account. The account is balanced for any group of split
    transactions that together represent a single original transaction.
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
        """Returns the transitory account used to book residual amounts when complex
        transactions are broken into simpler ones for compatibility with CashCtrl.

        Raises:
            ValueError: If the transitory account is not set, does not exist, or is
            denominated in a different currency than the reporting currency.

        Returns:
            int: The transitory account number.
        """
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

    def sanitize_journal(
        self, journal: pd.DataFrame | pl.DataFrame,
        pandas: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Modify journal to ensure coherence and compatibility with CashCtrl.

        Extends the base `sanitize_journal` to address CashCtrl-specific constraints:
        - Splits collective journal entries with multiple currencies (other than
          the reporting currency) into separate entries for each currency that can
          be represented in CashCtrl.
        - Ensures transactions conform to CashCtrl's eight-digit precision limit.
        - Creates additional compensating transactions to balance residual amounts
          using the designated `transitory_account`.

        Args:
            journal (pd.DataFrame | pl.DataFrame): The journal DataFrame with
                transactions to be processed.
            pandas: If True, return pandas DataFrame; otherwise polars.

        Returns:
            pd.DataFrame | pl.DataFrame: The modified journal DataFrame.
        """
        journal = self.journal.standardize(
            ensure_polars(journal, "ExtendedCashCtrlLedger.sanitize_journal"),
            pandas=False,
        )

        # Insert missing base currency amounts
        mask = journal['report_amount'].fill_nan(None).is_null()
        if mask.any():
            null_rows = journal.filter(mask)
            filled = self.report_amount(
                amount=null_rows['amount'].to_list(),
                currency=null_rows['currency'].to_list(),
                date=null_rows['date'].to_list(),
            )
            col = journal['report_amount'].fill_nan(None)
            col = col.scatter(
                mask.arg_true(), pl.Series(filled, dtype=pl.Float64),
            )
            journal = journal.with_columns(report_amount=col)

        # Number of currencies other than reporting currency
        reporting_currency = self.reporting_currency
        foreign = journal.filter(pl.col("currency") != reporting_currency)
        n_currency = foreign.group_by("id").agg(
            n=pl.col("currency").n_unique()
        )
        multi_ids = n_currency.filter(pl.col("n") > 1)["id"].to_list()

        # Split entries with multiple currencies into separate entries for each currency
        if len(multi_ids) > 0:
            multi_currency = self.journal.standardize(
                journal.filter(pl.col("id").is_in(multi_ids)), pandas=False,
            )
            multi_currency = self.split_multi_currency_transactions(
                multi_currency, pandas=False,
            )
            others = journal.filter(~pl.col("id").is_in(multi_ids))
            df = pl.concat([others, multi_currency], how="diagonal")
        else:
            df = journal

        # Ensure foreign currencies can be mapped, correct with FX adjustments otherwise
        transitory_account = self.transitory_account
        result = []
        for txn in df.partition_by("id", maintain_order=True):
            new_txn = self._add_fx_adjustment(
                txn,
                transitory_account=transitory_account,
                reporting_currency=reporting_currency,
            )
            result.append(self.journal.standardize(new_txn, pandas=False))

        if len(result) > 0:
            result = pl.concat(result, how="diagonal")
        else:
            result = df

        if pandas:
            return to_pandas(result, self.journal._schema)
        return result

    def split_multi_currency_transactions(
        self, journal: pd.DataFrame | pl.DataFrame,
        transitory_account: int | None = None, pandas: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """
        Splits multi-currency transactions into individual transactions for each currency.

        CashCtrl restricts collective transactions to a single currency beyond the reporting
        currency. This method splits multi-currency transactions into several transactions
        compatible with CashCtrl, each involving a single currency and possibly
        the reporting currency. If there is a residual balance in any currency, it is
        recorded in the `transitory_account`. The total of all entries in the transitory
        account across these transactions will balance to zero.

        Args:
            journal (pd.DataFrame | pl.DataFrame): The journal DataFrame containing
                transactions to be split.
            transitory_account (int, optional): The account number for recording balancing
                entries. If not provided, the instance's `transitory_account` will be used.
            pandas: If True, return pandas DataFrame; otherwise polars.

        Returns:
            pd.DataFrame | pl.DataFrame: Modified journal entries with split transactions
                and any necessary balancing entries.
        """
        journal = ensure_polars(
            journal, "ExtendedCashCtrlLedger.split_multi_currency_transactions",
        )
        reporting_currency = self.reporting_currency
        is_reporting = journal["currency"] == reporting_currency
        journal = journal.with_columns(
            report_amount=pl.when(is_reporting)
            .then(pl.col("amount"))
            .otherwise(pl.col("report_amount")),
        )

        if journal["report_amount"].is_null().any():
            raise ValueError(
                "Reporting currency amount missing for some items."
            )
        if transitory_account is None:
            transitory_account = self.transitory_account

        result = []
        for (txn_id, currency), group in journal.group_by(
            ["id", "currency"], maintain_order=True,
        ):
            sub_id = f"{txn_id}:{currency}"
            result.append(
                group.with_columns(id=pl.lit(sub_id))
            )
            balance = self.round_to_precision(
                group["report_amount"].sum(), reporting_currency,
            )
            if balance != 0:
                clearing_txn = pl.DataFrame({
                    "id": [sub_id],
                    "description": [
                        "Split multi-currency transaction "
                        "into multiple transactions compatible with CashCtrl."
                    ],
                    "amount": [-1 * balance],
                    "report_amount": [-1 * balance],
                    "currency": [reporting_currency],
                    "account": [transitory_account],
                })
                result.append(clearing_txn)

        result = pl.concat(result, how="diagonal")
        result = self.journal.standardize(result, pandas=False)

        if pandas:
            return to_pandas(result, self.journal._schema)
        return result

    def _add_fx_adjustment(
        self, entry: pl.DataFrame, transitory_account: int,
        reporting_currency: str,
    ) -> pl.DataFrame:
        """
        Adjusts journal entries to conform with CashCtrl's eight-digit FX rate precision.

        This method ensures that the reporting currency amounts in a journal entry match
        CashCtrl's precision limit for foreign exchange rates of eight digits after
        the decimal point. If an adjustment is needed due to rounding differences,
        it adds a balancing journal entry using the `transitory_account`, so the
        financial result remains consistent.

        Args:
            entry (pl.DataFrame): The journal entry or entries to be adjusted.
            transitory_account (int): The account number for recording balancing transactions.
            reporting_currency (str): The reporting currency against which adjustments are made.

        Returns:
            pl.DataFrame: The adjusted journal entries and any necessary balancing entries.
        """
        if len(entry) == 1:
            # Individual transaction: one row in the journal data frame
            row = entry.row(0, named=True)
            if row["amount"] == 0 or row["currency"] == reporting_currency:
                return entry
            else:
                amount = self.round_to_precision(
                    row["amount"], row["currency"],
                )
                reporting_amount = self.round_to_precision(
                    row["report_amount"], reporting_currency,
                )
                fx_rate = round(reporting_amount / amount, 8)
                balance = reporting_amount - self.round_to_precision(
                    amount * fx_rate, reporting_currency,
                )
                if balance == 0.0:
                    return entry
                else:
                    balancing_txn = entry.clone().with_columns(
                        id=pl.format("{}:fx", pl.col("id")),
                        currency=pl.lit(row["currency"]),
                        amount=pl.lit(0.0),
                        report_amount=pl.lit(balance),
                    )
                    entry = entry.with_columns(
                        report_amount=pl.col("report_amount") - balance,
                    )
                    result = pl.concat(
                        [
                            self.journal.standardize(entry, pandas=False),
                            self.journal.standardize(
                                balancing_txn, pandas=False,
                            ),
                        ],
                        how="diagonal",
                    )
                    result = result.with_columns(
                        amount=pl.Series(self.round_to_precision(
                            result["amount"], result["currency"],
                        )),
                        report_amount=pl.Series(self.round_to_precision(
                            result["report_amount"],
                            pl.Series([reporting_currency] * len(result)),
                        )),
                    )
                    return self.journal.standardize(result, pandas=False)

        elif len(entry) > 1:
            # Collective transaction: multiple rows in the journal data frame
            currency, fx_rate = \
                self._collective_transaction_currency_and_rate(entry)
            fx_rate = round(fx_rate, 8)
            if currency == reporting_currency:
                return entry
            else:
                is_reporting = entry["currency"] == reporting_currency
                has_null_report = entry["report_amount"].is_null()
                entry = entry.with_columns(
                    report_amount=pl.when(is_reporting & has_null_report)
                    .then(pl.col("amount"))
                    .otherwise(pl.col("report_amount")),
                )
                entry = entry.with_columns(
                    report_amount=pl.Series(self.round_to_precision(
                        entry["report_amount"],
                        pl.Series([reporting_currency] * len(entry)),
                    )),
                )
                if entry["report_amount"].is_null().any():
                    raise ValueError("Reporting currency missing.")

                # Convert reporting currency rows to foreign currency
                is_reporting = entry["currency"] == reporting_currency
                if is_reporting.any():
                    entry = entry.with_columns(
                        amount=pl.when(is_reporting)
                        .then(pl.col("report_amount") / fx_rate)
                        .otherwise(pl.col("amount")),
                        currency=pl.when(is_reporting)
                        .then(pl.lit(currency))
                        .otherwise(pl.col("currency")),
                    )
                entry = entry.with_columns(
                    amount=pl.Series(self.round_to_precision(
                        entry["amount"], entry["currency"],
                    )),
                )
                entry = self._add_balancing_leg(
                    entry, fx_rate=fx_rate,
                    account=transitory_account, currency=currency,
                )

                amounts = entry["amount"]
                report_amounts = entry["report_amount"]
                balance = report_amounts - pl.Series(self.round_to_precision(
                    amounts * fx_rate,
                    pl.Series([reporting_currency] * len(entry)),
                ))
                balance = pl.Series(self.round_to_precision(
                    balance, pl.Series([reporting_currency] * len(balance)),
                ))

                if (balance == 0.0).all():
                    result = entry
                else:
                    entry = entry.with_columns(
                        report_amount=pl.col("report_amount") - balance,
                    )
                    fx_adjust = entry.clone().with_columns(
                        currency=pl.lit(reporting_currency),
                        report_amount=balance,
                        amount=balance,
                        id=pl.format("{}:fx", pl.col("id")),
                        description=pl.format(
                            "Currency adjustments: {}", pl.col("description"),
                        ),
                    )
                    fx_adjust = fx_adjust.filter(
                        pl.col("report_amount") != 0,
                    )
                    fx_adjust = self._add_balancing_leg(
                        fx_adjust, fx_rate=1,
                        account=transitory_account,
                        currency=reporting_currency,
                    )

                    result = pl.concat(
                        [
                            self.journal.standardize(entry, pandas=False),
                            self.journal.standardize(
                                fx_adjust, pandas=False,
                            ),
                        ],
                        how="diagonal",
                    )
                result = result.with_columns(
                    amount=pl.Series(self.round_to_precision(
                        result["amount"], result["currency"],
                    )),
                    report_amount=pl.Series(self.round_to_precision(
                        result["report_amount"],
                        pl.Series([reporting_currency] * len(result)),
                    )),
                )
                account_present = result["account"].is_not_null()
                contra_present = result["contra"].is_not_null()
                multiplier = (
                    account_present.cast(pl.Int64)
                    - contra_present.cast(pl.Int64)
                )
                amount_col = pl.when(
                    pl.col("currency") == reporting_currency
                ).then(pl.col("amount")).otherwise(pl.col("report_amount"))
                amount_vals = result.select(
                    val=amount_col
                )["val"]
                rounding_error = (multiplier * amount_vals).sum()

                currency_precision = self.precision_vectorized(
                    [currency], [entry["date"][0]],
                )[0]
                if abs(rounding_error) > currency_precision / 2:
                    # Rounding after converting foreign currency amounts to reporting
                    # currency amounts by multipying with FX rate leads to rounding
                    # differences that sum up to a net rounding difference equal or
                    # larger than currency precision (0.01 for CHF, EUR, USD, etc.).
                    # -> We compensate this rounding difference by creating
                    #    transactions that have rounding differences with the same
                    #    amount but opposite sign.
                    # 1. Create a transaction with one cent rounding difference
                    first_row = entry.row(0, named=True)
                    txn_id = str(first_row["id"]) + ":rounding"
                    sign = math.copysign(1, rounding_error)
                    rounding_compensation = pl.DataFrame({
                        "id": [txn_id] * 3,
                        "date": [first_row["date"]] * 3,
                        "description": [
                            "Compensation of CashCtrl rounding differences"
                        ] * 3,
                        "account": [transitory_account] * 3,
                        "currency": [currency] * 3,
                        "amount": [
                            sign * x for x in [-0.01, -0.01, 0.02]
                        ],
                        "report_amount": [
                            sign * x for x in [-0.01, -0.01, 0.01]
                        ],
                    })
                    rounding_compensation = self.journal.standardize(
                        rounding_compensation, pandas=False,
                    )
                    # 2. Duplicate this transaction as often as needed to
                    #    compensate the full rounding error
                    n_repeats = abs(
                        round(rounding_error / currency_precision)
                    )
                    parts = [
                        rounding_compensation.with_columns(
                            id=pl.lit(f"{txn_id}-{i}"),
                        )
                        for i in range(n_repeats)
                    ]
                    result = pl.concat(
                        [result, *parts], how="diagonal",
                    )
                return self.journal.standardize(result, pandas=False)

        else:
            raise ValueError("Expecting at least one `entry` row.")

    def _add_balancing_leg(
        self, entry: pl.DataFrame, fx_rate: float,
        account: int, currency: str,
    ) -> pl.DataFrame:
        account_present = entry["account"].is_not_null().cast(pl.Int64)
        contra_present = entry["contra"].is_not_null().cast(pl.Int64)
        multiplier = account_present - contra_present
        balance = self.round_to_precision(
            -1.0 * (entry["amount"] * multiplier).sum(), currency,
        )
        if balance != 0:
            first_row = entry.head(1).clone()
            balancing_leg = first_row.with_columns(
                currency=pl.lit(currency),
                description=pl.lit("Balancing foreign currency amount"),
                amount=pl.lit(balance),
                account=pl.lit(account),
                report_amount=pl.lit(self.round_to_precision(
                    balance * fx_rate, self.reporting_currency,
                )),
            )
            entry = pl.concat(
                [
                    self.journal.standardize(entry, pandas=False),
                    self.journal.standardize(balancing_leg, pandas=False),
                ],
                how="diagonal",
            )
        return entry
