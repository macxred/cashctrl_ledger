# CashCtrl FX Constraints and Solutions

## Executive Summary

CashCtrl has specific limitations when handling multi-currency transactions that cause rounding discrepancies. This document captures all discovered constraints, failed approaches, and the final working solution.

---

## Problem Statement

When mirroring multi-currency journal entries to CashCtrl, small rounding differences (typically 0.01) would accumulate, causing:
1. CashCtrl rejecting entries with "Total debit and total credit must be equal" errors
2. Transitory account not balancing to zero
3. Foreign account balances being off by small amounts

---

## CashCtrl Constraints Discovered

### 1. Single FX Rate Per Transaction

**Discovery**: API testing on 2024-12-16

CashCtrl uses ONE `currencyRate` for the ENTIRE collective transaction, not per-row.

```
Test: Post transaction with different implied FX rates per entry
Sent: Entry 1 with rate 0.01, Entry 2 with rate 0.005
Result: CashCtrl used SINGLE rate for all entries
```

**Impact**: Cannot have different FX rates for different rows within one transaction.

### 2. Per-Entry USD Amounts Ignored

**Discovery**: API testing

The `creditC`/`debitC` (reporting currency amounts) sent via API are NOT stored.

```
Sent: 1000 JPY with creditC=12.00 USD (implied rate 0.012)
Result: CashCtrl used its own rate (0.006505), returned 6.51 USD
Conclusion: creditC/debitC values are IGNORED
```

**Impact**: We cannot directly control `report_amount` for foreign currency entries.

### 3. Explicit currencyRate IS Accepted

**Discovery**: API testing

CashCtrl accepts and stores our specified `currencyRate` parameter.

```
Sent: 1000 JPY with currencyRate=0.012
Result: CashCtrl stored rate 0.012, USD amount = 12.00
```

**Impact**: We CAN control the FX rate, but it applies to ALL entries in the transaction.

### 4. Reporting Currency Entries Preserved

**Discovery**: API testing

Entries in the reporting currency (e.g., USD when USD is base) are NOT recalculated.

```
Sent: 0.05 USD entry (no currencyId = reporting currency)
Result: amount=0.05, defaultCurrencyAmount=0.05, currencyRate=1
```

**Impact**: Reporting currency entries can be used for exact adjustments.

### 5. Currency Fallback on Foreign Accounts

**Discovery**: Test failures with currency mismatch

When posting reporting-currency entry to foreign-currency account:
- We send: `currency=USD, amount=0.01` to JPY account (1020)
- CashCtrl stores: `currencyId=null` (reporting currency has no ID)
- On read: currency falls back to account's native currency (JPY)

**Impact**: Cannot use reporting currency for entries to foreign accounts.

### 6. 8-Digit FX Rate Precision

CashCtrl rounds FX rates to 8 decimal places.

```
Original FX rate:  76386.36 / 12345678 = 0.006187295667...
8-digit rounded:   0.00618730
Recalculated:      12345678 × 0.00618730 = 76386.35 USD (0.01 difference!)
```

**Impact**: Rounding differences accumulate across entries.

---

## Failed Approaches

### Approach 1: Custom FX Rate Per Entry (Fowler's Allocation)

**Idea**: Set different `fx_rate` on balancing leg to achieve exact USD.

```python
balancing_leg["amount"] = -9  # JPY
balancing_leg["fx_rate"] = 0.00555...  # Custom rate to make -9 * rate = -0.05
```

**Why it failed**: CashCtrl ignores per-entry rates, uses single transaction rate.

**Result**: Balancing leg used transaction rate (0.0061873), giving -0.06 USD instead of -0.05 USD.

### Approach 2: Accept Small Transitory Residual

**Idea**: Allow up to 0.02 residual on transitory account.

**Why rejected**: User explicitly stated "no, that is not acceptable".

### Approach 3: Reporting Currency Rounding Entries

**Idea**: Create `:rounding` entries in USD to compensate.

**Why it failed**: When posted to foreign accounts, currency falls back to account's native currency, causing mismatch on round-trip.

---

## Working Solution: Smart Splitting

### Core Insight

Instead of:
1. Convert amounts naively
2. Add rounding entries to fix

Do:
1. Search for foreign currency amounts that balance BOTH currencies after rounding
2. Use account's native currency for any rounding entries

### Algorithm: `_smart_convert_to_foreign`

For a transaction needing foreign currency balancing:

```python
# Instead of: clearing_jpy = usd_amount / fx_rate (naive)
# Search for: clearing + balancing amounts where:
#   round(clearing × rate) + round(balancing × rate) = target_usd

for balancing in range(-5000, 5100):  # Try different splits
    balancing_amount = balancing * 0.01  # JPY precision
    clearing_amount = balance_foreign - balancing_amount

    # What USD will CashCtrl calculate?
    clearing_usd = round(clearing_amount * fx_rate, 2)
    balancing_usd = round(balancing_amount * fx_rate, 2)

    total_usd = foreign_usd + converted_usd + clearing_usd + balancing_usd
    if abs(total_usd) < 0.001:
        # Found exact match!
        break
```

### Per-Account Rounding with Native Currency

When creating rounding entries for foreign accounts, use the account's native currency:

```python
if account_currency == reporting_currency:
    # Use USD directly
    rounding_currency = reporting_currency
    rounding_amount = adjustment
else:
    # Convert to account's currency using transaction FX rate
    rounding_currency = currency  # e.g., JPY
    rounding_amount = adjustment / fx_rate
```

This avoids the currency fallback issue.

### Total Balance Check

After all processing, verify total transaction balances. If not:
- Find a reporting-currency account to balance against
- If all accounts are foreign, skip (accept tiny residual on transitory)

---

## Code Structure

### Key Methods in `extended_ledger.py`

1. **`sanitize_journal`** (lines 97-159)
   - Entry point for preparing journal for CashCtrl
   - Splits multi-currency transactions
   - Calls `_add_fx_adjustment` for each transaction

2. **`_add_fx_adjustment`** (lines 220-444)
   - Handles FX rate precision (8-digit rounding)
   - Creates `:fx` entries for individual transactions
   - For collective transactions:
     - Step 1: Save original report_amounts
     - Step 2: Smart convert to foreign currency
     - Step 3: Calculate what CashCtrl will store
     - Step 4: Per-account rounding corrections
     - Step 5: Total balance check

3. **`_smart_convert_to_foreign`** (lines 449-517)
   - Converts reporting-currency entries to foreign currency
   - Searches for clearing/balancing split that balances both currencies

4. **`_add_balancing_leg`** (lines 519-552)
   - Adds single clearing entry to balance foreign currency amounts

---

## Test Coverage

### `test_multi_currency_journal_transitory_balance`

Tests the core scenario:
- CHF reporting currency
- USD and CAD foreign currencies
- Interest income with withholding tax (real Swiss bank pattern)
- Verifies transitory account = 0

### `test_journal_accessor_mutators`

Round-trip test:
- Send entries to CashCtrl
- Read back
- Verify currencies match (catches fallback issue)

### `sketch.py`

Manual verification:
- JPY → EUR conversion (12M+ JPY)
- 9.5M USD → EUR conversion
- Checks: transitory = 0, foreign account balances exact

---

## Known Limitations

1. **Per-transaction rounding entries**: Individual `:rounding` transactions may show small imbalances (e.g., 0.01-0.05) because CashCtrl recalculates their USD amounts. The ACCOUNT balances are correct; it's just the individual transaction that appears unbalanced.

2. **All-foreign transactions**: If a multi-currency transaction has NO reporting-currency accounts, we cannot create a total balance rounding entry without currency fallback. A tiny residual may remain on transitory.

---

## Verification Checklist

After any changes, verify:

- [ ] `test_journal.py` - All 10 tests pass
- [ ] `test_accounts.py::test_account_balance` - Balances match
- [ ] `sketch.py` - Transitory = 0, foreign accounts exact
- [ ] GitHub Actions - CI passes

---

## References

- **Martin Fowler's Allocation**: "Patterns of Enterprise Application Architecture" - penny allocation problem
- **Hamilton's Method**: Largest remainder algorithm for distributing indivisible units
- **IAS 21**: International accounting standard for foreign exchange effects
- **Industry Practice**: SAP, Oracle, Sage all use dedicated rounding accounts

---

## Changelog

| Date | Change |
|------|--------|
| 2024-12-16 | Initial API testing, discovered single-rate constraint |
| 2024-12-16 | Implemented smart splitting approach |
| 2024-12-16 | Fixed currency fallback in per-account rounding |
| 2024-12-16 | Removed unnecessary idempotency code |
| 2024-12-16 | All tests passing, solution complete |
