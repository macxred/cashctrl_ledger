# CashCtrl FX Constraints - Complete Analysis

## VERIFIED BEHAVIOR (2024-12-16 API Tests)

### Key Findings from Direct API Testing

| Test | Result |
|------|--------|
| Explicit `currencyRate` | ✓ CashCtrl ACCEPTS and stores our rate |
| Per-item `creditC`/`debitC` | ✗ IGNORED - not stored, not returned |
| Single rate per transaction | ✓ CONFIRMED - one `currencyRate` for all items |
| USD (reporting currency) entries | ✓ NOT recalculated (`currencyRate = 1`) |

### Test Evidence

**Test 1: Per-item USD amounts ignored**
```
Sent: 1000 JPY with creditC=12.00 USD (implied rate 0.012)
Result: CashCtrl used its own rate (0.006505), gave 6.51 USD
Conclusion: creditC/debitC values are IGNORED
```

**Test 2: Explicit currencyRate accepted**
```
Sent: 1000 JPY with currencyRate=0.012
Result: CashCtrl stored rate 0.012, USD amount = 12.00
Conclusion: We CAN control FX rate via currencyRate parameter
```

**Test 3: USD adjustment entry preserved**
```
Sent: 0.05 USD entry (no currencyId = reporting currency)
Result: amount=0.05, defaultCurrencyAmount=0.05, currencyRate=1
Conclusion: Reporting currency entries are NOT recalculated
```

### Confirmed Solution Approach

**Problem:** CashCtrl recalculates `report_amount = amount × fx_rate` for foreign currency entries

**Solution:** Use USD adjustment entries for rounding compensation
1. Main JPY transaction: CashCtrl will recalculate USD (unavoidable)
2. Separate USD adjustment entry: NOT recalculated, preserves exact amount
3. Net effect: Correct USD balances on both accounts

**Example:**
```
Original: 12345678 JPY = 76386.36 USD
CashCtrl: 12345678 × 0.0061873 = 76386.41 USD (0.05 difference)

Solution:
  Entry 1 (JPY): +12345678 JPY = +76386.41 USD (recalculated)
  Entry 2 (USD): -0.05 USD adjustment (preserved)
  Net on JPY account: +76386.36 USD ✓
```

---

## CashCtrl Data Model Constraints

| Constraint | Description | Impact |
|------------|-------------|--------|
| Single transaction currency | Each collective transaction has ONE foreign currency + implicit reporting currency | Multi-currency transactions must be split |
| Account currency restriction | Foreign-currency accounts ONLY accept entries in their native currency | Can't post EUR to JPY account |
| Null currency for reporting | Reporting-currency transactions stored with `currencyId=null` | Causes currency fallback on read |
| 8-digit FX precision | FX rates rounded to 8 decimal places | Causes rounding differences |
| **Report amount recalculation** | CashCtrl ALWAYS recalculates `report_amount = amount × fx_rate` | **Our report_amount adjustments are IGNORED** |
| Custom FX rates allowed | CashCtrl stores our specified rate (via `report_amount/amount`) | Gives us flexibility for FX rate |

---

## Problem 1: Multi-Currency Transactions

**Problem:** CashCtrl only supports ONE foreign currency per transaction.

**Example:**
```
Original: 12345678 JPY → -70791.78 EUR (converting JPY to EUR)
```

**Current Solution:** Split into separate transactions per currency:
- `1:JPY` - JPY side with transitory clearing
- `1:EUR` - EUR side with transitory clearing

**Status:** ✅ SOLVED

---

## Problem 2: 8-Digit FX Rate Precision

**Problem:** CashCtrl rounds FX rates to 8 decimal places.

**Example:**
```
Original FX rate:  76386.36 / 12345678 = 0.006187295667...
8-digit rounded:   0.00618730
CashCtrl calculates: 12345678 × 0.00618730 = 76386.35 USD (different!)
```

**Current Solution:** Create `:fx` adjustment entries to compensate for the difference.

**How it works:**
1. Calculate implied FX rate from `report_amount / amount`
2. Round to 8 digits
3. Calculate difference: `original_report_amount - (amount × rounded_fx_rate)`
4. Create `:fx` entry to adjust

**Status:** ✅ SOLVED

---

## Problem 3: Currency Fallback on Read

**Problem:** When posting reporting-currency entry to foreign account:
- We send: `currency=USD, amount=0.01`
- CashCtrl stores: `currencyId=null`
- On read: currency falls back to account's native currency (JPY)

**Current Solution:** Use foreign currency for `:fx` entries:
- Send: `currency=JPY, amount=1, report_amount=0.05`
- CashCtrl stores with `currencyCode=JPY`
- On read: currency preserved as JPY ✓

**Status:** ✅ SOLVED

---

## Problem 4: Report Amount Recalculation (THE BLOCKER)

**Problem:** CashCtrl ALWAYS recalculates `report_amount = amount × fx_rate`.

**Example:**
```
We send:           -12345669 JPY with report_amount = -76386.35 USD
CashCtrl stores:   -12345669 JPY with report_amount = -76386.36 USD (recalculated!)
                   (-12345669 × 0.00618734 = -76386.3578... → -76386.36)
```

**Why this matters:**
When splitting a transaction, amounts are distributed and each rounds independently:
```
Original:  12345678 JPY = 76386.36 USD
Split into:
  +12345678 JPY → +76386.41 USD (12345678 × 0.00618734)
  -12345669 JPY → -76386.36 USD (12345669 × 0.00618734)
  -9 JPY        → -0.06 USD     (9 × 0.00618734)
  ─────────────────────────────
  Sum:          → -0.01 USD     (IMBALANCED!)
```

**Current Solution Attempt:** Create `:rounding` entry to compensate.

**The Blocker:**
- `:rounding` entry is a balanced double-entry (debit one account, credit another)
- It moves the 0.01 imbalance from transitory to foreign account
- Result: Transitory = 0 ✓, but Foreign account = -0.01 ✗

**Why we can't just adjust report_amount:**
- CashCtrl ignores our report_amount values
- It recalculates from `amount × fx_rate`
- We have NO CONTROL over final report_amount for foreign-currency entries

**Status:** ❌ BLOCKED - Need different approach

---

## Problem 5: Independent Rounding of Split Amounts

**Problem:** When we split JPY amount between clearing and balancing entries, each rounds independently.

**Mathematical Example:**
```
Need to distribute: -12345678 JPY
Clearing entry:     -12345669 JPY (main amount)
Balancing entry:    -9 JPY        (difference to balance JPY amounts)

JPY amounts balance: -12345669 + -9 = -12345678 ✓
But USD amounts don't balance:
  Clearing:   -12345669 × 0.00618734 = -76386.3578... → -76386.36
  Balancing:  -9 × 0.00618734        = -0.0556...    → -0.06
  Sum:        -76386.42 USD

Original:     12345678 × 0.00618734 = 76386.4134... → 76386.41 USD

Net: 76386.41 - 76386.42 = -0.01 USD (IMBALANCE)
```

**Root Cause:** Rounding happens AFTER multiplication, not before.

**Status:** ❌ BLOCKED - Same root cause as Problem 4

---

## Summary Table

| Problem | Description | Solution | Status |
|---------|-------------|----------|--------|
| Multi-currency | One currency per transaction | Split by currency | ✅ Solved |
| FX precision | 8-digit rounding | `:fx` adjustment entries | ✅ Solved |
| Currency fallback | USD becomes JPY on read | Use foreign currency | ✅ Solved |
| Report amount recalc | CashCtrl ignores our values | `:rounding` entries | ⚠️ Partial (moves error) |
| Independent rounding | Split amounts round separately | ? | ❌ Blocked |

---

## What Blocks Us

**The fundamental blocker:**
1. CashCtrl recalculates `report_amount = amount × fx_rate` for ALL foreign-currency entries
2. We cannot control the final `report_amount` values
3. When amounts round independently, the sum of rounded values ≠ rounded sum
4. Double-entry accounting requires balanced entries, so we can only MOVE the error, not ELIMINATE it

**The only values we control:**
- `amount` (in foreign currency)
- `fx_rate` (indirectly, via our chosen values)

**The value we DON'T control:**
- `report_amount` for foreign-currency entries (always recalculated)

---

## THE SOLUTION: Fowler's Allocation Algorithm

### The Problem in Current Code

In `_add_balancing_leg` (line 445-446):
```python
balancing_leg["report_amount"] = self.round_to_precision(
    balancing_leg["amount"] * fx_rate, self.reporting_currency,
)
```

**Bug:** USD `report_amount` is calculated from `amount * fx_rate`, NOT from what's needed to balance USD.

### The Fix

**Instead of:** Calculate each report_amount independently
```
entry1: report_amount = round(amount1 * fx_rate)
entry2: report_amount = round(amount2 * fx_rate)  ← independent calculation
```

**Do:** Use allocation - compute last entry as remainder
```
entry1: report_amount = round(amount1 * fx_rate)
entry2: report_amount = total_needed - entry1.report_amount  ← remainder ensures balance
entry2: amount = round(report_amount / fx_rate)  ← derive JPY from USD target
```

### Implementation

**Challenge:** CashCtrl recalculates `report_amount = amount * fx_rate`, so we can't just set report_amount directly.

**Solution:** Find `amount` such that `round(amount * fx_rate) = target_report_amount`

In `_add_balancing_leg`:
```python
# OLD (wrong):
balancing_leg["amount"] = balance  # JPY to balance JPY
balancing_leg["report_amount"] = round(balance * fx_rate)  # USD calculated independently

# NEW (correct - Fowler's Allocation):
# Step 1: Calculate target USD (the remainder)
total_report_needed = -1.0 * (entry["report_amount"] * multiplier).sum()
target_usd = round(total_report_needed)

# Step 2: Find JPY amount that gives this USD after CashCtrl's recalculation
# Initial estimate: amount = target_usd / fx_rate
candidate_amount = round(target_usd / fx_rate, currency_precision)

# Step 3: Verify and adjust if needed
actual_usd = round(candidate_amount * fx_rate, usd_precision)
if actual_usd != target_usd:
    # Try candidate ± 1 unit and pick the one closest to target
    for delta in [-1, 1]:
        alt_amount = candidate_amount + delta * currency_precision
        alt_usd = round(alt_amount * fx_rate, usd_precision)
        if alt_usd == target_usd:
            candidate_amount = alt_amount
            break

balancing_leg["amount"] = candidate_amount
balancing_leg["report_amount"] = target_usd  # Will be recalculated by CashCtrl, but now matches!
```

### Why This Works

**Example:**
```
Target USD: -0.05 (the remainder needed to balance)
fx_rate: 0.00618734

Step 1: candidate_amount = -0.05 / 0.00618734 = -8.08 → round to -8 JPY
Step 2: verify: -8 × 0.00618734 = -0.0495 → rounds to -0.05 ✓

CashCtrl stores: amount=-8 JPY, recalculates report_amount=-0.05 USD
Total now balances: 76386.41 - 76386.36 - 0.05 = 0.00 ✓
```

### Edge Case: No Exact Match

If no JPY amount gives exactly the target USD (rare), we:
1. Pick the closest amount
2. Let the small difference (< 0.005 USD) go to the `:rounding` entry
3. This should be extremely rare with proper fx_rate precision

---

## Currency Mismatch Problem (Connected Issue)

### The Problem

When posting reporting-currency entry to foreign-currency account:
```
Send:    currency=USD, amount=0.01 to JPY account (1020)
Store:   currencyId=null (CashCtrl treats as reporting currency)
Read:    currency=JPY (falls back to account's native currency)
Result:  Mismatch! Sent USD, got back JPY
```

### Connection to Rounding

Both problems occur in the **same transactions** - multi-currency splits with balancing legs:
1. Split creates clearing entries (transitory ↔ foreign)
2. Balancing leg is added to balance foreign currency amounts
3. Rounding causes USD imbalance ← **Rounding problem**
4. Using USD for adjustments causes currency mismatch ← **Currency problem**

### Solution: Use Foreign Currency

For ALL entries involving foreign-currency accounts, use the foreign currency:
```python
# Balancing leg: use JPY, not USD
balancing_leg["currency"] = currency  # JPY (foreign)
balancing_leg["amount"] = find_jpy_that_gives_target_usd(target_usd, fx_rate)
balancing_leg["report_amount"] = target_usd  # CashCtrl recalculates, but now matches!
balancing_leg["fx_rate"] = fx_rate  # 8-digit precision
```

This solves BOTH problems:
1. **Currency preserved**: JPY in, JPY out ✓
2. **USD correct**: JPY amount chosen to give correct USD after recalculation ✓

---

## Identifying Affected Transactions

### Transactions That Need Fowler Allocation

A transaction needs special handling when ALL of these are true:
1. **Multi-currency**: Contains entries in different currencies
2. **Split required**: More than one foreign currency involved
3. **Balancing leg created**: `_add_balancing_leg` is called

### Code to Identify

```python
def needs_allocation(entry):
    """Returns True if transaction needs Fowler allocation."""
    currencies = entry["currency"].dropna().unique()
    has_foreign = any(c != reporting_currency for c in currencies)
    has_reporting = reporting_currency in currencies or any(entry["report_amount"].notna())

    # Multi-currency transaction that will be split
    return has_foreign and has_reporting and len(currencies) > 1
```

### Where to Apply

In `_add_fx_adjustment` (extended_ledger.py):
- When processing multi-currency transactions
- Before calling `_add_balancing_leg`
- Calculate the target USD using Fowler allocation

---

## Accounting Validity

### Fowler Allocation - Is It Legal?

**Yes, absolutely.** This is a well-established approach used by:
- Major accounting software (SAP, Oracle Financials)
- Banks for currency conversion
- Martin Fowler documented it in "Patterns of Enterprise Application Architecture"

### Key Principles (All Preserved)

| Principle | Before Fix | After Fix |
|-----------|-----------|-----------|
| **Debits = Credits** | ✓ JPY balances, USD doesn't | ✓ Both balance |
| **Total preserved** | ✗ 0.01 lost to rounding | ✓ Exact total |
| **Audit trail** | ✓ All entries recorded | ✓ All entries recorded |
| **FX rate documented** | ✓ Stored in entry | ✓ Stored in entry |

### What Changes?

**Old approach:**
```
Entry 1: -12345669 JPY → -76386.36 USD (calculated independently)
Entry 2: -9 JPY → -0.06 USD (calculated independently)
Total: -76386.42 USD (doesn't match +76386.41!)
```

**New approach (Fowler):**
```
Entry 1: -12345669 JPY → -76386.36 USD (calculated)
Entry 2: -8 JPY → -0.05 USD (remainder to ensure balance)
Total: -76386.41 USD ✓
```

### Is -8 JPY vs -9 JPY a Problem?

**No.** Because:
1. The **USD value** (reporting currency) is correct
2. The **JPY difference** (1 yen = ~$0.006) is immaterial
3. The **transitory account** balances to zero
4. The **foreign currency accounts** show correct USD values

### Accounting Standard Compliance

- **IAS 21** (Effects of Changes in Foreign Exchange Rates) - allows different methods for currency conversion as long as consistently applied
- **GAAP** - requires consistency and materiality (1 yen difference is immaterial)

---

---

## NEW FINDING: Single FX Rate Per Transaction

### Discovery (2024-12-16)

During implementation testing, we discovered a critical constraint:

**CashCtrl uses ONE fx_rate for the ENTIRE transaction, not per-row.**

### What This Means

When we set a custom `fx_rate` on the balancing leg:
```python
balancing_leg["amount"] = -9  # JPY
balancing_leg["report_amount"] = -0.05  # USD
balancing_leg["fx_rate"] = 0.00555...  # Custom rate to make -9 * rate = -0.05
```

**CashCtrl ignores our custom fx_rate** and uses the transaction's single fx_rate (0.0061873):
```
CashCtrl recalculates: -9 * 0.0061873 = -0.056 → rounds to -0.06 USD
We wanted: -0.05 USD
Difference: 0.01 USD
```

### Why Fowler Allocation (As Implemented) Doesn't Work

The original Fowler allocation idea was:
1. Calculate target USD (-0.05)
2. Find JPY amount that gives this USD
3. Use custom fx_rate to achieve it

But CashCtrl's single-fx-rate-per-transaction constraint means:
- We CAN'T have different fx_rates for different rows
- The balancing leg will use the same fx_rate as other rows
- So -9 JPY will always become -0.06 USD, not -0.05 USD

### The Conflict

| Constraint | Required Amount | Why |
|------------|-----------------|-----|
| Balance JPY | -9 JPY | 12345678 - 12345669 = 9, need -9 to balance |
| Balance USD | -8 JPY | -8 * 0.0061873 = -0.0495 → -0.05 USD |

**These are mutually exclusive!** We can't have both.

### Current Code Flow

1. `_add_balancing_leg` adds -9 JPY with custom fx_rate 0.00555...
2. Code calculates `balance = report_amount - (amount * transaction_fx_rate)`
3. For balancing leg: balance = -0.05 - (-0.06) = 0.01
4. This creates `:fx` adjustment entry for 0.01
5. `:fx` entry faces same problem (uses 1 JPY with custom fx_rate)
6. `:rounding` entry tries to compensate in USD

### Actual Results from Testing

```
=== Result of _add_fx_adjustment ===
               id  account  currency      amount  report_amount
0           1:JPY     1020      JPY  12345678.0       76386.41
1           1:JPY     1999      JPY -12345669.0      -76386.36
2           1:JPY     1999      JPY        -9.0          -0.06  ← Should be -0.05!
3        1:JPY:fx     1020      JPY        -1.0          -0.05
4        1:JPY:fx     1999      JPY         1.0           0.01
5  1:JPY:rounding     1999      USD        0.05           <NA>

=== Per-transaction USD balance ===
1:JPY: -0.0100      ← Not balanced!
1:JPY:fx: -0.0400   ← Not balanced!
1:JPY:rounding: 0.0000 ✓
```

---

## INDUSTRY RESEARCH: How Others Solve This

### Major ERP Systems Approach

| System | Solution |
|--------|----------|
| **SAP FICO** | Dedicated rounding difference accounts (configured via SPRO). Differences auto-posted to first non-automatic line item. |
| **Oracle Financials** | Two methods: (1) Post to largest journal line, or (2) Post to dedicated rounding imbalances account |
| **Sage Accounting** | Automatic "Currency and Exchange Rounding" account. System adjusts small differences automatically. |
| **Xero/NetSuite** | Dedicated GL accounts for rounding differences |

**Common Pattern**: ALL major systems use **dedicated GL accounts** for rounding differences, ensuring double-entry integrity while accommodating computational artifacts.

### Hamilton's Method (Largest Remainder Algorithm)

This is THE standard solution for the "penny allocation" problem, proposed by Alexander Hamilton in 1792:

```
Problem: Distribute $0.05 in 30%/70% ratio
- 30% × $0.05 = $0.015 (can't have half-penny)
- 70% × $0.05 = $0.035 (can't have half-penny)
- Simple rounding loses/gains pennies

Hamilton's Solution:
1. Allocate integer part to each bucket
2. Sort by fractional remainder (descending)
3. Distribute leftover pennies to buckets with largest remainders
```

**Key Insight**: Don't try to make each individual allocation "correct" - instead, ensure the TOTAL is correct and distribute any remainder fairly.

### IAS 21 / GAAP Guidelines

- **IAS 21.28**: Exchange differences on monetary items → Profit & Loss
- **Materiality principle**: Immaterial rounding differences may use practical expedients
- **Industry practice**: Rounding differences → expense/revenue account or dedicated rounding account

### Peter Selinger's Multi-Currency Accounting Tutorial

Key insight: "Adjusting entries" that violate double-entry principles remove redundancy needed to distinguish FX fluctuations from arithmetical errors.

**Solution**: Post explicit rounding differences to dedicated accounts, maintaining full double-entry integrity.

---

## ANALYSIS: Which Option Aligns With Industry Practice?

### Options Comparison

| Option | Aligns with Industry? | Pros | Cons |
|--------|----------------------|------|------|
| **A: Prioritize USD, accept JPY imbalance** | ⚠️ Partial | USD balances exactly | CashCtrl may reject JPY imbalance |
| **B: Per-transaction imbalance + :rounding** | ✅ **YES** | Matches SAP/Oracle approach | Need to fix current bugs |
| **C: Separate adjustment transaction** | ✅ YES | Clean separation | More entries to manage |

### Research Conclusion

**Option B is the industry-standard approach**, used by SAP, Oracle, and other major systems. The current implementation conceptually aligns with industry best practices:

1. ✅ Uses transitory/clearing account (like SAP's rounding account)
2. ✅ Posts explicit rounding differences (like Oracle's approach)
3. ✅ Maintains double-entry integrity
4. ✅ Uses reporting currency for `:rounding` (CashCtrl preserves these)

### The Real Problem

The **concept is correct**, but there's a **bug in the implementation**:

The `:rounding` compensation calculates the error **after** balancing legs and :fx adjustments are created, but:
1. Each layer introduces its own rounding
2. The custom FX rate on balancing legs gets rounded to 8 digits
3. The final rounding error doesn't capture all accumulated differences

### Root Cause Hypothesis

The `rounding_error` calculation (line 390) uses **already-rounded** values:
```python
rounding_error = (multiplier * amount).sum()  # summing rounded values
```

Instead, it should track the **cumulative exact difference** from the original transaction.

---

## RECOMMENDED APPROACH

### Strategy: Fix the :rounding calculation to capture actual total error

Instead of calculating rounding error from intermediate values, calculate the **final actual discrepancy** by comparing:
- What we WANT: The original report_amounts summed correctly
- What CashCtrl will STORE: The recalculated report_amounts

### Implementation Steps

1. **Track original totals** before any adjustments:
   ```python
   original_total = (entry["report_amount"] * multiplier).sum()
   ```

2. **Calculate what CashCtrl will actually store** after all processing:
   ```python
   final_total = sum(round(amount * fx_rate) for each entry)
   ```

3. **Create :rounding entry for the ACTUAL difference**:
   ```python
   actual_error = final_total - original_total
   ```

4. **Use Hamilton's method** for distributing any remaining fractions fairly across entries.

### Why This Will Work

- Captures ALL rounding errors, not just intermediate ones
- Uses reporting currency for :rounding (CashCtrl preserves these)
- Matches industry standard (SAP/Oracle approach)
- Maintains double-entry integrity

---

## DETAILED IMPLEMENTATION PLAN

### File: `cashctrl_ledger/extended_ledger.py`

#### Step 1: Capture original transaction total at start of `_add_fx_adjustment`

At the beginning of the collective transaction processing (after line 315), save the original totals:

```python
# Save original totals BEFORE any modifications
multiplier = entry["account"].notna().astype(int) - entry["contra"].notna().astype(int)
original_report_total = (entry["report_amount"] * multiplier).sum()
```

#### Step 2: Simplify `_add_balancing_leg`

Remove the custom FX rate logic (it doesn't work with CashCtrl's single-rate constraint). Instead:

```python
def _add_balancing_leg(self, entry, fx_rate, account, currency):
    """Add balancing leg to balance foreign currency amounts only.

    We don't try to balance USD here - the :rounding entry handles that.
    """
    multiplier = entry["account"].notna().astype(int) - entry["contra"].notna().astype(int)

    # Balance foreign currency only
    balance_foreign = self.round_to_precision(
        -1.0 * (entry["amount"] * multiplier).sum(), currency
    )

    if balance_foreign != 0:
        balancing_leg = entry.head(1).copy()
        balancing_leg["currency"] = currency
        balancing_leg["amount"] = balance_foreign
        balancing_leg["account"] = account
        # Let CashCtrl calculate report_amount from amount * fx_rate
        balancing_leg["report_amount"] = self.round_to_precision(
            balance_foreign * fx_rate, self.reporting_currency
        )
        entry = pd.concat([...])
    return entry
```

#### Step 3: Fix the rounding error calculation

At the end of `_add_fx_adjustment`, calculate the ACTUAL total error:

```python
# Calculate what CashCtrl will actually store (all entries recalculated)
cashctrl_report_total = 0
for idx, row in result.iterrows():
    if row["currency"] == reporting_currency:
        cashctrl_report_total += row["amount"] * multiplier[idx]
    else:
        # CashCtrl recalculates foreign currency entries
        recalc = self.round_to_precision(row["amount"] * fx_rate, reporting_currency)
        cashctrl_report_total += recalc * multiplier[idx]

# The ACTUAL error is the difference from original
actual_error = cashctrl_report_total - original_report_total
```

#### Step 4: Create :rounding entry for actual error

```python
if abs(actual_error) > currency_precision / 2:
    adjustment = self.round_to_precision(-actual_error, reporting_currency)
    # Create :rounding entry in reporting currency (CashCtrl preserves these)
    rounding_compensation = pd.DataFrame({
        "id": [rounding_id],
        "date": [rounding_date],
        "description": ["Compensation of CashCtrl rounding differences"],
        "account": [account],
        "contra": [contra],
        "currency": [reporting_currency],
        "amount": [abs(adjustment)],
        "report_amount": [abs(adjustment)],
    })
```

### Key Changes Summary

| What | Current | New |
|------|---------|-----|
| Balancing leg | Uses custom FX rate (ignored by CashCtrl) | Uses transaction FX rate |
| Rounding calc | Based on intermediate values | Based on original vs final totals |
| :rounding entry | Compensates intermediate error | Compensates ACTUAL total error |

---

## ALTERNATIVE APPROACH: Smart Amount Splitting (Hamilton's Method)

### User Idea: Split transaction amounts to minimize rounding

Instead of accepting whatever JPY balance exists and compensating USD, **choose JPY amounts that give better USD results**.

### How It Would Work

```
Current approach:
  Clearing amount = -76386.36 / 0.0061873 = -12345669.35... → round to -12345669 JPY
  Balance needed = 12345678 - 12345669 = 9 JPY
  Balancing: -9 * 0.0061873 = -0.0557... → -0.06 USD (wrong!)

Smart splitting:
  Target: Find clearing amount where balance gives correct USD

  For -0.05 USD: need -0.05 / 0.0061873 = -8.08... → -8 JPY
  So clearing = 12345678 - 8 = 12345670 JPY
  Verify: 12345670 * 0.0061873 = 76386.36... → 76386.36 USD ✓
```

### Advantages

- **No `:rounding` entries needed** for most transactions
- **Both JPY and USD balance** within the same transaction
- Cleaner, fewer entries
- Matches what CashCtrl will actually calculate

### Implementation Sketch

In `_add_fx_adjustment`, when converting USD clearing to JPY:

```python
# Instead of: clearing_jpy = -76386.36 / fx_rate → round
# Do:
target_usd = entry["report_amount"]  # -76386.36
main_jpy = entry["amount"]  # 12345678

# Find clearing_jpy such that:
#   main_jpy + clearing_jpy + balance_jpy = 0  (JPY balance)
#   round(main_jpy * fx) + round(clearing_jpy * fx) + round(balance_jpy * fx) = 0  (USD balance)

# Work backwards from desired balance_jpy
for candidate_balance in [-8, -9, -7, -10, ...]:  # Try amounts near estimate
    clearing_jpy = -(main_jpy + candidate_balance)
    recalc_main = round(main_jpy * fx_rate)
    recalc_clearing = round(clearing_jpy * fx_rate)
    recalc_balance = round(candidate_balance * fx_rate)
    if recalc_main + recalc_clearing + recalc_balance == 0:
        # Found exact match!
        break
```

### Need to Verify First

Before implementing, we should test:
1. **Does CashCtrl really ignore custom per-entry FX rates?**
2. **Can we always find a JPY split that balances both currencies?**

---

## FIRST: Test CashCtrl FX Behavior

### Questions to Answer

1. **Does CashCtrl support per-entry custom FX rates?**
   - Post collective transaction with entries having different implied FX rates
   - Check if CashCtrl preserves or recalculates each entry

2. **How does CashCtrl determine the transaction FX rate?**
   - From largest entry? From first entry? From average?

3. **Can we always find a JPY split that balances both currencies?**
   - Test with various amounts and FX rates
   - Check if there's always a valid solution

### Test Script

```python
"""Test CashCtrl FX rate behavior"""
from cashctrl_ledger import ExtendedCashCtrlLedger

engine = ExtendedCashCtrlLedger(1999)

# Test 1: Post transaction with custom FX rate on one entry
# Main entry: 1000 JPY @ 0.01 = 10.00 USD (standard rate)
# Clearing:   -992 JPY @ 0.01 = -9.92 USD (standard rate)
# Balancing:    -8 JPY @ 0.005 = -0.04 USD (custom rate!)

test_entry = pd.DataFrame({
    'id': ['test:1', 'test:1', 'test:1'],
    'account': [1020, 1999, 1999],
    'currency': ['JPY', 'JPY', 'JPY'],
    'amount': [1000, -992, -8],
    'report_amount': [10.00, -9.92, -0.08],  # Last entry uses different implied rate!
})

# Post and read back
engine.journal.add(test_entry)
result = engine.journal.list()

# Check: Did CashCtrl preserve -0.08 or recalculate to -0.08?
# If recalculated: -8 * 0.01 = -0.08 (happens to match!)

# Test 2: More extreme - use rate that WILL differ
# Use -5 JPY with target -0.08 USD (implied rate 0.016)
# CashCtrl recalc: -5 * 0.01 = -0.05 USD (different!)
```

### Expected Outcomes

| Scenario | If CashCtrl preserves | If CashCtrl recalculates |
|----------|----------------------|--------------------------|
| Custom FX rate | We can use Fowler allocation | Need smart splitting or :rounding |
| Per-entry rates | Each entry independent | Single rate for whole transaction |

---

## IMPLEMENTATION ORDER

1. **Test CashCtrl FX behavior** (verify assumptions)
2. **Choose approach** based on test results:
   - If custom rates work: Use Fowler allocation
   - If single rate: Use smart splitting (user's idea) or fix :rounding
3. **Implement chosen approach**
4. **Run full test suite**

---

## Files to Modify

1. `cashctrl_ledger/extended_ledger.py`:
   - `_add_fx_adjustment` method (lines 251-431) - capture original total, fix rounding calc
   - `_add_balancing_leg` method (lines 436-486) - simplify, remove custom FX rate

---

## Verification

After fix, these should all pass:
- `test_multi_currency_journal_transitory_balance` - Transitory = 0
- `test_account_balance` - Foreign accounts have correct values
- `test_journal_accessor_mutators` - Currency preserved on round-trip

---

## Testing with sketch.py

### Current sketch.py - Good for Basic Test

The existing sketch tests the JPY→EUR conversion case where the 0.01 problem occurs.

### Enhancements Needed

Add these checks to verify the fix:

```python
# 1. Check foreign account balance is EXACT (not off by 0.01)
print("\n=== FOREIGN ACCOUNT BALANCE CHECK ===")
balance_1020 = engine.individual_account_balances(accounts=1020, period="2024")
expected_1020 = 76386.36  # Original report_amount, should be preserved
actual_1020 = balance_1020.query("account == 1020")["report_balance"].iloc[0]
print(f"Account 1020 (JPY): expected={expected_1020}, actual={actual_1020}")
assert actual_1020 == expected_1020, f"Expected {expected_1020}, got {actual_1020}"

# 2. Check currency round-trip (what we send = what we get back)
print("\n=== CURRENCY ROUND-TRIP CHECK ===")
from_cashctrl = engine.journal.list()
for col in ['id', 'account', 'currency', 'amount', 'report_amount']:
    print(f"{col}: sent={initial[col].tolist()}")
    print(f"{col}: recv={from_cashctrl[col].tolist()}")

# 3. Verify per-transaction balance AFTER CashCtrl round-trip
print("\n=== POST-CASHCTRL TRANSACTION BALANCE ===")
for txn_id in from_cashctrl['id'].unique():
    txn = from_cashctrl[from_cashctrl['id'] == txn_id]
    balance = txn['report_amount'].sum()
    status = "✓" if abs(balance) < 0.001 else "✗"
    print(f"{txn_id}: sum={balance:.4f} {status}")
```

### What Success Looks Like

```
=== TRANSITORY BALANCE FROM CASHCTRL ===
report_balance: 0.0  ← Must be exactly 0

=== FOREIGN ACCOUNT BALANCE CHECK ===
Account 1020 (JPY): expected=76386.36, actual=76386.36  ← Must match exactly

=== POST-CASHCTRL TRANSACTION BALANCE ===
1:EUR: sum=0.0000 ✓
1:JPY: sum=0.0000 ✓  ← Currently fails with -0.01
1:JPY:fx: sum=0.0000 ✓
```

### Run Command

```bash
CC_API_ORGANISATION=alex39 CC_API_KEY=... python sketch.py
```
