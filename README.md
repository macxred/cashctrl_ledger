# Pyledger CashCtrl Integration

[![codecov](https://codecov.io/gh/macxred/cashctrl_ledger/branch/main/graph/badge.svg)](https://codecov.io/gh/macxred/cashctrl_ledger)

`cashctrl_ledger` is a Python package that implements the `pyledger.LedgerEngine`
interface, enabling seamless integration with the CashCtrl accounting service. With this package,
users can perform various accounting operations programmatically, directly from Python.

## Core Components

1. **CashCtrlLedger**
   Implements the `pyledger` interface by connecting to the CashCtrl online
   accounting software via REST API.

2. **ExtendedCashCtrlLedger**
   Extends `CashCtrlLedger` to ensure that all transactions defined under the
   `pyledger` interface can be accurately represented within CashCtrl, despite
   its restrictions and limitations.

   CashCtrl’s data model imposes constraints, such as limiting FX rates to
   eight-digit precision and restricting collective ledger entries to a single
   currency beyond the reporting currency. `ExtendedCashCtrlLedger` addresses
   these challenges by splitting unrepresentable transactions into multiple
   simpler transactions that conform to CashCtrl’s standards while preserving
   the overall financial result. This enables seamless integration with
   `pyledger` and accurate recording of all transactions in the CashCtrl system.


## Key Features:
This package streamlines the connection between your local ledger system and CashCtrl system.
It simplifies managing tax codes, accounts, and ledger entries by keeping your local and CashCtrl
data in sync. You can easily add, update, or delete financial records and attachments,
ensuring everything is consistent across both systems.

## Credentials

An active Pro subscription is required to interact with your CashCtrl account
via the API. New users can explore the Pro version with a free 30-day trial.
Software developers can create a new test account when the trial period
expires, as they generally do not mind the data loss associated with switching
accounts.

To set up a free test account, follow these steps:

1. Go to https://cashctrl.com/en.
2. 'Sign up' for an account using an email address and an organization name;
    accept the terms and conditions.
3. A confirmation email with an activation link and a password will be sent.
    The activation link brings up a 'First Steps' page.
4. On the 'First Steps' dialog, select 'Try the PRO demo' and
   confirm with 'Update to PRO'.
5. Navigate to Settings (gear icon in the top right corner) ->
   Users & roles -> Add (plus icon) -> Add [API User].
6. Assign the role of 'Administrator' and generate an API key.

The organization name and API key will be used to authenticate API requests.

## Installation

Easily install the package using pip:

```bash
pip install https://github.com/macxred/cashctrl_ledger/tarball/main
```

## Testing Strategy

Tests are housed in the [cashctrl_ledger/tests](tests) directory and are automatically
executed via GitHub Actions. This ensures that the code is tested after each
commit, during pull requests, and on a daily schedule. We prefer pytest for its
straightforward and readable syntax over the unittest package from the standard
library.


## Package Development and Contribution

See [cashctrl_api /CONTRIBUTING.md](https://github.com/macxred/cashctrl_api/blob/main/CONTRIBUTING.md) for:

- Setting Up Your Development Environment
- Type Consistency with DataFrames
- Standards and Best Practices
- Leveraging AI Tools
- Shared Learning thru Open Source