# SecuriAce WHMCS-native VPS suite

This directory contains the WHMCS half of the SecuriAce VPS architecture.

```text
modules/addons/contabo_pricing       catalog, mappings, prices, snapshots, ops UI
modules/servers/securiacevps         canonical provisioning module
modules/servers/contabo_vps          staged migration compatibility shim
templates/orderforms/securiace-vps   Standard Cart child for VPS products
```

It supports WHMCS 8.13.x/9.x and PHP 7.4-compatible module syntax. Runtime
durability uses module-owned WHMCS MySQL tables and WHMCS cron. There is no
Python, PostgreSQL, Redis, Celery, or external queue dependency.

## Runtime responsibilities

### Rust pricing/catalog API

The Rust service scrapes and normalizes plans, provider SKU IDs,
configuration dimensions, prices, FX, availability, and payload hashes. It
publishes a versioned `/api/v1/catalog` contract. It does not receive WHMCS
customer credentials or manage customer VPS resources.

### `contabo_pricing` addon

The addon imports the Rust catalog and manages:

- versioned plan profiles and catalog items;
- WHMCS product/configurable-option mappings;
- customer and renewal price previews/publications;
- compatibility and capability observations;
- drift, repricing, approvals, audit, and maintenance;
- immutable paid-order snapshots;
- the provisioning operations/reconciliation workbench;
- additive installation of the shared native schema.

Rust/API unavailability may pause imports or new quotation. It does not block
an existing service because the provisioning module never calls the Rust API.

### `securiacevps` module

The canonical server module reads a sealed snapshot and calls the official
Contabo Customer API. It accepts lifecycle intent into deterministic,
MySQL-backed operations; WHMCS cron polls and reconciles them.

Supported controls appear only when provider capabilities are certified.
Uncertified resize/migration and other actions fail closed and remain absent
from customer action surfaces.

### VPS order form and service experience

`securiace-vps` inherits WHMCS `standard_cart`. It changes the VPS product and
configuration presentation only. WHMCS retains shared cart, session,
authentication, CSRF, coupons, tax, invoices, gateways, payment, fraud, and
service ownership.

The client service page renders local resource/operation projections and sends
mutations through POST + WHMCS CSRF + ownership + capability checks.

## Installation order

Build verified local artifacts:

```bash
bash scripts/predeploy-check.sh
bash scripts/package-whmcs-suite.sh
```

Then, in an authorized staging or operator-controlled rollout:

1. Install/upgrade `contabo_pricing` and activate it.
2. Confirm schema v12 health and run the migration twice to prove idempotency.
3. Keep global and per-capability provider writes disabled.
4. Import/validate a Rust catalog and publish a mapping version.
5. Install `securiacevps` and the `contabo_vps` compatibility shim.
6. Run read-only existing-service adoption and resolve conflicts.
7. Certify the minimum lifecycle in an allowlisted staging cohort.
8. Assign the `securiace-vps` order form only to VPS product groups.
9. Reassign legacy services to `securiacevps` in verified cohorts.

See the complete [release/migration runbook](modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md).
Repository scripts do not deploy production.

## Server credentials

In WHMCS server configuration:

| WHMCS field | Meaning |
|---|---|
| Username | Contabo OAuth client ID |
| Password | Contabo OAuth client secret |
| Access Hash | API user identifier and password in the documented module format |

WHMCS encrypts these fields. The module redacts credentials and nested
secret-bearing provider payloads from logs, audit metadata, operations,
communications, and customer output.

The six per-product module config options remain visible only for compatibility
and migration inventory. Native provisioning requires a sealed paid-order
snapshot and never derives the requested server from mutable current product
fields.

## Order and mapping workflow

```text
import catalog
  -> validate catalog hashes/availability
  -> create/version profile
  -> map product and option machine codes
  -> preview publication and prices
  -> publish immutable mapping version
  -> validate customer selections at checkout
  -> seal paid/fraud-eligible order snapshot
  -> create deterministic provisioning operation
```

The snapshot stores mapping, catalog, profile, provider, compatibility, price,
cart, option, and customer-visible-label identities. It is immutable after
sealing; corrections supersede it.

## Lifecycle workflow

```text
WHMCS callback/customer action
  -> authorize and verify capability/write switches
  -> create or return deterministic operation
  -> claim service lease with fencing token
  -> submit exact provider request
  -> poll/reconcile
  -> verify provider result
  -> project resource, billing saga, communication, and WHMCS status
```

WHMCS remains Pending until create readiness is verified. Suspend, unsuspend,
and terminate update commercial state only after provider verification. An
accepted request followed by timeout is `unknown_outcome`, not automatic
failure; reconciliation precedes retry.

## Existing-service adoption

Adoption is read-only and records evidence/confidence:

`verified`, `probable`, `ambiguous`, `missing_upstream`, `orphan_upstream`,
`conflict`, or `excluded`.

Destructive actions require exact provider account/resource identity, the exact
`whmcs-{serviceId}` tag, and `verified` ownership. The module never guesses,
auto-adopts, or auto-deletes an orphan.

## Product structure

- **Self-Managed VPS:** fixed `self_managed`; no paid management option.
- **Managed VPS:** exactly one required recurring management value: Lite, Pro,
  or Enterprise.
- Management upgrades may be immediate/invoiced; downgrades are
  renewal-effective.
- Legacy duplicate addon/configurable-option billing is reported for operator
  review and never silently rewritten.

## Secret delivery

Customer root credentials use encrypted temporary storage and an
authenticated, owner-bound, short-lived, one-time reveal token. Plaintext is
not stored in normal service rows, browser storage, logs, exports, audit
metadata, analytics, or email.

## Tests

- Addon: `modules/addons/contabo_pricing/vendor/bin/phpunit`
- Provisioning: from `modules/servers/securiacevps`,
  `../../addons/contabo_pricing/vendor/bin/phpunit -c phpunit.xml`
- Complete local release gate: `bash scripts/predeploy-check.sh`
- Package inspection: `bash scripts/package-whmcs-suite.sh`

The gate reports unavailable development WHMCS environments instead of
claiming production compatibility from a skipped test.

## Normative documents

- [Architecture](modules/addons/contabo_pricing/docs/WHMCS_NATIVE_ARCHITECTURE.md)
- [Provisioning contract](modules/addons/contabo_pricing/docs/PROVISIONING_CONTRACT.md)
- [Release/migration runbook](modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md)
- [Test-surface policy](modules/addons/contabo_pricing/docs/TESTING_SCOPE.md)
- [Changelog](modules/addons/contabo_pricing/CHANGELOG.md)
