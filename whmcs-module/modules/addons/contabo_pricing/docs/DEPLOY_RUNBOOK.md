# SecuriAce WHMCS-native VPS release and migration runbook

This runbook prepares and verifies a release. It deliberately contains no
automatic production deployment. Applying an artifact to production is a
separate operator-controlled change with explicit authorization, target
resolution, maintenance/rollback ownership, and evidence capture.

The release contains three independently versioned artifacts:

1. Rust pricing/catalog API.
2. `contabo_pricing` WHMCS addon.
3. SecuriAce VPS suite: canonical `securiacevps` module, `contabo_vps`
   compatibility shim, and `securiace-vps` Standard Cart child order form.

## Gate 0: establish repository and deployment truth

Before migration:

- identify the exact deployed Rust binary, addon tree, server-module tree,
  hooks, order-form templates, cron entrypoints, and database schema version;
- hash deployed and candidate files without reading secret values;
- diff tracked source against the deployment;
- record every product and service assigned to `contabo_vps` or
  `securiacevps`;
- record custom fields, product config fields, mapping/publication identities,
  active hooks, email templates, and module-owned table counts;
- prove a compatible database backup can be restored in an isolated
  environment;
- retain the previous code/package artifact and its checksum.

Do not use an untracked workstation tree as a release source.

## Gate 1: build and verify locally

Run:

```bash
bash scripts/predeploy-check.sh
bash scripts/package-whmcs-suite.sh
```

The gate must prove:

- addon and canonical module unit/contract suites pass;
- the compatibility shim loads and delegates;
- PHP syntax is valid on the supported matrix;
- local WHMCS 8.13.x and 9.x schema/integration checks pass when those
  environments are available;
- the Rust service passes format, tests, check, and Clippy;
- the package contains runtime files only and no tests, caches, VCS state,
  development dependencies, credentials, or local tooling state.

The package script writes an archive, a SHA-256 file, and a sorted manifest
under `dist/`. It does not connect to any host.

## Gate 2: stage additive schema and safety controls

In an isolated restore or staging environment:

1. Install/upgrade the addon first so schema v12 tables exist.
2. Confirm the migration is additive, idempotent, and safe to run twice.
3. Confirm the previous application artifact can run against the upgraded
   schema before declaring code rollback safe.
4. Set the global provider-write switch off.
5. Set every mutation capability to `not_certified` or disabled.
6. Confirm read-only catalog import, schema health, provider-account health,
   operations workbench, and reconciliation pages.
7. Confirm environment and WHMCS installation identities.
8. Confirm lifecycle email templates are present and use synthetic recipients.

No provider write is permitted in this gate.

## Gate 3: import catalog and publish mappings

- Import a versioned Rust catalog and verify payload hashes.
- Validate stable plan/profile, provider SKU, dimension, and value identities.
- Preview product/configurable-option changes before publication.
- Publish a staging mapping version under the named publication lock.
- Repeat the same publication and prove it is a no-op.
- Confirm a changed payload creates a new version rather than mutating the old
  one.
- Verify self-managed and managed product groups, management-tier rules,
  currencies, cycles, setup/renewal amounts, tax policy, and decimal rounding.
- Exercise quote expiration and re-quotation.

## Gate 4: existing-service adoption

Run the read-only adoption inventory before customer service controls are
exposed:

- exact resource-ID matches;
- exact `whmcs-{serviceId}` tags;
- provider account, IP, creation-time, SKU, and region evidence;
- duplicate mappings;
- WHMCS services with no provider resource;
- provider resources with no WHMCS service;
- drift/conflict findings and confidence.

An operator must approve every non-exact match. Only `verified` adoption enables
destructive actions. Record baseline orphan and drift totals.

## Gate 5: staged module-name migration

Never replace the old module name as one unverified rename.

1. Install `securiacevps` beside `contabo_vps`.
2. Install the new `contabo_vps` shim; it contains delegation only.
3. Verify existing test services through both entrypoints.
4. Produce a dry-run reassignment report.
5. Reassign an allowlisted test cohort to `securiacevps`.
6. Verify client page, admin page, cron, create, inspect, reconciliation, and
   certified lifecycle callbacks.
7. Expand cohorts only after evidence is clean.
8. Retain the shim for at least one rollback window after the final
   reassignment.
9. Remove it only after searches of products, services, hooks, scripts, logs,
   package builders, templates, and documentation prove no remaining runtime
   reference.

Rollback reassigns the cohort to the compatibility entrypoint and restores the
previous code artifact. It does not reverse an additive database migration
unless an independently tested database restore is required.

## Gate 6: dark-launch provider writes

Use a staging provider account and allowlisted WHMCS customers/products only.

1. Certify provider capabilities and rate/error/timeout behavior.
2. Enable only `inspect` and the single action under test.
3. Prove duplicate callback and manual/cron overlap create one operation.
4. Prove a crash after provider acceptance reconciles the existing resource.
5. Prove a stale worker cannot overwrite a newer fencing token.
6. Prove cancellation during provisioning has a deterministic recovery path.
7. Prove the global write switch halts new mutations while inspection remains.
8. Inspect attempts, correlation IDs, billing sagas, communications, and safe
   customer error references in the workbench.

## Gate 7: certify minimum lifecycle

Public checkout remains disabled until evidence covers:

- create and readiness verification;
- inspect/reconcile;
- suspend and unsuspend;
- terminate and absence verification;
- failed-delete orphan detection;
- power and password reset where certified;
- failed/unknown operation recovery;
- billing-persistence failure after provider success;
- one-time secret reveal, expiry, replay prevention, and redaction.

## Gate 8: configure the VPS order-form child

Place `templates/orderforms/securiace-vps` under the WHMCS template root and
assign it only to the VPS product groups. Its `theme.yaml` inherits
`standard_cart`.

The child wraps product discovery and configuration only. WHMCS continues to
own shared cart review, login/session, coupons, tax, invoice creation, gateways,
payment, and checkout. If rollback is needed, restore the product-group order
form to `standard_cart`; no order or invoice data changes.

Verify:

- unavailable/deprecated catalog items fail before order creation;
- every selected option resolves to an enabled published machine code;
- management-tier rules match the selected product group;
- price, renewal, tax, discount, and quote-expiry summaries are truthful;
- a paid/fraud-eligible order seals one immutable snapshot;
- invalid or expired snapshots cannot provision.

## Gate 9: release evidence and go/no-go

The evidence pack must contain:

- commit SHA, artifact versions, manifests, and SHA-256 checksums;
- repository status and source/deployment hashes;
- migration install/upgrade/repeat/rollback results;
- PHP, WHMCS, Rust, unit, integration, security, and accessibility results;
- capability certification and write-switch snapshot;
- service adoption, conflict, drift, and orphan counts;
- operation concurrency/unknown-outcome proofs;
- billing and credential-reveal proofs;
- staging resource cleanup/reconciliation report;
- rollback owner, release owner, reviewer, and go/no-go authority.

Any failed invariant is a no-go. A missing environment check is `unverified`,
not pass.

## Artifact layout

The SecuriAce VPS suite archive lays down:

```text
modules/
  servers/
    securiacevps/
    contabo_vps/
templates/
  orderforms/
    securiace-vps/
```

The addon archive lays down:

```text
modules/
  addons/
    contabo_pricing/
```

The addon must be installed/upgraded before the server module is enabled because
it owns the shared schema. Runtime packages exclude tests, `vendor/`, internal
deployment documents, caches, Composer development files, `.git`, graph data,
and local agent/tool state.

## Rollback boundaries

- **Rust catalog outage:** keep last verified imported catalog; pause refresh
  and new quotation. Existing service lifecycle remains available.
- **Addon UI/catalog regression:** restore prior addon code; pause publication.
  Provisioning uses sealed snapshots and existing service projections.
- **Provisioning regression:** disable global provider writes, let inspection
  and workbench remain available, restore the previous server-suite artifact,
  and reconcile submitted operations before accepting new ones.
- **Order-form regression:** reassign the VPS product groups to
  `standard_cart`.
- **Provider ambiguity:** do not roll forward or back blindly; reconcile by
  operation, request, exact tag, and provider resource identity.
- **Database incompatibility:** use only the tested restoration procedure; do
  not hand-delete module rows or WHMCS product/service records.
