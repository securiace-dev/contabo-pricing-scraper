# SecuriAce VPS provisioning contract

This document is the normative runtime contract for the WHMCS-native VPS
suite. The canonical server module system name is `securiacevps`.
`contabo_vps` is a temporary compatibility entrypoint that delegates to the
same implementation while existing services are reassigned in cohorts.

The supported runtime is WHMCS 8.13.x or 9.x with PHP 7.4-compatible module
syntax. WHMCS MySQL and WHMCS cron provide persistence and background progress.
Python, PostgreSQL, Redis, Celery, FastAPI, and permanent worker daemons are not
runtime dependencies.

## 1. Component and authority boundaries

| Component | Owns | Must not own |
|---|---|---|
| Rust pricing/catalog API | Normalized plans, profiles, provider identifiers, configuration dimensions, price observations, FX observations, availability, versioned catalog payloads | WHMCS customers, orders, invoices, service ownership, provider mutations |
| `contabo_pricing` addon | Catalog imports, profiles, WHMCS product/option mappings, price publications, drift/repricing decisions, immutable order snapshots, operator workbench | Direct customer resource creation or deletion |
| `securiacevps` provisioning module | Lifecycle intents for WHMCS services, provider observations, durable operations, ownership/adoption, reconciliation, customer service actions | Catalog scraping, invoice calculation, mutable remapping of paid orders |
| WHMCS core | Customer identity, service ownership, orders, invoices, transactions, credits, refunds, commercial service state, authentication, CSRF, coupons, tax, payment gateways | Provider-resource truth |
| Contabo Customer API | Actual provider resource and request observations | WHMCS billing or customer authority |

The Rust API may be unavailable without affecting inspection or management of
an already-provisioned service. The provisioning module reads a sealed snapshot
and calls the Contabo Customer API directly. It never loads addon PHP classes;
the two modules share documented MySQL records and stable machine identifiers.

## 2. Independent state machines

No single status column represents the system.

### WHMCS commercial service

```text
Pending -> Active -> Suspended -> Active
Pending/Active/Suspended -> Terminated
Pending/Active/Suspended -> Cancelled
```

This is the customer-visible commercial state. It changes only after the
provider effect required by the callback has been verified.

### Provisioning

```text
not_requested -> queued -> creating -> configuring -> verifying -> ready
                                      \-> retry_scheduled
                                      \-> failed_retryable
                                      \-> failed_terminal
                                      \-> unknown_outcome
                                      \-> manual_review
```

### Provider resource

```text
unknown | creating | running | stopped | suspended | deleting | deleted |
provider_error
```

This is an observation, not an instruction. A service can be commercially
Active while its resource is temporarily stopped.

### Billing

```text
unpaid | paid | overdue | refund_pending | refunded | chargeback |
cancelled
```

Payment eligibility is checked by WHMCS before the create intent is accepted.
A paid invoice does not mean the provider resource is ready.

### Operation

```text
accepted -> claimed -> submitted -> provider_pending -> reconciling -> succeeded
```

Alternative states are `retry_scheduled`, `failed_retryable`,
`failed_terminal`, `unknown_outcome`, `manual_review`, `cancelled`, and
`superseded`.

## 3. Lifecycle transition matrix

| Action | Preconditions | WHMCS state while running | Provider effect | Verified success | Failure/compensation |
|---|---|---|---|---|---|
| Create | Paid/fraud-eligible service; sealed snapshot; published mapping; certified create; writes enabled | Pending | Create exact snapshot configuration | Resource identity, strict tag, expected SKU/configuration, and network/readiness observations persist; then Active | Retry classified transient errors; inspect ambiguous outcomes before retry; terminal/unknown remains Pending with operator action |
| Suspend | Verified ownership; certified suspend; no conflicting mutation | Prior commercial state | Certified provider suspend/stop behavior | Provider observation confirms policy; then Suspended | Retain prior commercial state and reconcile |
| Unsuspend | Verified ownership; certified unsuspend; no conflicting mutation | Suspended | Certified provider start/unsuspend behavior | Provider observation confirms policy; then Active | Remain Suspended and reconcile |
| Terminate | Verified ownership; certified delete; typed/operator authorization where applicable | Prior commercial state | Delete/cancel exact resource | Absence/deletion is verified; then Terminated | Create orphan-risk reconciliation; never report fully terminated while upstream remains |
| Power | Verified ownership; certified action | No commercial change | Start/stop/reboot | Persist provider observation | Retry/reconcile; never reinterpret technical stop as billing suspension |
| Reinstall/reset | Verified ownership; certified capability; owner/admin authorization | No commercial change | Exact certified request | Read-back/request completion verified; one-time secret created where applicable | Unknown outcome/manual review; never reveal or log provider payloads |
| Snapshot create | Verified ownership; certified snapshot write; valid name/description | No commercial change | Create against the exact owned resource | Snapshot identity appears in the complete provider inventory and local projection | Reconcile by exact Contabo request audit before any replay |
| Snapshot delete | Verified ownership; snapshot exists in the local provider-account projection; typed confirmation | No commercial change | Delete the exact snapshot | Snapshot is absent from a complete refreshed inventory | Preserve the existing intent and reconcile; never blindly repeat an ambiguous delete |
| Snapshot rollback | Verified ownership; snapshot exists in the local provider-account projection; typed confirmation acknowledging newer snapshots are deleted | No commercial change | Roll back the exact snapshot | Exact request audit exists and the complete inventory is refreshed | Keep the operation pending or manual review; never claim completion from the HTTP acknowledgement alone |
| Change package | Sealed superseding snapshot; certified resize/migration; billing saga accepted | Current package | Provider resize or migration | Provider and WHMCS billing projections reconciled before completion | Do not repeat provider change after billing persistence failure; expose billing repair |

Uncertified features do not appear as customer or administrator controls.
`ChangePackage` currently fails closed until resize/migration certification and
the complete billing saga are enabled.

## 4. Sealed paid-order snapshot

Native creation never reads mutable current catalog data or legacy product
config fields. The addon creates a draft snapshot, recalculates it after any
material cart change, and seals it only after payment and fraud eligibility.

The sealed record includes:

- WHMCS installation, order, service, product, and product-group identities.
- Snapshot UUID, publication/mapping version, Rust catalog version, pricing
  profile version, compatibility version, and payload hashes.
- Provider SKU, region, image, storage, backup, IP, bandwidth, control panel,
  management, and every exposed configurable-option machine code.
- Billing cycle, currency, setup/recurring/renewal decimal amounts, discount,
  coupon, tax policy, cart-total hash, and price hash.
- Customer-visible labels, quote creation/expiry, and payment/seal timestamps.

Sealed rows are immutable. Corrections create a superseding snapshot. The same
snapshot UUID is part of deterministic operation identity; a current mapping
change cannot alter a paid order.

## 5. Operation identity, idempotency, and concurrency

Deterministic command identity is derived from:

```text
environment
+ WHMCS installation identity
+ service ID
+ sealed snapshot UUID
+ operation type
+ operation generation
```

Each operation persists its UUID, service/snapshot IDs, provider account,
operation type and state, command ID, request fingerprint, idempotency
identity, provider reference, attempt count, retry schedule, lease owner and
expiry, fencing token, safe error code, retry classification, unknown-outcome
flag, correlation ID, payload, and timestamps.

Rules:

1. The same command and payload returns the existing operation.
2. The same command with a different payload is rejected.
3. Two simultaneous callbacks create one local operation.
4. One mutating operation can hold a service lease at a time.
5. A worker must present the current fencing token before updating state.
6. An expired worker cannot overwrite a newer worker.
7. Manual and cron retries use the same operation identity.
8. Provider acceptance followed by timeout becomes `unknown_outcome`.
9. Unknown create/delete outcomes are inspected before any replay.
10. A successful provider effect is not repeated to repair a failed WHMCS
    billing or status projection.
11. The internal deterministic identity is mapped to a stable UUIDv4-form
    `x-request-id` before calling Contabo. The internal identity remains the
    WHMCS idempotency key; the UUID is the exact provider-audit identity.
12. A 429/5xx or transport ambiguity on a provider mutation is returned to the
    durable operation immediately. Only read-only requests retry inside the
    HTTP client.
13. A newer create/suspend/unsuspend/terminate intent supersedes and fences
    older non-terminal commercial intents. Projection is serialized on the
    WHMCS service row and accepts only the documented predecessor state.

WHMCS cron claims bounded due work, polls provider requests, classifies retries,
releases expired leases, reconciles ambiguity, progresses cancellation, and
emits operator findings. Cron interruption cannot create a new intent and
cannot permanently abandon a lease. Operator-command and customer-communication
claims also carry an opaque fencing token and expiry so a crashed worker can be
reclaimed without allowing its later completion write to win.

## 6. WHMCS callback semantics

### `CreateAccount`

1. Validate module product, service identity, payment/fraud eligibility, and
   global/capability write switches.
2. Load and verify the sealed snapshot and its published mapping hashes.
3. Create or return the deterministic create operation.
4. Submit only if no equivalent provider request or adopted resource exists.
5. Persist the request reference before progressing.
6. Verify resource identity, strict tag, configuration, and readiness.
7. Return `success` only after the verified result has been persisted and the
   service is Active.

If work is still pending, the callback returns a sanitized, non-final module
message and WHMCS remains Pending. Cron resumes the same operation. A terminal
or unknown result creates a customer-safe reference and an operator next action;
it does not silently refund, cancel, or issue a second create.

### Suspend, unsuspend, and terminate

These callbacks return `success` only after the provider effect is verified and
the commercial state projection succeeds. Ambiguous responses preserve the
prior WHMCS state. Termination is not complete until deletion/absence is
verified; failed deletion produces an orphan-risk finding.

### Customer and administrator actions

All mutations use POST, WHMCS CSRF validation, server-side role/ownership
checks, certified capabilities, the same durable operation engine, and typed
confirmation for destructive actions. Client pages render local projections
only, so a provider outage does not turn a page view into a remote operation.

Snapshot inventory refresh is an explicit read operation. It fetches and
validates every provider page before atomically replacing the local projection.
Create, delete, and rollback persist a provider-request record before the
network mutation. An ambiguous create reconciles through Contabo's exact request
audit and inventory; it cannot issue a second POST. Rollback confirmation
states that Contabo deletes snapshots newer than the selected restore point.

## 7. Capability certification and kill switches

Capabilities are stored per provider account/action with one of:

```text
supported | unsupported | read_only | requires_polling |
requires_manual_action | not_certified
```

Certification records cover authentication scopes, API version, request IDs,
create semantics, provider idempotency, rate limits, pagination, timeouts,
eventual consistency, error taxonomy, SKU/region availability, read-after-write,
power/suspension/deletion semantics, reinstall, password reset, console tokens,
snapshot quotas, reverse DNS, resize/migration, storage restrictions, and
maintenance/capacity errors.

The current certified snapshot contract is the official Contabo Customer API:
list/create/get/delete/rollback under
`/v1/compute/instances/{instanceId}/snapshots`, with request-audit lookup used
for ambiguous mutations. Console/VNC, per-instance telemetry, and package
resize/migration have no certified callable endpoint in the current official
contract and therefore remain absent. Reverse DNS and private networking have
documented endpoints but remain unavailable until their complete WHMCS
authorization, validation, billing, and recovery workflows are certified.

The global provider-write switch and individual capability switches fail
closed. Read-only inspection, reconciliation, and customer local projections
remain available while writes are disabled.

## 8. Ownership and existing-service adoption

The exact provider tag is `whmcs-{serviceId}`. Word-boundary or partial matches
are insufficient. Destructive actions require:

- a module resource projection;
- a provider resource ID;
- matching provider account;
- exact service tag;
- a `verified` adoption/ownership result;
- no unresolved conflict or duplicate mapping.

The read-only adoption scan matches explicit resource IDs first, then records
IP, creation time, SKU, region, and metadata evidence. Results are `verified`,
`probable`, `ambiguous`, `missing_upstream`, `orphan_upstream`, `conflict`, or
`excluded`. Only `verified` records enable destructive self-service. Provider
resources with exact WHMCS tags but no service projection are reported as
orphans; they are never automatically adopted or deleted.

## 9. Billing sagas

Database transactions can protect local MySQL writes but cannot roll back
Contabo side effects. Provider-affecting billing changes therefore have a
durable saga projection.

- Provider success plus failed WHMCS persistence records `billing_repair`;
  retry repairs WHMCS without repeating the provider action.
- Paid create plus failed local resource persistence reconciles by command,
  request, tag, and provider identity before any new request.
- Failed provider deletion retains an upstream-liability/orphan-risk finding.
- Refund, reversal, chargeback, overdue, grace-period, and fraud outcomes remain
  explicit billing states; none implicitly creates or destroys a provider
  resource.
- All monetary values are decimal strings and WHMCS remains the invoice,
  transaction, credit, coupon, tax, and refund authority.

## 10. Credential and secret policy

WHMCS server credentials remain in encrypted WHMCS server configuration. They
are never copied into snapshots, operation payloads, client state, exports,
audit metadata, or logs.

Customer root credentials use encrypted temporary storage with an application
key, an authenticated owner-bound reveal token, short expiry, limited views,
`Cache-Control: no-store`, and destruction after reveal/expiry. Logs record
that a secret was issued or revealed, never its value. Plaintext credentials
are not emailed. Support roles cannot reveal a customer's secret by default.

## 11. Module-owned tables

The addon migration owns the shared native schema:

- `mod_securiacevps_schema`
- `mod_securiacevps_order_snapshots`
- `mod_securiacevps_resources`
- `mod_securiacevps_operations`
- `mod_securiacevps_operation_attempts`
- `mod_securiacevps_provider_requests`
- `mod_securiacevps_service_locks`
- `mod_securiacevps_capabilities`
- `mod_securiacevps_reconciliation`
- `mod_securiacevps_adoption`
- `mod_securiacevps_billing_sagas`
- `mod_securiacevps_audit_events`
- `mod_securiacevps_operator_commands`
- `mod_securiacevps_secrets`
- `mod_securiacevps_communications`
- `mod_securiacevps_snapshot_inventory`

Catalog versions and items remain addon-owned (`mod_contabo_catalog_versions`,
`mod_contabo_catalog_items`) alongside the existing profile, mapping,
publication, repricing, approval, sync, and audit records.

## 12. Release invariants

A release is blocked unless evidence proves:

- one paid-order intent can create at most one provider resource;
- ambiguous acceptance is never treated as definite failure;
- mutable mappings cannot change a sealed order;
- readiness is verified before Active;
- deletion is verified before Terminated;
- destructive actions require verified ownership;
- each provider mutation has one operation, command, and correlation identity;
- failed/unknown operations have a safe operator next action;
- manual/cron overlap cannot duplicate effects;
- an ambiguous snapshot mutation cannot cause a second provider mutation;
- snapshot rollback cannot proceed without verified local ownership evidence
  and explicit acknowledgement that newer snapshots are deleted;
- Rust catalog outage cannot block existing-service lifecycle operations;
- provider credentials, raw provider responses, and plaintext customer secrets
  do not appear in logs, exports, audit metadata, or customer UI;
- public checkout remains disabled until create, inspect, reconcile, suspend,
  unsuspend, terminate, and failure recovery are certified.
