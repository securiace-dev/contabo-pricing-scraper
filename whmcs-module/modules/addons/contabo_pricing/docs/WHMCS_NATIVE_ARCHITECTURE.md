# WHMCS-native VPS architecture

## Decision

SecuriAce VPS is a standard WHMCS addon plus provisioning module, supported by
a read-oriented Rust pricing/catalog service. It is not an external Python
control plane.

```text
Contabo public pricing/configuration
             |
             v
Rust scraper + versioned catalog/quote API
             |
             v
contabo_pricing addon
  catalog -> profiles -> mappings -> publications -> sealed paid-order snapshot
             |
             v
WHMCS orders, invoices, services and cron
             |
             v
securiacevps provisioning module
  durable operation -> official Contabo Customer API -> reconciliation
             |
             v
Contabo resource observation
```

## Why MySQL and WHMCS cron

WHMCS officially loads PHP addon/provisioning callbacks and runs scheduled hook
work in the WHMCS process. The native design therefore stores jobs, leases,
attempts, provider references, snapshots, resources, sagas, and audit events in
module-owned MySQL tables and progresses them from WHMCS cron.

This supplies the durability that Redis/Celery would normally provide without
adding an unsupported availability dependency. A permanent daemon is not
required. If cron stops, persisted work remains claimable when cron resumes.

## Failure domains

| Failure | Expected behavior |
|---|---|
| Rust API unavailable | Catalog refresh/new quote may pause; existing-service lifecycle remains available |
| Addon catalog/publication failure | No new mapping is published; provisioning continues from sealed snapshots |
| Provisioning module/cron interrupted | Durable operations retain state/leases and resume or reconcile |
| Contabo Customer API unavailable | Provider mutations retry/reconcile; WHMCS billing/catalog records are not rewritten |
| Mail delivery fails | Communication state records failure; provider operation is not repeated |
| WHMCS status/billing write fails after provider success | Billing/reconciliation repair is recorded; provider mutation is not repeated |

## Runtime coupling rule

The provisioning module does not call the Rust API and does not load classes
from the addon directory. The addon and module share only versioned database
records and stable identifiers. The addon owns schema installation; the
provisioning module fails closed when required tables or columns are absent.

## UI composition

Hallmark informs hierarchy, progressive disclosure, task grouping, state
clarity, and visual restraint; it is not shipped as a dependency.

- The VPS child order form inherits WHMCS Standard Cart.
- WHMCS retains cart, authentication/session, CSRF, coupon, tax, invoice,
  payment, and service-ownership behavior.
- The client service page reads local projections and derives actions from
  certified capabilities.
- The addon workbench exposes catalog health, mappings, pricing, operations,
  adoption, reconciliation, billing repair, communications, credentials,
  maintenance, and audit.
- Styles are namespaced; no global `:root`, font, Bootstrap, or event-handler
  override is introduced.

## Product policy

- Self-Managed VPS is a distinct WHMCS product group with fixed
  `self_managed`; it has no paid management option.
- Managed VPS has exactly one required recurring management choice: Lite, Pro,
  or Enterprise.
- Upgrades to managed service may be immediate and invoiced; downgrades are
  renewal-effective.
- Duplicate legacy addon/configurable-option billing is reported for operator
  review and never silently rewritten.

## Security posture

- WHMCS server credentials are encrypted by WHMCS and redacted from all module
  surfaces.
- Every mutation has method, CSRF, administrator permission or customer
  ownership, capability, write-switch, and operation-identity checks.
- Destructive actions require verified provider ownership.
- One-time customer secrets are encrypted separately, owner-bound, expiring,
  replay-resistant, and never emailed.
- Provider responses become typed internal observations and customer-safe error
  references; raw payloads are not exposed.
- Configuration writes, publication, repricing, and operator commands are
  versioned, locked, and audited.

## Compatibility policy

- Continuous CI checks PHP syntax and WHMCS 8.13.x/9.x contracts.
- The final WHMCS 9 production qualification is a separate release gate.
- `contabo_vps` remains a compatibility shim during cohort reassignment.
- Unsupported or uncertified provider features are absent from action surfaces,
  not presented as apparently functional controls.
