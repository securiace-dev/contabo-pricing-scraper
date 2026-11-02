# Upgrade runbook: add-on 1.1.3

Applies to an installation that already runs 1.1.2 and does not yet enforce sealed
paid-order snapshots (schema 16 not applied, or no published mapping). Do not deploy
without an explicit go.

## Why order matters

`PreModuleCreate` refuses to create a VPS unless the service has a sealed paid-order
snapshot. Snapshots are drafted at checkout and sealed on payment, which needs schema 16
and a published mapping. Checkout for native VPS products is blocked until both exist
(`CheckoutGuard`). Orders paid **before** that point have no snapshot and, without 1.1.3,
would fail closed on module create.

## Steps (in this order)

1. **Back up** `mod_contabo_*`, `mod_securiacevps_*`, `tblpricing`, and keep a hashed copy of
   the previous add-on tree.
2. **Copy the 1.1.3 files**, then **open the add-on admin page once**. This runs
   `contabo_pricing_upgrade()` and `SchemaHealth::assertOrMigrate()`:
   - applies schema 16 (if not yet applied);
   - pins `mod_contabo_settings.snapshot.grandfather_before` to the current time
     (insert-if-absent, never moved forward by a re-run).
   Confirm `schema_version = 16` and that the setting exists. Orders placed before this
   timestamp are the only ones that can be grandfathered.
3. **Publish the mapping** for each VPS product (Mappings, review, publish). Until a mapping
   is published, VPS checkout stays blocked by design. Optionally hide the product group
   during steps 2-3.
4. **Enable**: verify with one test order that a draft is created at checkout and sealed on
   payment, then reopen the products.
5. **Drain the backlog.** Pre-cutoff paid orders provision normally; each one writes
   `WARNING: Contabo provisioning gate allowed ... grandfathered_pre_snapshot_paid_order`
   to the Activity Log (and the module log). When none are left, set
   `snapshot.grandfather_enabled = 0` to remove the exception.

## Behaviour of the gate

| Service state | Result |
|---|---|
| Sealed snapshot exists | allowed |
| No snapshot row of any state, order dated before `snapshot.grandfather_before`, invoice Paid, order not fraud/cancelled, grandfathering enabled | allowed once, `WARNING` logged |
| Order at or after the cutoff, unpaid, no order, or any non-sealed snapshot row (draft, invalid, superseded) | **aborted**, reason logged |
| Cutoff missing, grandfathering disabled, or the lookup throws | **aborted**, reason logged |

Every abort logs `Contabo provisioning gate ABORTED module create: service #N module M code C: detail`
to the Activity Log and writes a `PreModuleCreate` entry to the module log.

## Rollback

Restore the previous add-on tree. Schema 16 is additive; the grandfather settings are inert
without 1.1.3.
