# Operational Correction Set Runbook

> **HISTORICAL (harvested from PR #26, 2026-10-02).** Written for the Rust-API
> era: every `contabo-pricing:8080/api/v1`, `api_base_url`, `CONTABO_PRICING_API_BASE_URL`
> and Dokploy-sidecar step below no longer applies after PR #32 (addon-native
> scrape, `PlanSourceFactory::fromSettings()`). The audit findings and the
> operational-correction sequencing remain useful; re-derive any API/health
> probe from the Data sources / Scrape runs admin pages instead.


This runbook packages the remaining roadmap item
`ship-operational-correction-set` for the Contabo Pricing addon.

Scope:

1. ship the daily digest fix;
2. ship the source-period/admin fix;
3. execute the production-safe remediation for the bad unused `36mo` source
   profiles;
4. verify the issue does not recur.

This document prepares a release-quality operator bundle only. It does not
authorize or perform production deployment by itself.

## Package contents

The correction set consists of four parts:

1. a reviewed addon artifact containing the digest and source-period fixes;
2. the Phase 1-2 production audit note in
   `docs/PRODUCTION_AUDIT_PHASE1_2.md`;
3. this operator runbook for deploy order, remediation order, and rollback;
4. a controlled post-rollout verification checklist.

## Runtime files that must ship

Ship the current `contabo_pricing` addon artifact containing these runtime
changes:

- `hooks.php`
- `lib/DailyOperationsDigestBuilder.php`
- `lib/AdminController.php`
- `lib/CronDriver.php`

The supporting regression coverage for this package is in:

- `tests/DailyOperationsDigestBuilderTest.php`
- `tests/AdminSourcePeriodSelectionTest.php`

## Why the order matters

The invalid `36mo` cleanup must happen after the code rollout.

- `SyncEngine` still correctly rejects a profile whose source
  `period_months = 36` when Contabo does not offer a real `36` month upstream
  period.
- the old admin save path could recreate the same bad state by deriving source
  period from the longest published cycle;
- the new hook path also gives a cleaner post-change signal than the old raw
  cron email.

Operational rule:

1. ship the addon artifact first;
2. prove the new digest and source-period code paths are live;
3. only then remove the bad unused `36mo` profiles;
4. run one controlled cron/sync pass and confirm no recurrence.

## Local release preparation

Run these commands from the repo root before any production change window:

```bash
bash scripts/predeploy-check.sh
bash scripts/package-whmcs-suite.sh
```

Required outputs:

- a green predeploy gate;
- a packaged addon artifact under `dist/`;
- a checksum file for the package;
- a manifest suitable for operator evidence capture.

Do not use an untracked or partially reviewed tree as the release source.

## Pre-change production proof

Capture these read-only facts immediately before rollout.

### Host and sidecar

```bash
docker ps --filter name=contabo-pricing
docker inspect contabo-pricing --format '{{json .NetworkSettings.Networks}}'
docker exec whmcs-production-jvjwfo-app-1 \
  curl -fsS http://contabo-pricing:8080/api/v1/health
docker exec whmcs-production-jvjwfo-app-1 \
  curl -s http://contabo-pricing:8080/api/v1/meta | jq '.snapshot_meta.generated_at, .snapshot_meta.plan_count, .scraper_version'
ls -la /var/lib/contabo-pricing/output/
```

Record:

- current running sidecar/container identity;
- WHMCS-to-sidecar reachability;
- `snapshot_meta.generated_at`;
- whether the snapshot output path is present.

### Addon schema and compatibility posture

If safe DB read access is available, capture:

```sql
SELECT value FROM mod_contabo_settings WHERE `key` = 'schema_version';
SHOW TABLES LIKE 'mod_contabo_catalog_versions';
SHOW TABLES LIKE 'mod_contabo_mapping_publications';
```

### Bad-profile eligibility recheck

Before any data mutation, re-prove that the candidate bad profiles are still
unused. The audited set is exactly 16 rows and they must remain unreferenced.

Minimum eligibility for every target row:

- `period_months = 36`;
- version count = `0`;
- mapping count = `0`;
- live service count = `0`.

If the count is not 16, or any candidate now has versions, mappings, or live
service references, stop and re-audit before any cleanup.

## Backup boundary

Before rollout:

1. take a full production DB backup;
2. prove the DB backup can be restored in an isolated environment;
3. retain a filesystem copy of the currently deployed addon tree;
4. record the previous artifact checksum and the candidate artifact checksum.

Hard rule: once the bad profiles are purged, restoring only the old addon files
is not enough. Row recovery becomes a DB restore concern.

## Deploy order

### 1. Ship the addon artifact

Deploy the packaged `contabo_pricing` addon tree first.

### 2. Trigger normal schema/bootstrap path

Open the addon in WHMCS admin so `SchemaHealth::assertOrMigrate()` can apply
any additive migration work in the normal runtime path.

### 3. Verify the new runtime path is live

Confirm production now contains and is executing:

- `DailyOperationsDigestBuilder` from `hooks.php`;
- `CronDriver()->runObserveSweep()` from the daily cron hook;
- `coerceSourcePeriodMonths()` in the profile create/save path.

Stop here if the deployed code path is not proven live.

## Production remediation for the bad `36mo` profiles

### Recommended action

Purge the 16 unused bad `36mo` source profiles after the code rollout is live.

This is safer than editing them in place because in-place normalization leaves
identity drift risk between slug/name and `period_months`, while the audited
rows have no versions, mappings, or service references to preserve.

### Required sequence

1. export the exact 16 candidate profile ids and slugs;
2. if any are active in the normal listing, move them to Trash first;
3. purge each target through the guarded profile purge path;
4. use the required confirmation phrase exactly:
   `PURGE CONTABO PRICING DATA`;
5. record the post-purge remaining counts.

### What not to do

Do not:

- bulk-edit `period_months` in SQL as the first-choice fix;
- leave slug/name values implying `36mo` while changing only the period field;
- hand-delete mixed rows from WHMCS product, service, or invoice tables;
- perform cleanup before the source-period fix is live.

### Future recreation rule

If any of these source profiles are intentionally recreated later, use the
audited normalization:

- `cloud-vps-* -> 12`
- `vds-* -> 24`
- `storage-vps-* -> 24`

Customer-facing long-cycle publication can remain separate from the source
period. The profile source basis is not the same thing as the set of offered
billing cycles.

## Controlled post-rollout verification

Run one controlled daily cron/sync pass after deploy and after the purge.

Verify all of the following:

1. the new notification subject is shaped like
   `Contabo Pricing daily digest [...]`;
2. the message body begins with `Unified daily Contabo operations digest`;
3. the digest includes the repricing observation block;
4. sync no longer fails on `period ... not offered by Contabo` for the old
   `36mo` cohort;
5. no new `36mo` source profiles are recreated for `cloud-vps-*`, `vds-*`, or
   `storage-vps-*`;
6. no unexpected new versions or mappings were created for the removed rows.

The correction set is successful only when the new code path is live, the old
email shape is gone, the 16 bad profiles are gone, and the first controlled run
shows no recurrence.

## Rollback

### Before bad-profile purge

If deploy verification fails before data cleanup:

1. restore the previous addon artifact/tree;
2. clear or reload PHP runtime caches as required by the environment;
3. do not proceed to profile cleanup.

### After bad-profile purge

If the profiles have already been purged and rollback is required:

- code rollback alone is insufficient;
- restore the purged rows only from the tested DB restore path.

## Explicit go/no-go boundaries

Go only when all of the following are true:

- local gate and packaging completed successfully;
- production runtime proof was captured;
- the new hook/admin code path is proven live;
- the candidate bad-profile set is still exactly 16 and still unused;
- a tested DB rollback path exists.

No-go if any proof above is missing or contradictory.
