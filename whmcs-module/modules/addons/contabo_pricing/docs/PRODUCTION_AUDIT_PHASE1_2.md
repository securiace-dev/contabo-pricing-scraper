# Production Audit - Phases 1-2

> **HISTORICAL (harvested from PR #26, 2026-10-02).** Written for the Rust-API
> era: every `contabo-pricing:8080/api/v1`, `api_base_url`, `CONTABO_PRICING_API_BASE_URL`
> and Dokploy-sidecar step below no longer applies after PR #32 (addon-native
> scrape, `PlanSourceFactory::fromSettings()`). The audit findings and the
> operational-correction sequencing remain useful; re-derive any API/health
> probe from the Data sources / Scrape runs admin pages instead.


This note records concrete progress on roadmap steps 1-2 for the live runtime
and deploy contract, without performing any production mutation.

Scope:

1. verify and document the actual production/runtime invariants that are
   already grounded in tracked repo state;
2. call out what remains runtime-only or drift-prone because the live MCP read
   path is currently blocked; and
3. package the operational correction set for the digest fix, source-period
   fix, and bad `36mo` profile remediation.

## 1. Verified contract from tracked source

The repository's current tracked contract is internally consistent on the
points below.

- Production shape is a private Dokploy sidecar, not a public API host and not
  a native `systemd` unit.
- WHMCS is expected to reach the API over Docker DNS at
  `http://contabo-pricing:8080/api/v1`.
- Production snapshot state is expected on the host at
  `/var/lib/contabo-pricing/output`.
- The refresh bearer token is expected as a host-owned file at
  `/etc/contabo-pricing/auth_token`.
- Proxy credentials, when used, are expected as a host-owned env file at
  `/etc/contabo-pricing/proxy.env`.
- Freshness automation is still external-only. `CONTABO_REFRESH_CRON` is
  accepted as config but is not wired to an in-process scheduler.
- The addon's runtime fallback remains `http://localhost:8080/api/v1` when
  `CONTABO_PRICING_API_BASE_URL` and the saved WHMCS setting are absent, so a
  containerized production install must explicitly override that default.
- The addon database contract is currently `Installer::SCHEMA_VERSION = 14`.
- Publication/catalog tables are intentionally compatibility-gated rather than
  hard-required at runtime; `SchemaHealth` reports them as optional capability,
  not global health failure.

Primary evidence in tracked files:

- `README.md`
- `deploy/README.md`
- `SCHEMA_VERSION.md`
- `whmcs-module/modules/addons/contabo_pricing/lib/Settings.php`
- `whmcs-module/modules/addons/contabo_pricing/lib/SchemaHealth.php`

## 2. Drift and unverified live invariants

The following production truths are not proven by repo state alone and still
require operator-side host verification before any rollout:

- the exact live sidecar image digest currently running;
- the exact live saved addon `api_base_url` value, if present;
- whether production is relying on the saved addon URL or the
  `CONTABO_PRICING_API_BASE_URL` environment override;
- the exact Docker network attachment for the WHMCS app and `contabo-pricing`
  sidecar;
- whether `/etc/contabo-pricing/proxy.env` is mounted and whether the sidecar
  process actually sees `SCRAPER_PROXY`;
- the current `/api/v1/meta.snapshot_meta.generated_at` freshness timestamp;
- the current `mod_contabo_settings.schema_version` value in the live WHMCS DB;
- whether production is in publication/catalog compatibility mode or has the
  v10+ optional tables available;
- whether any manual production-side edits drift from the tracked
  `docker-compose.dokploy-sidecar.yml` shape.

### Current blocker

Available WHMCS MCP reads are only partially useful right now:

- `get_capability_matrix` works and confirms the integration knows about the
  expected read actions, including `WhmcsDetails`, `GetAutomationLog`, and
  `GetServers`;
- actual live reads attempted through `get_whmcs_details`,
  `get_server_health`, and `get_automation_log` returned HTTP `403`.

Treat that as a real evidence gap, not as proof that production is healthy or
misconfigured in any specific way. Until the `403` path is resolved, live
runtime facts must be gathered from host-side read-only checks.

## 3. Local correction set status

The two code fixes called out in this audit are already present locally.

### Daily digest fix

`DailyOperationsDigestBuilder` now:

- includes `snapshot_generated_at` in the digest body when available;
- includes the effective `api_base_url` in the digest body when available;
- groups failure lines into more operator-meaningful buckets;
- keeps repricing/renewal messaging explicitly observational when Phase B is
  not active.

Verification surface:

- `whmcs-module/modules/addons/contabo_pricing/lib/DailyOperationsDigestBuilder.php`
- `whmcs-module/modules/addons/contabo_pricing/tests/DailyOperationsDigestBuilderTest.php`

### Source-period/admin fix

`AdminController` now keeps source basis and customer-facing published cycles as
separate inputs:

- create flow reads `period_months` directly from the posted field and no
  longer derives it from the published cycle mask;
- save flow only patches `period_months` when the form actually posted it, and
  only patches `published_cycles_mask` when that field was posted;
- the save path reloads the current profile to preserve the existing source
  period when the edit did not submit a new one.

Verification surface:

- `whmcs-module/modules/addons/contabo_pricing/lib/AdminController.php`
- `whmcs-module/modules/addons/contabo_pricing/tests/AdminSourcePeriodSelectionTest.php`

## 4. Operational correction package

Prepare one approved rollout bundle, but do not apply it yet.

### Package contents

1. The addon tree containing the digest fix and the source-period/admin fix.
2. The operator remediation procedure for invalid unused `36mo` profiles in
   `docs/OPERATIONAL_CORRECTION_SET_RUNBOOK.md`.
3. A read-only runtime-proof checklist that must be completed immediately
   before rollout.
4. A post-rollout verification checklist that proves the bad-profile condition
   does not recur.

### Required preflight, in order

1. Prove the live sidecar identity and image digest.
2. Prove WHMCS-to-sidecar reachability from inside the WHMCS runtime context.
3. Read `/api/v1/meta` and record `snapshot_meta.generated_at`.
4. Record whether `SCRAPER_PROXY` is present in the sidecar runtime contract.
5. Record live addon schema version and compatibility/publication posture.
6. Inventory candidate bad `36mo` profiles and confirm they are unused before
   any cleanup action.
7. Package the exact addon artifact that contains the two local fixes.

### Bad `36mo` remediation guardrails

The invalid `36mo` cleanup must remain downstream of the source-period fix.

Why:

- `SyncEngine` still correctly errors when a profile's source
  `period_months` is not actually offered by the Contabo plan.
- Cleaning the bad rows before the source-period/admin fix is live risks
  allowing the same invalid source basis to be recreated from the UI path.

Operational rule:

1. deploy the addon artifact that contains the source-period separation fix;
2. confirm profile create/edit no longer derives source period from published
   cycles;
3. only then execute the production remediation for unused bad `36mo`
   profiles documented in `docs/OPERATIONAL_CORRECTION_SET_RUNBOOK.md`;
4. after cleanup, run an observe/sync pass and confirm no repeat
   `period ... not offered by Contabo` profile errors are emitted for that
   cohort.

## 5. Host-side read-only proof checklist

Use these checks on the production host or from the WHMCS app container. They
are read-only and safe to gather before any approved change window.

```bash
docker ps --filter name=contabo-pricing
docker inspect contabo-pricing --format '{{json .NetworkSettings.Networks}}'
docker exec whmcs-production-jvjwfo-app-1 \
  curl -fsS http://contabo-pricing:8080/api/v1/health
docker exec whmcs-production-jvjwfo-app-1 \
  curl -s http://contabo-pricing:8080/api/v1/meta | jq '.snapshot_meta.generated_at, .snapshot_meta.plan_count, .scraper_version'
ls -la /var/lib/contabo-pricing/output/
```

If the operator has safe DB read access, also capture:

```sql
SELECT value FROM mod_contabo_settings WHERE `key` = 'schema_version';
```

And, if needed to confirm compatibility/publication posture:

```sql
SHOW TABLES LIKE 'mod_contabo_catalog_versions';
SHOW TABLES LIKE 'mod_contabo_mapping_publications';
```

## 6. Done in this pass

- audited the tracked deploy/runtime contract against current repo docs;
- confirmed graph freshness against current `HEAD`;
- confirmed the local digest fix and source-period fix exist in code and tests;
- confirmed the live WHMCS MCP surface is partially blocked by HTTP `403`,
  leaving a real production-read limitation;
- wrote this phase 1-2 audit artifact so the remaining production proof work
  and the correction package are explicit.

## 7. Open blockers

- Live WHMCS/API runtime facts are not fully retrievable through MCP because
  actual reads are returning `403`.
- Production deployment and the bad-profile purge still require explicit
  operator authorization and host-side execution, even though the local
  correction package and runbook are now prepared.
