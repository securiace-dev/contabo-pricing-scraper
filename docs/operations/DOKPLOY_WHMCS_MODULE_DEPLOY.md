# Dokploy / whmcs-prod-new — WHMCS module ship path

**Status:** operator guidance grounded in what this cloud environment could
prove. **No production deploy is performed from this document.**

**Authoritative deploy target:** Dokploy-hosted WHMCS on the WHMCS prod server
(alongside other prod apps). FastPanel and `root@195.7.4.219` rsync are
**retired** and must not be used as the ship path.

**Source of truth for the full pipeline** (not mounted here):

- Local checkout: `/Users/kritananda/Projects/whmcs-prod-new`
- Related private repos (Gmail/GitHub notifications; not cloneable with the
  current agent token): `yashodhank/whmcs-prod`,
  `securiace-dev/securiace-whmcs-prod`

Until those trees are shared into the agent environment, treat the steps below
as the best-known contract and the blocked-files list as the wiring gap.

---

## What this repo produces (matches the module-admission layout)

This repository is **WHMCS modules only**. After PR #32 there is **no** Contabo
pricing Rust sidecar to publish.

Build verified artifacts:

```bash
bash scripts/predeploy-check.sh          # fail-closed gate
bash scripts/package-whmcs-suite.sh --all
```

Outputs under `dist/` (plus `.sha256` and `.manifest`):

| Archive | Layout inside zip (WHMCS document root relative) |
|---|---|
| `contabo_pricing-v*.zip` | `modules/addons/contabo_pricing/` |
| `securiacevps-v*.zip` | `modules/servers/securiacevps/`, `modules/servers/contabo_vps/`, `templates/orderforms/securiace-vps/` |

That path layout is the same family admitted by the `yashodhank/whmcs-prod`
release gate (`is_supported_module_file`: `modules/addons/*/*`,
`modules/servers/*/*`, gateways/registrars/reports/widgets). Do not invent a
different tree or rsync loose files onto a FastPanel host.

`scripts/deploy.sh` intentionally exits without connecting to any host.

---

## How whmcs-prod-new / Dokploy deploys WHMCS modules (known shape)

Evidence comes from Gmail PR notifications and historical notes in this repo,
not from a live clone of `whmcs-prod-new`.

### Two cooperating private systems

1. **`yashodhank/whmcs-prod`** — sealed WHMCS vendor image / recovery /
   module-artifact admission:
   - Exact vendor archives (content + marker), SHA-256 gates.
   - Module zip contents must land under supported `modules/<type>/<name>/…`
     paths (and related widget/report/gateway/registrar paths).
   - Persistent vendor overrides under `vendor/whmcs` with checksum
     expected-source / expected-current publication locks.
   - Offline recovery generation from a signed archive; DB contract targets
     Compose MariaDB (`db_host=mariadb`); admin path from
     `customadminpath`.
   - Document root contract: `/var/www/html`; private root:
     `/var/www/private`.

2. **`securiace-dev/securiace-whmcs-prod`** — Dokploy first-publication /
   instance contract for the live WHMCS app:
   - `instance.yaml` asserts PHP major.minor, document root `/var/www/html`,
     and admin path **`/shriram`** (not `/admin`).
   - `scripts/finalize-dokploy-first-publication.sh` polls Dokploy deployment
     status, then gates on Docker volume consumers
     (`PRIVATE_VOLUME`, `DATABASE_VOLUME`) and steady container identities.
   - Live app containers have historically looked like
     `whmcs-production-*-app-1` (see older ops notes).

### Operator apply sequence (module package → Dokploy WHMCS)

Until `whmcs-prod-new` scripts are available, operators should:

1. Run the gate + package in this repo; retain `dist/*.zip`, `.sha256`,
   `.manifest`, and the git SHA.
2. Hand the addon (and suite, if shipping) archives to the **whmcs-prod-new /
   securiace-whmcs-prod** admission path — the same path used for other
   verified module artifacts (not FastPanel rsync).
3. Apply via that repo’s reviewed publication / Dokploy upgrade flow so the
   files land inside the WHMCS app volume at
   `/var/www/html/modules/...` (and templates for the suite).
4. Open `https://my.securiace.com/shriram/addonmodules.php?module=contabo_pricing`
   so `contabo_pricing_upgrade()` / `SchemaHealth::assertOrMigrate()` run.
5. Capture evidence: `AdminController::VERSION`, `schema_version`, sync
   observe-only preview. See addon release traps in
   [`DEPLOY_RUNBOOK.md`](../../whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md).

Never treat an untracked workstation tree or a direct SSH file copy onto a
retired FastPanel host as the release source.

---

## Standalone containers on the Dokploy WHMCS prod server

| Workload | Posture after PR #32 |
|---|---|
| Contabo Rust pricing API / Dokploy sidecar (`contabo-pricing:8080`) | **Obsolete.** Addon-native scrape + hosted extractors; do not redeploy the sidecar for pricing. |
| WHMCS app + MariaDB | Dokploy-managed compose/stack (whmcs-prod-new / securiace-whmcs-prod). |
| Other prod apps | Separate Dokploy applications on the same host/network — not owned by this repo. |
| Optional future Contabo helpers (e.g. offline scrapers) | Only if explicitly reintroduced; would be a **separate Dokploy app** beside WHMCS, never FastPanel packages, never required for the current addon. |

Historical sidecar compose / host paths
(`docker-compose.dokploy-sidecar.yml`, `/var/lib/contabo-pricing`,
`/etc/contabo-pricing/*`) remain in archived audit docs as **HISTORICAL** only.

---

## FastPanel retirement

| Retired | Replacement |
|---|---|
| FastPanel2 on `195.7.4.219` as deploy host | Dokploy WHMCS prod stack |
| `rsync` of addon trees to `root@195.7.4.219` | Package zip → whmcs-prod-new / Dokploy module admission |
| `/opt/php74/bin/php` as the prod apply tool | PHP inside the Dokploy WHMCS app container (`instance.yaml` version) |
| SSH “server-side render” via FastPanel for visual QA | Prefer local `scripts/local-whmcs.sh`; prod only when explicitly asked, via HTTPS admin URLs |

PHP **7.4 polyglot** source constraints in this repo remain as a compatibility
floor until `instance.yaml` + CI explicitly raise the floor; they are not a
FastPanel deploy requirement.

---

## Blocked: files/paths still needed to finish script wiring

Share or mount any of the following to replace “point at whmcs-prod-new” with
concrete script names and compose stubs in this repo:

1. `/Users/kritananda/Projects/whmcs-prod-new` (or a tarball/clone) — README,
   deploy scripts, Dokploy compose / `instance.yaml` patterns.
2. Clone or export of `securiace-dev/securiace-whmcs-prod` — especially
   `instance.yaml`, `scripts/finalize-dokploy-first-publication.sh`,
   `scripts/lib/common.sh`, hardening contract tests.
3. Clone or export of `yashodhank/whmcs-prod` — module-admission helpers
   (`is_supported_module_file`, vendor override publisher), release README.
4. Dokploy project/service identifiers and the exact operator command that
   admits a `contabo_pricing-v*.zip` / `securiacevps-v*.zip` into the live
   WHMCS volume.
5. Confirmation whether module apply is image rebuild, volume overlay, or
   vendor-override publication (Gmail evidence exists for all three concepts
   in the private repos; which one is live for modules is not proven here).

Until then, this repo’s safe automation stops at **gate + package**. Production
apply stays operator-controlled inside whmcs-prod-new / Dokploy.
