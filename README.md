# SecuriAce VPS — WHMCS-native Contabo pricing and provisioning

This repository is a WHMCS-modules-only codebase. Its runtime is PHP inside WHMCS;
there is no separate service, container or database to operate.

> The Rust scraper/catalog service (`contabo-scraper`, `/api/v1/*`) was removed on
> 2026-10-01. The last commit that still contains it is `aa275b6`
> (`git show aa275b6:README.md` for its old operations docs).

## What is here

- `whmcs-module/modules/addons/contabo_pricing` — the pricing addon (schema 15).
  Acquires upstream Contabo prices, versions and approves catalog snapshots,
  publishes WHMCS products/config options/prices, converts FX, quotes, and seals
  paid-order snapshots.
- `whmcs-module/modules/servers/securiacevps` — canonical provisioning module
  (module-owned MySQL operations, WHMCS cron).
- `whmcs-module/modules/servers/contabo_vps` — staged migration shim only.
- `whmcs-module/templates/orderforms/securiace-vps` — Standard Cart child template.
- `tests/fixtures/api/v1.1/` — frozen catalog/quote/plans fixtures (including
  `*.rust-generated.json` parity vectors) used by the addon's PHPUnit contract tests.

## How upstream prices are acquired

Contabo's site is bot-protected, so the addon does not scrape from the WHMCS host.
It calls hosted extraction providers configured inside the addon (AlterLab, treg,
TinyFish) from the addon admin UI / cron, validates and diffs the result, and
requires human approval before anything is published. Provider selection and the
spike results are in [`docs/spike0-2026-10-01.md`](docs/spike0-2026-10-01.md).

## Run the tests

```bash
cd whmcs-module/modules/addons/contabo_pricing
composer install
vendor/bin/phpunit -c phpunit.xml --do-not-cache-result

# full local release gate (PHPUnit for both modules, PHP lint matrix, Hallmark
# audit, package contract, dev-WHMCS smokes); never touches production
bash scripts/predeploy-check.sh
```

## Release packaging

`bash scripts/package-whmcs-suite.sh --addon|--suite|--all` builds installable
zips plus `.sha256` and `.manifest` files in `dist/`. CI lives in
`.github/workflows/`: `release-validation.yml` (build packages without publishing),
`release-contabo-pricing.yml` / `release-contabo-vps.yml` (tagged releases),
`whmcs-release-contract.yml` (changelog, package and runner-trust contracts) and
`fork-policy.yml`. Operator procedure: [`DEPLOY_RUNBOOK.md`](whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md).

## Schemas

See [`SCHEMA_VERSION.md`](SCHEMA_VERSION.md): API 1.1 plan shape, Catalog exchange
1.0, and the addon DB schema. Further reading: the addon's `docs/` directory
(`WHMCS_NATIVE_ARCHITECTURE.md`, `PROVISIONING_CONTRACT.md`).
