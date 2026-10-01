# Round 2 results: provider cross-comparison on required values

Status: Mode A baseline on the round-1 bodies is done (zero cost). Everything under "Mode A through the addon" and
"Mode B" is to be filled by the main session. Scorer: `php scripts/round2-score.php --mode=A|B --provider=<name> --input=<file>`
(add `--json` for machine output, `--scorecard-out=<file>` to keep the decoded scorecard, `--notes="..."`).
Ground truth: `ground-truth.json` (screenshots + blob). Schema: `scorecard.schema.json`.

Scoring definitions:
- **Completeness** = required values present and non-empty / required values total. Required per plan: title, slug, availability,
  prices EUR/USD/GBP, per-period discount/effective_monthly/total for every expected period (1/3/6/12/24; dedicated 1/3/6/12),
  the family's spec keys (snapshots only for Core/Performance/GPU; gpu only for GPU VPS), and, for the two configurator plans
  (`cloud-vps-core-4`, `gpu-vps-plus-18`), the option groups (regions, storage_upgrades, backup, object_storage, os_images, apps, panels; no backup for GPU).
  A plan missing from the output counts all its required values as missing.
- **Correctness** = ground-truth checks that match (41 checks: prices, specs, period discounts, option prices, gross = net x 1.18 against the screenshot
  values, 15 %/20 % discount percentages).
- Caveat on the Mode A baseline: expected plan lists and non-configurator values come from the same blob, so 100 % completeness only proves the decoder +
  `CatalogStructure` recover everything the blob has. The independent checks are the screenshot-derived ones (gross values, discount percentages, the
  derived object-storage price). Negative control: a deliberately damaged scorecard scores 46 % completeness / 43.9 % correctness; an empty one 0 %.

## Round 1 bodies, Mode A (zero cost, no network)

| Provider | Mode | Completeness | Correctness | Bytes | Decode | Notes |
|---|---|---|---|---|---|---|
| treg / anyapi.web.scrape | A | 100.0 % | 100.0 % (41/41) | 2,035,974 | yes (SapperLiteralDecoder) | 1 call, full blob |
| AlterLab / js | A | 100.0 % | 100.0 % (41/41) | 2,037,565 | yes (SapperLiteralDecoder) | 1 call, full blob |

Per family (identical for both bodies):

```
  vps                Core VPS               plans 6/6  100.0 % (175/175)
  performance-vps    Performance VPS        plans 6/6  100.0 % (168/168)
  vds                Max Performance VPS    plans 5/5  100.0 % (135/135)
  storage-vps        Storage VPS            plans 5/5  100.0 % (135/135)
  dedicated-servers  Dedicated Servers      plans 4/4  100.0 % (96/96)
  gpu-vps            GPU VPS                plans 1/1  100.0 % (35/35)
```

Not scored: `alterlab_auto.html` (323,881 bytes, truncated): `SapperLiteralDecoder` throws
`DecoderException: Could not find end of __SAPPER__ payload`, so Mode A fails (exit 3) on a truncated body, consistent with round 1.
Both full bodies decode to 168 products, 15 categories; 27 family plans, 141 legacy, 847 addon groups.

## Mode A through the addon (dry runs from Scrape runs on whmcs-devbox)

| Provider | Mode | Completeness | Correctness | Bytes | Decode | Notes |
|---|---|---|---|---|---|---|
| treg / anyapi | A | | | | | |
| AlterLab / js | A | | | | | |
| TinyFish Fetch (markdown cross-check only) | A | | | | | |

## Mode B (provider-side structured extraction)

| Provider | Mode | Completeness | Correctness | Bytes | Decode | Notes |
|---|---|---|---|---|---|---|
| AlterLab extraction_schema | B | | | | | |
| treg / scrapegraphai | B | | | | | |
| TinyFish Agent output_schema | B | | | | | |

## Decision table

| Provider | Completeness % | Correctness % | Effort (1-5) | Cost per full catalogue refresh | Latency | Failure modes | Lock-in |
|---|---|---|---|---|---|---|---|
| | | | | | | | |

## Step 5 changes (self-learning family registry, 2026-10-01)

Behaviour of the addon after step 5 (schema v16). Nothing about the lineup is configured: the site is the
source of truth every run, and the registry remembers what it learned.

**Discovery (`CatalogStructure::discover`).** A family is a category the site's own navigation links to. A nav entry is
matched to a category by (a) link last segment = category slug, (b) normalised nav title = category title, or (c) the same
dash-token set (`/vps-performance/` = `performance-vps`); a tie links nothing. Label = nav title (fallback category title),
order = nav order, membership = `categories[].products` (never `product.categoryId`). A product that is a member of several
families belongs to the most specific one (fewest members, then nav order), so the five `cloud-vps-plus-*` plans in the
`vps` category go to Performance VPS without a slug rule. Only plan-shaped products count (EUR price > 0 and a cpu or ram
spec): the object-storage region products, which the nav may link, are unit prices, never plans. Everything else is legacy
and excluded unless its slug is in `scrape.legacy_allowlist_json`.

**Registry (`mod_contabo_scrape_families`, `FamilyRegistry::reconcile`).** One row per `categories[].id`:
- learn: upsert by id, `title_history_json`, member slugs, `sample_product_url`, nav href/position;
- adapt: nav-linked plan family -> `active`; in the blob but not in the nav (VPS HP/MP, sales, 2026 re-launch) -> `hidden`;
  vanished -> `retired` (never deleted); first sight -> `new`, `approved=0`; a retired id that returns -> `reappeared`;
- calibrate: `typical_plan_count` = median of the last 5 accepted counts (`plan_count_history_json`);
- heal: a vanished id whose slug (or >= 80 % of whose member slugs) shows up under a new id is linked (`successor_of`),
  approval, display name and histories carry forward, and it is reported as `renamed`, not as new + retired;
- govern: `approved`, `admin_hidden`, `display_name` are operator-owned and never overwritten by a run.

The diff `{new, renamed, retired, delisted, reappeared, count_changes, plan_moves, unapproved, families}` is stored in the
run's `gates_json.families` and rendered on the run detail page. Commit rule: a run only writes the registry when it ends
`succeeded` or `needs_review`; dry, rejected and failed runs only preview, so a flagged new family is raised again until a run
is accepted.

**Validator.** A new, renamed, retired, delisted or reappeared family, a plan that moved family, and a family whose count
deviates by more than `scrape.max_drop_pct` from its typical count are `risky` -> `needs_review`, never `rejected`.
`min_plans_per_family` is per registry family: `max(1, floor(typical x (1 - max_drop_pct)))`; for a family with no learned
typical it falls back to the legacy `scrape.min_plans_*` setting for that name, then 1. `approved=0` families are imported
and flagged. Dedicated and GPU plans pass `schema_completeness` (cpu from `N x GHz`, snapshots optional).

**Fetching.** Category landing pages do not carry the blob; one PRODUCT page carries the whole catalogue. Targets: an
explicit `scrape.plan_urls_json` override, else the registry's active families' `sample_product_url` (first member product,
URL built exactly as `PlanNormalizer::productUrl`), else `https://contabo.com/en/vps/cloud-vps-core-4/`. The first target is
fetched; further pages only when no blob came back (fall back to the next family's page) or a family has fewer plans than its
typical count (cross-page consistency is then checked). Landing URLs (`nav_href`) are metadata. Each attempt records
`final_url`, the page url and the nav titles the page rendered (`nav_titles_json`).

**Normaliser.** `Virtual`/`Physical`/`vCPU` Cores and `N x GHz` cpu; `2 x 1 TB NVMe` sums drives, `300GB SSD / 150GB NVMe`
takes the first as primary; GPU spec -> `specs_parsed.gpu`; `snapshot_count`, `traffic`; `prices{EUR,USD,GBP}` and
`previous_price`; `base_monthly_price` stays EUR. Per-plan `options{regions, storage_upgrades, backup, object_storage,
os_images, apps, panels, monitoring, other}` with monthly and setup price (object storage = region product price x size /
250 GB), carried in `payload.options` and in `configurations.plans[slug]` (which become `configuration_option` items).

**Round-2 priors.** `treg_anyapi` {700, 0.95, 10000}, `alterlab` {4000, 0.95, 20000}, `treg_litescrape` {150, 0.50, 9000,
not auto-ranked: capacity 503 observed}, `tinyfish_fetch` {0, 0.999, 7000, no sapper}, `tinyfish_agent` manual.

**TinyFish Agent.** Launched once with `POST /v1/automation/run-async` (never retried on timeout or 5xx: a retry launched
duplicate paid runs), followed with `GET /v1/runs/{id}` every 10 s (budget 600 s), cancelled on budget or `max_steps`, cost =
`num_of_steps x $0.016`; `output_schema` is sanitised; the run id and spend stay on the attempt row.

Verification at this step: scorer on `round1_treg_anyapi.html` still 100 % (744/744 required values, 41/41 ground-truth
checks); the discovered families on that blob are Core VPS 6, Performance VPS 6, Max Performance VPS 5, Storage VPS 5,
Dedicated Servers 4, GPU VPS 1 (27 plans).

## Runs through the addon deployed on whmcs-devbox (WHMCS 8.13, 2026-10-01)

Deployment: addon copied into `whmcs-devbox-whmcs8-1`, upgraded to schema 15, keys stored sealed through
`DataSourcesForm::apply` (the same path the Data sources page uses), per-source Test, then dry runs.

### Mode A — raw product page → PHP decoder (`SapperLiteralDecoder` → `CatalogStructure`)

| Provider | Test | Fetch | Blob | Completeness | Correctness | Cost / page | Latency | Notes |
|---|---|---|---|---|---|---|---|---|
| treg → `anyapi.web.scrape` | ok | 200, 2.04 MB | yes | **100 %** (744/744) | **100 %** (41/41) | $0.0007 | 9.9 s | dry run through `ScrapeRunService`: 16 legacy-slug plans decoded, gates evaluated |
| AlterLab `mode: js` (tier 4) | ok | 200, 2.03 MB | yes | **100 %** | **100 %** | $0.004 | 21.7 s | identical values |
| treg → `litescrape.web.fetch.post` | ok | 503 `provider_capacity_unavailable` (shared key) | — | — | — | $0.00015 | — | first measurement was served by anyapi (options lost on save) and is not independent |
| TinyFish Fetch | ok | 200, 2 KB rendered text | **no** | 0 % | 0 % | $0 | 7.3 s | scripts stripped; cross-check only |
| Landing page `/en/vps/` via anyapi | ok | 200, 1.24 MB | **no** | — | — | $0.0007 | 10.4 s | **category landing pages do not carry the blob; only product pages do** |

### Mode B — provider-side structured extraction (scorecard JSON Schema)

| Provider | Result | Completeness | Correctness | Cost | Notes |
|---|---|---|---|---|---|
| AlterLab `extraction_schema` on `/scrape` | HTML returned, schema ignored (`filtered_content` null), still billed tier 4 | — | — | $0.004 ×3 | first two attempts 422 (`NS_ERROR_ABORT`, `origin_unavailable`) |
| AlterLab `/extract` | 403 `BETA_FEATURE_REQUIRED` (intelligent-extraction) | — | — | $0 | account-level beta flag; not available to this key |
| treg → `scrapegraphai.web.extract` (LLM) | 1 family, 6 plans, EUR only, no periods/options | 10.5 % | 22 % (9/41) | $0.02 | visible page only |
| TinyFish Agent, product page, `output_schema` (23 steps) | 1 plan; USD/GBP copied from EUR; discounts as %, not amounts; India region missing; panels/backup right | 4.6 % | 29 % (12/41) | $0.37 | hallucination risk is the governing issue |
| TinyFish Agent, landing page (10 steps) | ignored schema; markdown table of 5 tabs (Core 6, Performance 6, Max Performance 5, Storage 5, Windows 4) at 24-month prices; invented "Cloud VDS 4" names | n/a (not scorecard-shaped) | — | $0.16 | no regions/backup/object storage |
| TinyFish Agent, product page, no schema (4 steps) | 1 plan, EUR per period, periods correct (15 %/20 %) | ~3 % | partial | $0.06 | first attempt, before schema sanitising |

Agent total 37 steps ≈ $0.59; the adapter's transport-retry launched a duplicate run (cancelled) — a non-idempotent retry bug, fixed in step 5.

### Decision (round 2)

Results and effort, not cost, decide it: **Mode A wins outright** — one product-page fetch yields every required value for all
six nav families (prices in EUR/USD/GBP, periods, specs, per-plan regions/storage/backup/object storage/OS/app/panel
addons) with 100 % completeness and 100 % correctness against the checkout screenshots, decoded deterministically in PHP.
No provider-side extraction came close (best 10.5 %), and the LLM paths hallucinated values.

Within Mode A: **primary treg → `anyapi.web.scrape`** ($0.0007, ~10 s), **fallback AlterLab `mode: js`** ($0.004, ~22 s).
TinyFish Fetch stays free for the visible-price cross-check; TinyFish Agent stays manual-only. Round-1 ranking is confirmed,
now on results. `treg_litescrape` stays disabled (capacity 503 on the shared key).

### Findings that change the addon (implemented in step 5)
- Families are discovered per run from the blob's `categories` + nav (six nav families; VPS HP/MP are hidden price mirrors), kept in a registry with history, and only reviewed when new/renamed/retired.
- Fetch target must be a product page (first member of the first active family); landing pages are metadata only.
- Live spec strings changed: "6 Virtual Cores", "32 x 3.55 GHz", "2 x 1 TB NVMe", "300GB SSD / 150GB NVMe" — parser widened; dry run on live data had rejected VDS plans for missing `cpu_count`.
- Multi-currency prices and per-plan addon groups (97 on Cloud VPS 4) are now carried into the envelope.

### Open items (round 3)
- The landing page renders a **"Windows VPS" tab** that has no blob category; the registry must reconcile rendered tabs (DOM) against blob categories, and flag tabs with no category (and categories with no tab).
- `scrape.plan_urls_json` legacy 16 slugs are superseded by the registry; the first dry run still used them (16 legacy plans) — confirm after redeploy that runs use the registry.
- AlterLab intelligent extraction is a beta flag on the account; enable it only if a Mode B cross-check is wanted.
