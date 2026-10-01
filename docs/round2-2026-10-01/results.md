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
