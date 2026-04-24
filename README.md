# Contabo Pricing Scraper

Extracts pricing for all Contabo Cloud VPS, Storage VPS, and Cloud VDS plans from Contabo's embedded `__SAPPER__` server-side payload. Outputs structured JSON and CSV files ready for analysis or further automation.

## Requirements

- Node.js ≥ 18 (uses built-in `fetch`)

## Quick start

```bash
git clone https://github.com/yashodhank/contabo-pricing-scraper.git
cd contabo-pricing-scraper
node scripts/contabo_scraper.js
```

Output files are written to `data/output/` by default.

## Usage

```
node scripts/contabo_scraper.js [options]

OPTIONS
  -o, --output <dir>       Output directory       (default: data/output/)
  -c, --concurrency <n>    Parallel fetches        (default: 4)
  -r, --retries <n>        Retries per URL         (default: 3)
  -p, --plans <slugs>      Comma-separated plan slugs to limit scraping
  -q, --quiet              Suppress progress output (stderr stays active)
  -j, --json               Print JSON summary to stdout on completion
      --dry-run            Fetch pages but do not write any output files
  -v, --version            Print version and exit
  -h, --help               Show this help
```

### Examples

```bash
# Scrape all 16 plans (default)
node scripts/contabo_scraper.js

# Custom output directory
node scripts/contabo_scraper.js --output ./pricing-data

# Faster with higher concurrency
node scripts/contabo_scraper.js --concurrency 8

# Scrape a subset of plans
node scripts/contabo_scraper.js --plans cloud-vps-10,cloud-vps-20,vds-s

# Validate pages without writing files
node scripts/contabo_scraper.js --dry-run

# Machine-readable output for AI agents / pipelines
node scripts/contabo_scraper.js --json --quiet > result.json
node scripts/contabo_scraper.js --json --quiet | jq '.gaps'
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0`  | All plans scraped and written successfully |
| `1`  | Fatal error — no output written |
| `2`  | Partial success — some plans failed, output written for the rest |

## Output files

All files are written to `--output` (default `data/output/`). The directory is created automatically.

| File | Description |
|------|-------------|
| `contabo_base_plans.json` | Base plan specs and all contract period pricing |
| `contabo_configs.json` | Per-plan configurator state (options grouped by dimension) |
| `contabo_pricing_dataset.json` | Combined dataset with metadata, plans, options, and gaps |
| `contabo_base_plans.csv` | Flat CSV of base plans with pricing for all contract periods |
| `contabo_option_catalog.csv` | Normalized option catalog (one row per plan × option) |
| `contabo_gap_report.json` | Raw list of unclassified/failed items |
| `contabo_gap_summary.json` | Gap counts grouped by type |

> `data/output/` is excluded from git — run the scraper to regenerate.

## Data model

### Base plan fields

| Field | Description |
|-------|-------------|
| `family` | `Cloud VPS`, `Storage VPS`, or `Cloud VDS` |
| `product_slug` | Contabo plan slug, e.g. `cloud-vps-10` |
| `fetched_at` | ISO 8601 timestamp of when this plan was fetched |
| `cpu`, `ram`, `base_storage` | Spec strings |
| `base_monthly_price` | Month-to-month price in EUR |
| `periods[]` | Per-period pricing: `months`, `effective_monthly`, `setup_fee`, `total_period_cost`, `discount_total` |

### Option catalog dimensions

| Dimension | Categories |
|-----------|------------|
| `Region` | Europe, America, Asia, Australia |
| `Storage Type` / `Storage` | NVMe, SSD |
| `Data Protection` | Auto Backup, None |
| `Networking` | Bandwidth, IPv4, Private Networking |
| `Image` | OS, Panels, Apps, Blockchain |

## How it works

1. Fetches each plan URL in parallel (configurable concurrency)
2. Extracts the `__SAPPER__` JSON payload embedded in the HTML
3. Classifies each add-on option into a normalized dimension/category
4. Injects known defaults that Contabo renders via HTML but not the payload
5. Deduplicates and sorts, then writes JSON and CSV

## Snapshots

`data/snapshots/` contains saved HTML pages used during parser development. Not tracked by git for production runs.

## License

MIT
