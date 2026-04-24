# Contabo Pricing Scraper

This repository contains the current Contabo pricing extraction project for Cloud VPS, Storage VPS, and Cloud VDS plans.

## Contents

- `scripts/contabo_scraper_v5.js` - current enhanced scraper using Contabo's embedded `__SAPPER__` payload
- `data/output/contabo_base_plans_enhanced.csv` - base plan pricing export for all 16 plans
- `data/output/contabo_option_catalog_enhanced.csv` - normalized option catalog export
- `data/output/contabo_base_plans_enhanced.json` - base plan JSON export
- `data/output/contabo_configs_enhanced.json` - per-plan configuration JSON export
- `data/output/contabo_pricing_dataset_enhanced.json` - combined structured dataset
- `data/output/contabo_gap_report.json` - raw gap log
- `data/output/contabo_gap_summary.json` - summarized gaps
- `data/snapshots/cloud_vps10.html` - saved sample HTML snapshot used during parser development

## Current Status

- Scrapes all 16 plans
- Builds pricing from `__SAPPER__` payload instead of brittle regex-only parsing
- Emits enhanced CSV and JSON outputs
- Initializes with zero unresolved classification gaps in the current dataset

## Run

```bash
node scripts/contabo_scraper_v5.js
```

## GitHub Push

Example push flow once you add the remote:

```bash
git remote add origin git@github.com:yashodhank/contabo-pricing-scraper.git
git add .
git commit -m "Initialize Contabo pricing scraper project"
git push -u origin main
```
