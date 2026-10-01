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
