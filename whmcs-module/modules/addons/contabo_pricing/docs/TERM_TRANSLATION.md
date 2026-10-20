# Term translation

Contabo publishes contract terms up to 12 months. This add-on prices **any**
profile period of N months from that published set so 24/36-month (and other)
profiles keep working.

WHMCS can bill only six cycles (`tblpricing`: 1 / 3 / 6 / 12 / 24 / 36 months).
Any other N is supported for a profile headline period, Proposal Studio quotes
and provenance. It cannot become a WHMCS catalog cycle.

## Settings (`mod_contabo_settings`)

Editable on **Runtime & configuration**. Defaults keep existing catalog prices.

| Key | Default | Meaning |
|---|---|---|
| `term.policy` | `nearest-term` | Base plan: native rate, else longest published term ≤ N, else list monthly |
| `term.addon_policy` | `proportional` | Add-ons: native per-term map if present, else monthly × N |
| `term.adjust_pct_json` | `{}` | Extra percent off **derived** monthly rates only, clamped 0–50 |

Changing a setting changes the source vector; the next sync appends a new
profile version when the vector differs.

## GST

The translator never adds GST. `apply_gst_18` now defaults to **No**
(Securiace is not GST registered). An existing install that already stored
`yes` in `tbladdonmodules` keeps charging GST until Configure is set to No.

## Provenance

Derived terms are written to:

- `mod_contabo_sync_log.summary` → `term_policy`, `warnings[]`, `derived_terms[]`
- `mod_contabo_profile_version.options_snapshot._term_provenance`

The catalog envelope is never rewritten with derived numbers.
