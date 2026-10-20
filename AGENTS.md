# Agent notes

- This repo ships WHMCS modules only (addon + VPS suite). No Contabo pricing
  sidecar after PR #32.
- Production deploy target is **Dokploy / whmcs-prod-new**, not FastPanel.
  See `docs/operations/DOKPLOY_WHMCS_MODULE_DEPLOY.md` and
  `whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md`.
- `scripts/deploy.sh` never connects to a host; gate + package only.
- Project guide: `CLAUDE.md`.
