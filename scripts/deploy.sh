#!/usr/bin/env bash
#
# Production deployment is intentionally not automated from this repository.
# FastPanel / root@195.7.4.219 rsync is retired. Build immutable artifacts
# here, then apply via Dokploy / whmcs-prod-new (see
# docs/operations/DOKPLOY_WHMCS_MODULE_DEPLOY.md and
# whmcs-module/modules/addons/contabo_pricing/docs/DEPLOY_RUNBOOK.md).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Production deployment is disabled in scripts/deploy.sh." >&2
echo "FastPanel/rsync apply is retired; do not target 195.7.4.219 from here." >&2
echo "Build verified release artifacts with:" >&2
echo "  bash $SCRIPT_DIR/predeploy-check.sh" >&2
echo "  bash $SCRIPT_DIR/package-whmcs-suite.sh" >&2
echo "Then admit dist/*.zip via Dokploy / whmcs-prod-new; this command never connects to a host." >&2
exit 2
