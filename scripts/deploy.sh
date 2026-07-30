#!/usr/bin/env bash
#
# Production deployment is intentionally not automated from this repository.
# Build immutable, reviewable artifacts locally, then follow the operator-
# controlled migration gates in docs/DEPLOY_RUNBOOK.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Production deployment is disabled in scripts/deploy.sh." >&2
echo "Build verified release artifacts with:" >&2
echo "  bash $SCRIPT_DIR/predeploy-check.sh" >&2
echo "  bash $SCRIPT_DIR/package-whmcs-suite.sh" >&2
echo "Then use the reviewed operator runbook; this command never connects to a host." >&2
exit 2
