---
name: whmcs-local-testing
description: Test the contabo_pricing addon + contabo_vps module against real local WHMCS (8.13/PHP7.4 and 9.0/PHP8.2) using the whmcs-devbox stack. Use when verifying the WHMCS modules beyond the PHPUnit suite — schema migrations, activation, admin rendering, or live-schema behavior on real WHMCS before deploy.
---

# Testing the contabo WHMCS modules locally

Three test layers, increasing fidelity:

1. **Unit (no DB, fast)** — PHPUnit + FakeCapsule. Always available, run first:
   ```bash
   cd whmcs-module/modules/addons/contabo_pricing && vendor/bin/phpunit
   ```
   (Host PHP 8.x runs PHPUnit; code must stay PHP 7.4-compatible.)

2. **Real PHP 7.4 lint** — the polyglot floor, checked against actual 7.4:
   ```bash
   docker exec whmcs-devbox-whmcs8-1 sh -c 'for f in /var/www/html/modules/addons/contabo_pricing/lib/*.php; do php -l "$f" >/dev/null || echo "FAIL $f"; done'
   ```

3. **Real WHMCS (both versions)** — via `~/Projects/whmcs-devbox`:
   ```bash
   cd ~/Projects/whmcs-devbox
   ./bin/whmcs-devbox use contabo-pricing   # mounts both modules into 8.13 + 9.0
   ./bin/whmcs-devbox up                     # (install WHMCS once — see devbox skill)
   ./bin/whmcs-devbox activate 8 && ./bin/whmcs-devbox migrate 8   # repeat for 9
   ./bin/whmcs-devbox render 8 dashboard     # renders the addon admin page
   ```
   Healthy result: `migrate` -> `{"ok":true,"healthy":true,"schema_version":N}`,
   `activate` -> `mod_contabo_*` tables created, `render` -> full addon HTML.

## Why both versions matter

Prod runs PHP 7.4/8.1; the addon's polyglot floor is **7.4**. Bugs that only
surface on 7.4 (e.g. a dependency pulling PHP 8 union-type syntax) pass silently
on 9.0/PHP8.2 — always confirm on **whmcs8 (PHP 7.4)** too. The devbox shadows
the addon's dev `vendor/`+`tests/` from the runtime so they match `deploy.sh`'s
prod excludes (a leaked PHPUnit `vendor/` fatals PHP 7.4 at activate).

## Relationship to the predeploy gate

`scripts/predeploy-check.sh` stages 3–4 (live-schema + integration smoke) need a
running dev WHMCS. The legacy `scripts/local-whmcs.sh` targeted the old
securiace-vps-platform container names; the canonical local stack is now
**whmcs-devbox** (`use contabo-pricing`). Never deploy on a red gate.
