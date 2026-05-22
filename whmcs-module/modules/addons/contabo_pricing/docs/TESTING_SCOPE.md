# Testing scope guard — my.securiace.com (production WHMCS)

> **HARD RULE for human + AI contributors.** The deployment target `my.securiace.com`
> is a live, customer-facing WHMCS install. The owner has authorised work on the
> `contabo_pricing` addon **only**. Anything else is off-limits.

## Allowed test surfaces

When you have admin session cookies (or any other authenticated access) to `my.securiace.com`, you may hit ONLY these three URL patterns:

| URL pattern | Why |
|---|---|
| `https://my.securiace.com/shriram/addonmodules.php?module=contabo_pricing*` | The addon's admin pages. Any `&action=…` query is allowed. |
| `https://my.securiace.com/modules/addons/contabo_pricing/ajax.php*` | The addon's standalone JSON endpoint. Any `?action=…` query is allowed. |
| `https://my.securiace.com/modules/addons/contabo_pricing/assets/*` | Static asset URLs (CSS/JS/images shipped by this addon). |
| `https://my.securiace.com/shriram/login.php` and `…/shriram/logout.php` | Necessary infrastructure when establishing an admin session before touching the addon URLs. Always log out at end of test run. |

POST forms inside the addon are allowed because they only mutate `mod_contabo_*` tables:

- `action=profile-create` / `profile-save` / `profile-toggle`
- `action=mapping-save`
- `action=sync-run` / `refresh-api`
- `action=tax-settings-save`
- `action=service-policy` / `approve-decision` / `cancel-schedule` (once Phase B lands)

In Phase A / A.5, `ServicePriceWriter` is gated to `enabled=false`, so even the worst-case engine path cannot touch `tblhosting`. That gate is the safety net, **not** a substitute for this rule.

## Forbidden surfaces — always

Every other URL on `my.securiace.com` is **off-limits**, including read-only GETs:

- Any other admin page: `clientssummary.php`, `clientshosting.php`, `orders.php`, `invoices.php`, `tickets.php`, `configgeneral.php`, `configproducts.php`, anything under `/shriram/` that is not `addonmodules.php?module=contabo_pricing*`.
- The client area: `clientarea.php`, `cart.php`, `viewinvoice.php`, etc.
- The LocalAPI endpoint: `/includes/api.php?action=...`. Use of the LocalAPI from inside the addon's own PHP code (via `localAPI()` helper) is fine — that's a server-side call running in the addon's context. Calling it from outside (via curl + admin cookies) is forbidden.
- The cron endpoints: `crons/cron.php`, `crons/runcron.php`. Only the production cron daemon may invoke them.
- The DB directly: writes to any table other than `mod_contabo_*` are forbidden. Read-only `SELECT` for diagnostic purposes via SSH + PDO is permitted only when the owner has authorised that specific diagnostic in the current task.

## Why this matters

A stray GET on `/shriram/orders.php?action=delete&id=…` deletes an order. A stray GET on `/shriram/clientssummary.php?action=somedestructive` could trigger customer-visible side effects. Some WHMCS admin pages accept destructive actions via GET query strings, not just POST. Treat **every** non-addon admin URL as potentially destructive.

## How to apply this rule

1. **Before every authenticated curl, grep the URL against the 3 allowed patterns above.** If it doesn't match exactly, do not fire it.
2. **Never store admin cookies in this repo.** They are session credentials. Put them in `/tmp/whmcs_admin_cookies.txt` with mode `600`; they expire when the OS reboots or when the user logs out.
3. **Never store admin cookies in a knowledge-graph entry, memory drawer, or any file under `~/.claude/` other than a rule-description file.** The rule is portable; the credentials are not.
4. **Scope expands only on explicit per-task authorisation.** If the owner says "go ahead and create a test client", that authorises that one action, in that one turn — it does not authorise client creation in future tasks.
5. **When in doubt, ask the owner.** Pausing for a 30-second confirmation is cheaper than a 30-minute incident response.

## Where the rule is recorded

- This file (`docs/TESTING_SCOPE.md`) — in the addon repo, visible to anyone who clones it.
- `~/.claude/projects/-Users-kritananda/memory/feedback_whmcs_test_scope.md` — in the AI assistant's persistent memory, loaded into every future session.
- Memory index `MEMORY.md` entry pointing at the above.

Both records must say the same thing. If you update one, update the other.

## Owner's wording (verbatim, 2026-05-22)

> "make sure you dont touch anything as its prod whmcs, working on module is okey but everything not related module is off limit for you and write rules and docs to ensure it never gets [missed]."

— filed under the contabo_pricing addon, applies to all of `my.securiace.com`.
