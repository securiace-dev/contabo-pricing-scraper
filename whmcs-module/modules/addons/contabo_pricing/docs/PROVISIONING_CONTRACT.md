# Provisioning Contract — `contabo_vps` server module

The contract promised by PHASE_A52 §17: how the WHMCS server/provisioning
module (`modules/servers/contabo_vps`) reads product configuration and
customer selections, what it sends to the official Contabo REST API (the same
API the `cntb` CLI wraps), and the invariants that keep a WHMCS service from
ever touching the wrong Contabo instance.

Compatibility: WHMCS 8.x and 9.x, PHP 7.4 – 8.4 (polyglot floor 7.4).
Covers the compute product lines the create API accepts via `productId`
(VPS / VDS / Storage VPS). Object storage is a different API family and is out
of scope for this module.

## 1. Server credentials

WHMCS → System Settings → Servers, module **Contabo VPS**:

| WHMCS field | Contabo meaning |
|---|---|
| Username (`serverusername`) | OAuth2 `client_id` |
| Password (`serverpassword`) | OAuth2 `client_secret` |
| Access Hash (`serveraccesshash`) | `apiUser:apiPassword` (API user email + API password, colon-separated) |

All three are WHMCS-encrypted at rest. Auth is the Keycloak password grant at
`auth.contabo.com`; tokens are held in-memory only and refreshed 60 s before
expiry (plus one forced refresh on a 401).

## 2. Product configuration (module config options)

| # | Meaning | Notes |
|---|---|---|
| 1 | Contabo `imageId` | **Fallback** — used only when no Image selection reaches provisioning |
| 2 | Region | Slug (`EU`, `US-central`, `US-east`, `US-west`, `SIN`, `UK`, `AUS`, `IND`, `JPN`) or retail label; **fallback** for the Region selection |
| 3 | SSH secret id | Optional; numeric `secretId` of an SSH public key in the Contabo vault. Non-numeric values fail closed. |
| 4 | Contabo `productId` | e.g. `V45`. Required. |
| 5 | Cloud-init user data | Optional; passed as `userData` verbatim |
| 6 | Add-ons JSON | Optional; a Contabo `addOns` object, merged with selection-derived add-ons. Invalid JSON fails closed. |

### The instance-id custom field

The service custom field **`contabo_instance_id`** (stored as
`contabo_instance_id|Contabo Instance ID`, admin-only) anchors the WHMCS→
Contabo link. It is **auto-created on the product at first provision**; if it
cannot be created, provisioning refuses to run (an unlinked instance is an
orphan the module can never manage). Reads tolerate both the bare and
pipe-friendly fieldname forms. The value is kept after termination for audit.

## 3. Customer selections (configurable products)

For `customer_configurable_product` mappings the module reads the service's
selected configurable options and round-trips them through the addon's link
tables:

```
tblhostingconfigoptions.optionid  (selected WHMCS sub-option id)
  → mod_contabo_config_option_value_link.whmcs_sub_id
      → contabo_value_key ("category:label") + contabo_label
      → its option link: dimension_key + pass_to_provisioning
```

Rules:

- Every read is `hasTable`-guarded — without the addon the module silently
  falls back to config options 1–4 (fixed-product behaviour).
- `pass_to_provisioning = 0` selections are skipped (the admin's explicit
  exposure decision).
- Selections not curated by the addon (no value link) are ignored.

### Dimension → API mapping (v1 boundary)

| dimension_key | API field | Resolution |
|---|---|---|
| `Image` | `imageId` | Label → `GET /v1/compute/images` name match. **Fail-closed**: no match or ambiguity aborts provisioning (never a guessed OS). UUID selections pass through. |
| `Region` | `region` | Static label→slug map (fail-closed on unknown labels; slugs pass through). |
| `Networking:Private Networking` | `addOns.privateNetworking` | Enabled-style values only. |
| anything else (`Networking:IPv4` qty, `Networking:Bandwidth`, `Data Protection`, `Storage Type`, …) | — | **Acknowledged, not applied**: a `logActivity` line records the selection so the admin can apply it in the Contabo panel. Never silently dropped, never blocking. |

## 4. Billing cycle → Contabo `period`

`period` is REQUIRED by the create API (1, 3, 6 or 12 months). Mapping is
longest-period-≤-cycle, floor 1:

| WHMCS cycle | period |
|---|---|
| Monthly | 1 |
| Quarterly | 3 |
| Semi-Annually | 6 |
| Annually | 12 |
| Biennially / Triennially | 12 (logged; Contabo auto-renews) |
| Free / One Time / unknown | 1 (logged) |

## 5. Root password / secret lifecycle

Contabo accepts only a vault `secretId` as `rootPassword`, so:

- **Create**: the WHMCS service password (or a generated one when WHMCS sends
  none) is stored as vault secret `whmcs-svc-{serviceid}-root`
  (type `password`) and its id rides in as `rootPassword`. A generated
  password is written back to `tblhosting.password` (encrypted) so the
  password WHMCS shows the customer works on the server.
- **Reset Password** (admin button and client-area button): a fresh generated
  password PATCHes the same secret in place, `POST …/actions/resetPassword
  {rootPassword: secretId}` applies it, and only on API success is
  `tblhosting.password` updated.
- **Terminate**: the secret is deleted best-effort; vault failures never fail
  the termination.
- Secret **values never enter logs**: the module log sanitizer masks
  `value` / `password`-bearing keys (enforced by `LogRedactionTest`).
- Config option 3 (SSH secret) coexists with the root password.

## 6. Identity & idempotency invariants

Two anchors, maintained together:

1. **WHMCS side** — the `contabo_instance_id` custom field.
2. **Contabo side** — the instance `displayName` starts with the tag
   `whmcs-{serviceid}` (word-boundary matched: `whmcs-12` never matches
   `whmcs-123`).

Create flow (`CreateAccount`) in order:

1. Ensure the custom field exists (abort before any API call otherwise).
2. Stored id present → verify at Contabo: exists **and** tagged → idempotent
   `success` (a WHMCS retry never double-provisions); exists untagged → error
   directing the admin to "Sync from Contabo"; missing at Contabo → error
   requiring a deliberate field clear.
3. No stored id → search instances for the tag: exactly one match → **adopt**
   (recovers a create that died between the API call and the DB write); more
   than one → error (never guess); none → provision fresh.
4. Fresh create tags the displayName, links the id, vaults the password.

Write-policy matrix:

| Action | Requires |
|---|---|
| Status/read | stored id |
| start / stop / restart | stored id + instance exists (tag drift logs a warning) |
| terminate, resetPassword, **reinstall** | stored id + **tag match** (mismatch blocks with guidance) |
| relink to a different id | never silent — explicit admin action only |

Adoption/recovery (`findByTag`) queries Contabo's server-side `search` filter
on the tag first — it does not scan the whole account, so it stays reliable on
large/reseller fleets. If it must fall back to an unfiltered scan and that scan
is truncated by the page cap, it logs a warning rather than returning a partial
result. Candidates are deduped by instance id, and more than one distinct match
still errors (never guess).

`sync()` re-asserts a drifted displayName tag (the stored id is authoritative
because it was written under the no-silent-overwrite policy).

## 7. Sync surfaces

| Surface | Trigger | Writes |
|---|---|---|
| Admin service tab | every render (10 s timeout, degrades to cached IP) | `tblhosting.dedicatedip` / `assignedips`, displayName tag |
| Client area page | every render (same degradation) | same |
| "Sync from Contabo" admin button | on demand | same |
| `DailyCronJob` hook | daily, ≤100 active/suspended services, one auth per server, paced | same |

`dedicatedip` gets the primary IPv4; `assignedips` gets any additional IPv4s
followed by all IPv6s (newline-joined); writes only when changed. The admin tab
and client panel display IPv6 when present. The cron sweep groups services by
server so each server authenticates once and reuses its client across services.

## 8. Failure modes

| Condition | Behaviour |
|---|---|
| API/auth down during a view | Page renders last-synced IP + "live status unavailable" note |
| API down during an action | Human-readable error string returned to WHMCS; no partial writes |
| HTTP 429 / 5xx | Retries with backoff, total sleep capped at 6 s per call |
| 401 | One token refresh + replay; a second 401 is a credential error |
| Tag mismatch on destructive action | Blocked with instructions (Sync/verify first) |
| Multiple tagged instances | Blocked — resolve duplicates in the Contabo panel |
| Addon link tables absent | Fixed-product fallback (config options 1–4) |
| Missing custom field | Auto-created; if creation fails, provisioning refuses to start |
| ChangePackage | Honest error — the API has no resize (`/upgrade` is add-ons only) |

## 9. Out of scope / follow-ups

- Rescue-mode surfacing (`POST /{id}/actions/rescue`) — the temporary rescue
  password needs a display path that doesn't clobber the real OS password.
- Add-on-only upgrades via `POST /{id}/upgrade` from ChangePackage.
- Object Storage / S3 product line (different API family).
- VNC/console access.
