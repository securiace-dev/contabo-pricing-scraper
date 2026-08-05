# Plan 005 — Canonical WHMCS proposal, managed policy, and multi-resource operations

Status: SUCCESSOR PLAN · implementation slice in progress · production blocked
until a green pull request is reviewed and the operator explicitly authorizes
merge and deployment.

This plan supersedes the execution order in Plans 001–004 and open pull
requests #24/#25 without discarding their valid requirements. It starts from
the current canonical `main`: the Rust read-oriented catalog/quote API, the
`contabo_pricing` addon, and the `securiacevps` provisioning module. The
archived Python control plane and legacy `contabo_vps` module are
reference/test-vector sources only. They are not production runtimes.

## 1. Decisions and non-negotiable boundaries

### 1.1 Four business decisions

1. **Founder overage and monthly entitlement**
   - Founder overage is INR 2,500/hour plus applicable tax.
   - A written estimate and approval are required before work starts.
   - Included Founder hours expire each month.
   - Carry-forward is never automatic. An administrator may issue a dated,
     expiring credit with a reason; it is explicitly discretionary and
     non-contractual.
   - Emergency authority covers minimum stabilization only for an active
     outage, active or credible compromise/abuse, imminent material data loss,
     failed recovery, or critical administrative lockout. Routine tuning,
     maintenance, migration, and feature work are not emergencies.
   - One hour is the configurable internal approval guardrail. It is an
     operational recommendation pending legal sign-off, not a client
     entitlement.
   - Every purchase snapshots the rate, policy version, included minutes,
     tax mode, and emergency terms.

2. **Tax and Sum 9**
   - “Sum 9” is internal pricing-numerology/provenance metadata only. It never
     affects price, quantity, tax, term, or client output by default.
   - There is a release-blocking **Securiace output-tax** source conflict: older managed-service
     material describes the annual prices as GST-exclusive at 18%, while the
     newer Securiace legal draft says Securiace is not GST-registered, prices
     are all-inclusive, and no separate GST is charged.
   - Safe current behavior is Securiace output GST disabled/all-inclusive. The catalog
     may retain tax-eligible and nominal-rate metadata, but runtime tax mode
     comes only from a versioned WHMCS/commercial-settings snapshot.
   - Owner confirmation, verified GST-registration evidence, and matching
     WHMCS tax configuration are mandatory before output GST is enabled.
   - Provider/vendor tax is independent. It remains provider cash-cost
     provenance and is excluded from economic landed cost only when verified
     input-tax-credit recovery applies.

3. **Object Storage tenancy**
   - Default to pooled regional management resources with one isolated S3
     user/credential set, bucket namespace, restrictive policy, and committed
     quota per WHMCS service.
   - Offer a dedicated tenant/resource exception for contractual isolation.
   - Bill committed capacity, not aggregate provider usage. Usage/stats are
     visibility and operations data.
   - Ordering remains disabled until authenticated API canaries and
     bucket-policy isolation/purge tests pass.

4. **Cancellation and reassignment**
   - Normal cancellation is forward-dated. Client access and asset reservation
     continue through the paid-through/provider termination date.
   - Client entitlement, WHMCS billing, provider contract, provider asset, and
     inventory disposition are separate state machines.
   - Never reassign while entitlement/export obligations remain. Revoke access
     where legally permitted, quarantine, sanitize with evidence, then return
     the asset to inventory.
   - Provider cancellation is independently reversible until the provider
     termination date when supported. Renewal reversal within 72 hours is a
     manual-support exception, never guaranteed automation.
   - Provider termination deletes provider snapshots/backups; destructive
     confirmation must state that consequence.
   - Business accounts receive no assumed consumer 14-day revocation right.

### 1.2 Deterministic commercial boundary

The canonical semantics are `MarginCalculator.php`. Proposal/report JavaScript
is a narrower `cost_plus_pct` mirror; `cost_plus_amount` and `fixed` remain
server-side until explicitly added with parity fixtures. The ordered model is:

1. provider base plus selected provider options/add-ons in EUR;
2. provider/vendor tax cash when charged; include it in economic landed cost
   unless verified recoverability excludes it;
3. EUR-to-INR reference conversion;
4. FX volatility/acquisition-card and payment buffers;
5. seller owner margin using `cost_plus_pct` on landed economic cost;
6. managed INR lines, without FX or provider tax, with owner scope when selected;
7. Securiace customer output GST from the WHMCS tax snapshot, after margin,
   exactly once and only with verified registration;
8. final display rounding and output-tax net-revenue evidence.

One immutable pricing state must drive table, detail/configurator, comparison,
copied quote, proposal preview, and HTML/JSON/CSV exports. FX/card markup and
owner margin are separate fields. AI/Codex may write bounded narrative only; it
never computes or changes price, tax, FX, owner margin, discount, SLA,
availability, provisioning, or configuration facts.

### 1.3 WHMCS-native setup behavior

Never replace or simulate WHMCS Product/Service > Module Settings. Preserve all
four native choices:

1. automatically set up as soon as an order is placed;
2. automatically set up as soon as first payment is received;
3. automatically set up when an administrator manually accepts a pending
   order;
4. do not automatically set up.

The module callback accepts the WHMCS intent and applies the same durable,
idempotent capability checks. Admin-driven/manual acceptance is the product
default, but the other three choices remain fully supported.

## 2. Supersession matrix

| Source | Preserve | Amend/supersede | Destination |
|---|---|---|---|
| Plan 001 predeploy hardening | fail-closed health, digest/release proof, route checks | add proposal/tax/capability/schema gates | Phase 7 |
| Plan 002 deploy safety | immutable artifacts, explicit remote pull, rollback evidence | no production mutation from stale PR branches | Phase 7 |
| Plan 003 scraper resilience | Rust canonical scraper, taxonomy, gaps, Object Storage committed capacity | separate scraper facts from provisioning capability; correct Core false-NVMe audit | Phases 1 and 4 |
| Plan 004 proposal maker | immutable versions, visibility policy, comparisons, AI boundary, WHMCS ticket/email delivery | default OpenAI model/API guidance; current tax gate; no 15% hidden clamp | Phases 2 and 3 |
| PR #24 | managed tiers, catalog/model/report integration, resilient scraping findings | do not merge its stale ancestry, generated outputs, or graph artifacts | selective source port only |
| PR #25 | separate owner margin, proposal snapshot/document model, visibility states, Codex-safe narrative, export concepts | do not merge stale scraper parity drift or legacy module assumptions | selective source port only |
| current `main` | Rust API, `contabo_pricing`, canonical `securiacevps`, paid-order snapshots, capability registry, durable operations, Hallmark UI | extend in place; do not reintroduce Python/control-plane coupling | all phases |
| archived Python/control plane | failure scenarios and test vectors | never deploy or add runtime dependency | reference only |
| legacy `contabo_vps` | cohort compatibility entrypoint | no new features; retire after reassignment | migration only |

PR #24/#25 should be closed as superseded only after successor work is green
and reviewable. Do not rebase, merge, or force-push them as part of this plan.

## 3. Release-blocking evidence and defects

### 3.1 Screenshot pricing contradiction

Observed Storage VPS 30 state:

- toolbar displayed FX markup 45%;
- modal displayed/applied 15.0%;
- INR 2,049 equaled EUR 16.52 × 107.856 × 1.15;
- the UI also claimed ambiguous “+18% GST/incl GST” although that amount did
  not include it and did not distinguish provider tax from output GST.

Acceptance:

- 45% displayed means 45% calculated, or the input is rejected before state
  changes; there is no hidden 15% clamp;
- table, modal, comparison, copy, preview, and all exports share the same
  normalized state and calculation function;
- ordered formula and tax-mode provenance are visible in internal evidence;
- every input change reactively recalculates open/current surfaces;
- provider tax and output GST have separate labels, state, sources, and
  recoverability/registration gates;
- enabled-but-unverified output GST fails closed and raises a mandatory review warning.

### 3.2 Live release-binary audit

The direct release binary fetched 36/36 plans and 1,975 options with no fetch
failures and six gaps, but exposed:

- `--dry-run --json ... scrape` logged `dry_run=false`, wrote outputs, and
  emitted no JSON stdout summary;
- configs/view model carry Core VPS, Performance VPS, and Max Performance VPS,
  while the pricing dataset can flatten to legacy family names;
- six false Core VPS `storage_policy_violation` NVMe gaps are emitted even
  though live Core configuration choices are SSD-only;
- exact Core VPS 4 one-month selection:
  base EUR 5.50 + India EUR 2.40 + Auto Backup EUR 1.65 + Private Networking
  EUR 2.29 + 200 GB SSD EUR 1.50 = EUR 13.34 recurring before active tax and
  owner pricing;
- generated passwords are 8–30 characters, alphanumeric only.

Generated `data/output/*` from that diagnostic run is not a commit artifact.
Add deterministic fixtures instead.

### 3.3 Current taxonomy contract

- Core VPS: SSD storage options only.
- Performance VPS: NVMe storage options only.
- Max Performance VPS: current name for legacy VDS; preserve alias imports.
- Object Storage is not eligible for Founder Managed server tiers.

## 4. Target architecture and roadmap

### Phase 0 — canonical branch and contract reconciliation

Dependencies: none.

- Start every successor slice from latest `origin/main`.
- Port only bounded files/behavior from PR #24/#25.
- Check in managed commercial policy v2, proposal snapshot v1 foundations,
  formula provenance, visibility semantics, and golden fixtures.
- Keep mutable tax/rate/policy fields versioned and snapshotted.
- Resolve the legal GST conflict before production enablement.

Acceptance:

- no generated scrape datasets or graph artifacts from stale branches;
- Node policy/proposal tests pass;
- generated report has no hardcoded 15% clamp and no unverified tax charge;
- source branch contains only intentional files.

### Phase 1 — scraper truth and release CLI

Dependencies: Phase 0.

- Reproduce and fix the `--dry-run`/subcommand argument wiring; prove no file
  mtimes or contents change and JSON summary appears on stdout.
- Emit canonical and legacy family fields consistently in every JSON/CSV view.
- Change storage-policy validation to inspect selectable configuration options,
  not legacy marketing/base storage fields.
- Add the exact Core VPS 4 fixture and password-rule assertions.
- Preserve Rust as canonical; Node parity is a compatibility gate or is
  explicitly retired with a migration decision.

Acceptance:

- dry-run before/after tree hashes identical;
- Rust/Node normalized parity green or a reviewed retirement record exists;
- Core has zero NVMe selectable options/gaps; Performance has zero SSD
  selectable options; Max Performance alias round-trips;
- 36-plan fixture schema passes without committing live volatile outputs.

### Phase 2 — Proposal Studio domain and immutable versions

Dependencies: Phases 0–1 commercial schema.

- Add checked-in strict schemas for snapshot, proposal document, and AI
  narrative.
- Persist proposal aggregate, immutable versions, normalized line items,
  pricing/tax provenance, visibility, source hashes, approval state, artifacts,
  AI usage, and delivery attempts in module-owned WHMCS MySQL tables.
- Revalidate browser imports against server catalog and recalculate all money.
- Support primary plus four independently priced alternatives.
- Visibility states: show, total-only, silent-include, internal-only, exclude,
  and calculated-only.
- Saved versions never recalculate from mutable current terms.

Workflow:

`draft -> deterministic preview -> optional AI narrative -> review -> approved -> delivery`

A changed fact creates a new draft version and invalidates approval. Delivery
uses only the approved immutable version and an idempotency key.

### Phase 3 — OpenAI-compatible narrative and WHMCS delivery

Dependencies: Phase 2.

Provider configuration:

- provider default: `openai`;
- cost-efficient default model: `gpt-5.6-luna`;
- manual model override;
- bounded reasoning, output tokens, timeout, retries, and estimated spend;
- Responses API default for OpenAI;
- explicit Chat Completions compatibility mode for third-party endpoints;
- never hardcode an OpenAI model for generic compatible endpoints;
- capability-detect structured outputs and validate the strict proposal JSON
  schema;
- deterministic fallback on timeout, refusal, invalid JSON/schema, or budget
  failure;
- persist model, endpoint profile identifier (not key), prompt/schema versions,
  token usage, and estimated cost on each immutable version;
- API key encrypted at rest and absent from logs, templates, prompts, artifacts,
  and exception detail.

Delivery:

- use WHMCS LocalAPI `OpenTicket` with markdown and supported base64
  attachments, or `AddTicketReply` for an existing ticket;
- use `SendEmail` with a dedicated WHMCS proposal template/custom message;
  attach dynamic proposal files through a narrowly scoped `EmailPreSend` hook
  keyed by immutable delivery token and proposal version; never invoke
  PHPMailer directly;
- restrict recipient to the selected WHMCS client/contact;
- store attachments outside the webroot and validate MIME, safe filename, size,
  and hash; when over the cap, send a secure expiring client-area download link;
- use a durable outbox keyed by proposal-version/channel/recipient;
- treat delivery timeout as unknown/manual-review and reconcile before resend;
- record approving/sending admin, recipients, rendered-body hash, attachment
  hashes, WHMCS ticket/email result, and timestamps without secrets;
- human approval and an immutable preview are required; duplicate sends return
  the original delivery result.

Compatibility gates cover WHMCS 8.12/8.13 and 9.x with the supported PHP matrix.
Do not claim direct-email attachment support until both runtime families pass.

### Phase 4 — resource adapters and capability registry

Dependencies: stable paid-order snapshots and lifecycle operations.

Use separate adapters:

- **Compute adapter:** certified VPS/VDS create, delete, start, stop, reinstall,
  password reset, observations, and supported upgrades.
- **Object Storage adapter:** order, capacity upgrade, cancellation, autoscale,
  stats, S3 user/bucket/policy/quota leasing, and regional pool reconciliation.
- **Inventory/manual adapter:** Storage VPS and Dedicated while official API
  support is absent, plus Dedicated/add-on upgrades requiring provider support.

Official capability constraints:

- Storage VPS is not currently supported by Compute API/CLI.
- Private Networking supports Cloud VPS/VDS, not Storage VPS.
- First private-network attachment requires paid addon plus reinstall; later
  attachment requires restart.
- VPS/VDS upgrades are provider daily-prorated.
- Direct downgrade is unsupported: create smaller service, migrate, then cancel
  old.
- Dedicated is region-bound; upgrades/add-ons are manual support work.
- Object Storage permits one management resource per location and shared
  location credentials through user management.

Capability registry values are certified, unsupported, manual, inventory, or
canary-only. Unsupported actions are absent from Simple mode and cannot be
invoked by forged requests. Advanced mode exposes only capability-certified or
explicit manual workflows with consequences and acknowledgements.

Supported WHMCS/module surfaces to implement or certify:

- loader functions and metadata/module parameters;
- CreateAccount, SuspendAccount, UnsuspendAccount, TerminateAccount;
- ChangePackage upgrade/downgrade saga;
- reinstall, change/reset password, start/stop/restart, private networking/VPN
  where certified;
- Service Properties;
- ClientArea and ClientAreaCustomButtonArray;
- Admin Services Tab fields and AdminCustomButtonArray;
- admin dashboard widgets;
- module logging with redaction/correlation IDs;
- server sync and orphan/adoption reconciliation;
- custom actions and cron resume;
- Simple/Advanced mode policy.

### Phase 5 — Founder-hour ledger

Dependencies: managed purchase snapshot contract.

- Monthly entitlement grants by service and month.
- Append-only usage entries with work category, duration, actor, ticket/change
  reference, emergency flag, approval evidence, and immutable rate snapshot.
- Monthly hours expire; close job records unused balance without creating a
  credit.
- Discretionary credits require admin role, reason, issued-at, expires-at, and
  non-contractual marker.
- Overage estimate/approval precedes normal work. Emergency minimum
  stabilization can consume up to the configurable one-hour internal guardrail
  before further approval.
- Invoice draft/line generation uses the snapshotted INR 2,500/hour rate and
  active WHMCS tax mode. No AI calculation.
- Client area shows contractual monthly allowance and recorded usage; it never
  presents discretionary carry-forward as an entitlement.

### Phase 6 — provider contract, inventory, and reassignment

Dependencies: resource adapters and reconciliation.

Introduce durable records for provider contract, asset, client lease,
entitlement, cancellation intent, quarantine, sanitization proof, inventory
availability, carrying cost, and renewal deadline.

Normal cancellation:

`requested -> provider forward-cancel scheduled -> active through paid-through
-> access/export end -> provider termination or quarantine -> sanitize ->
inventory/closed`

Immediate/cause flow requires admin authority and legal/business confirmation.
No dual lease. Snapshot/backup deletion consequences are acknowledged. A
provider contract retained for economic reasons may return to inventory only
after client entitlement ends and sanitization/reversal checks pass.

Downgrade is a migration saga, not an in-place provider downgrade:

`quote smaller -> approve -> create -> migrate -> verify -> cut over ->
forward-cancel old -> dispose`

### Phase 7 — operator UI, tests, deployment, rollback

Dependencies: each preceding domain slice.

UI design contract:

- primary human is the WHMCS administrator/operator;
- focal point is the next safe action;
- hierarchy is proposal workflow, then commercial totals, then
  provider/provisioning evidence;
- preserve Hallmark warm-neutral/amber palette, type/spacing tokens, restrained
  borders, and flat operational surfaces;
- no generic card-grid redesign;
- Simple mode shows common safe actions; dangerous/manual/unsupported paths are
  capability-gated under Advanced;
- the Truth Rail connects:
  `client entitlement | WHMCS operation | provider asset | provider contract`
  and highlights disagreement, stale observation, and next repair action.

Exact gates:

1. `npm run test:node`
2. deterministic report generation in a temporary output copy; inspect
   consistency report and assert no unverified tax/15% strings;
3. report browser tests for table, modal, comparison, copy, HTML, JSON, CSV,
   reactive 45% FX, owner margin, and both tax modes;
4. `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
   and `cargo test --all-targets --all-features`;
5. both addon and server-module PHPUnit suites;
6. PHP syntax over every module PHP file on PHP 7.4-compatible syntax and
   qualified PHP 8.2/8.3 runtimes;
7. Hallmark audit and responsive/accessibility smoke;
8. package/release-contract tests, secret scan, and immutable artifact hash;
9. WHMCS 8.12/8.13 and 9.x install/upgrade/integration fixtures;
10. staging canaries for each certified provider capability;
11. predeploy gate proves database backup, module package, health beyond root
    HTTP 200, proxy route, cron progress, and rollback artifact.

Production is blocked until the PR is green, reviewed, and the operator
explicitly authorizes merge and deployment. The implementation agent never
self-merges. Deployment verifies the immutable package/image digest actually
running, then runs schema health, proposal preview without AI, operation
reconciliation, and a non-destructive provider observation.

Rollback:

- stop new writes/actions;
- restore previous immutable module package;
- roll back only backward-compatible code; do not drop proposal, ledger,
  operation, contract, or audit records;
- disable AI and provider write switches independently;
- restore previous report artifact/catalog pointer;
- reconcile in-flight operations before reopening actions.

## 5. Risk register and acceptance

| Risk | Control | Release acceptance |
|---|---|---|
| tax charged without registration | verified tax-mode snapshot, default off | unverified request adds zero tax and mandatory warning |
| visible 45%, calculated 15% | shared model and explicit 0–100% boundary | all six surfaces produce the same fixture total |
| mutable terms alter old sale | immutable purchase/proposal snapshots | old fixture unchanged after catalog mutation |
| AI changes money/scope | strict narrative schema and deterministic merge | hostile model fixture cannot alter commercial sections |
| unsupported action appears automated | certified capability registry | Storage VPS/Dedicated automated create absent |
| destructive networking surprises client | consequence acknowledgment + saga | reinstall/restart states and audit evidence persist |
| premature reassignment/data exposure | entitlement/quarantine/sanitize gates | no inventory lease before proof |
| pooled Object Storage cross-tenant access | policy isolation and purge canaries | deny cross-tenant fixture and verified cleanup |
| stale scraper branch regresses canonical module | selective ports from current main | diff has no stale outputs/control plane |
| WHMCS version incompatibility | explicit runtime matrix | both supported families green |
| duplicate ticket/email/provider mutation | idempotency keys and durable result | replay returns same outcome |
| production artifact drift | immutable digest/package verification | running identity equals approved artifact |

## 6. First successor slice scope

This first slice is intentionally bounded to:

- this successor plan and preserved Plans 001–004;
- managed catalog/model/tests with versioned commercial terms and safe tax
  activation;
- proposal domain/tests with separate FX/owner fields, all visibility states,
  ordered provenance, immutable managed terms, and deterministic AI boundary;
- report-side proposal workspace ported without stale datasets;
- regression coverage for 45% versus 15%, tax disabled/enabled, and the exact
  Core VPS 4 EUR 13.34 combination.

Deferred precisely:

- WHMCS proposal persistence/UI/AI/delivery;
- managed-hour database ledger and invoice workflow;
- scraper CLI/family/storage-gap fixes;
- Compute/Object Storage/manual adapters;
- provider-contract inventory and reassignment;
- Truth Rail implementation;
- production deployment.
