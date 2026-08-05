# Report proposal workspace

Status: implemented on the `feat/proposal-generator` branch.

The report now has two related but separate responsibilities:

1. show a transparent provider-price calculator; and
2. turn a reviewed selection into a client proposal artifact.

The second responsibility is deliberately structured. The browser builds a
`proposal.snapshot.v1` object, the Rust API validates its primary plan against
the loaded snapshot, and Codex CLI may return only a `proposal.v1` JSON document.
The browser renders that document through an allow-list renderer. Codex never
returns HTML, JavaScript, CSS, a checkout action, or a provisioning action.

## Margin semantics

The report calls the new field **Owner markup (%)**. It is a cost-plus markup,
not a gross-margin percentage. `10%` means “sell the relevant cost at 1.10×”.
It is intentionally distinct from card/FX markup.

For provider charges, the calculation order is:

```text
provider option cost
  → provider tax/GST (when enabled)
  → EUR→INR conversion (when INR is selected)
  → FX/card markup
  → owner markup
```

One-time setup fees follow the same order and are included in the billed total.
The canonical EUR provider value remains available in the proposal JSON and
server facts even when the report displays the seller value.

The scope selector has two safe choices:

- **Provider charges** (default): apply owner markup to Contabo charges only.
- **Provider + managed**: also apply owner markup to the INR managed-service
  add-on.

Managed service prices are canonical INR and never receive EUR→INR FX markup.
GST follows the managed catalog's tax basis. A managed tier is not a published
SLA or automatic provisioning promise; draft/approval flags remain visible.

FX markup is capped at 15%; owner markup is capped at 100%. Invalid/stale
local-storage values are normalized back into the visible input immediately.

## Proposal profiles and visibility policy

The wizard supplies these profiles:

- `quick-quote`: short client quote;
- `technical`: configuration and option details;
- `managed`: infrastructure plus Founder security/server-management hours;
- `comparison`: primary plan versus plans selected in the compare drawer; and
- `internal`: adjustment and review detail for the operator.

Each content group can be set to `show`, `total_only`, `silent_include`,
`internal_only`, or `exclude`:

| Group | Meaning |
|---|---|
| Configuration | Plan, term, specs, and selected options |
| Provider pricing | Provider/seller totals |
| Provider line items | Monthly/setup detail versus total-only |
| Managed services | Founder hours, tier, inclusions, exclusions |
| Alternatives | Compare-drawer plan comparison |
| Source links | Provider links and snapshot provenance |
| Tax / FX / owner markup | Commercial adjustments and disclosure policy |
| Client notes | Operator-supplied notes intended for the recipient |
| Internal notes | Operator-only context |

`silent_include` means a value still contributes to the structured calculation
and full JSON audit snapshot but is not mentioned as a separate line in the
client document or policy-filtered CSV; the seller total still includes it.
Mandatory
warnings cannot be hidden: missing FX, stale snapshots, capped adjustments,
and cross-family comparison warnings always remain review callouts.

## Codex CLI boundary

The report never spawns a process. When the report is served by the Rust binary,
proposal routes are available on loopback without a token, or behind the
configured bearer token on an intentionally exposed bind address:

```text
GET  /api/v1/proposals/capabilities
POST /api/v1/proposals/preview
POST /api/v1/proposals/generate
GET  /api/v1/proposals/:id
GET  /api/v1/proposals/:id/artifact
```

The adapter invokes `codex exec --ephemeral --sandbox read-only` with a bounded
temporary working directory, a JSON schema, a 120-second timeout, bounded
stdout/stderr, and a bounded in-memory job table. `CONTABO_CODEX_BIN` may point
to an explicit local binary; otherwise `codex` is resolved from `PATH`.

The prompt fences the context, client notes, visibility policy, and server facts
as data. It instructs Codex to treat scraped labels and client notes as
untrusted text. If Codex is absent, times out, exits non-zero, emits malformed
JSON, or violates the block allow-list, the deterministic document is returned
with a review warning.

The server's `server_facts` override client-supplied quote prose. The boundary
validates plan slug, term, option labels, and storage-capacity selections against
the loaded snapshot, then recalculates provider monthly/setup/period totals from
the server catalog. Codex output is post-validated and canonical commercial
sections are restored from those server facts; only short non-commercial
narrative blocks can be adopted from the model response.

## Exports and approval

The wizard supports:

- HTML: escaped structured document with small standalone styles;
- JSON: full `proposal.snapshot.v1`, including internal audit inputs;
- CSV: stable provider/seller/managed summary rows; and
- clipboard brief: a short human-reviewable summary.

The operator must review the preview before sending. Exports do not contain
passwords, auth tokens, checkout credentials, or payment actions. A proposal is
not an order and does not call Contabo checkout.

## Edge-case contract

- No FX rate: INR is unavailable and the mandatory warning stays visible.
- GST off: tax rows are omitted from the seller calculation but raw provider
  prices remain intact.
- Setup-only fee: setup is still marked and included in period total.
- Storage capacity: capacity pricing is represented as a selected structured
  delta, not as a fabricated compute option.
- Managed tier on an ineligible family: it is not offered by the report.
- More than four alternatives: the compare drawer and proposal snapshot cap it.
- Mixed-family comparison: allowed for exploration but produces a mandatory
  parity warning.
- Stale snapshot: allowed for review/export, never silently represented as
  current.
- Malicious client note/Codex text: escaped or rejected; no arbitrary HTML block.
- `file://` report: deterministic preview/export works; local API generation
  explains that the Rust server is required.
- Non-loopback server without bearer token: proposal process-spawning routes
  return 503.

## Test and release gates

```bash
npm test
cargo test --test api_smoke
cargo build --release
node .github/scripts/generate_html.js
```

The browser smoke should verify the owner field, clamping, proposal preview,
fallback, and all three downloads. The release binary should be started
directly (not through a sandbox wrapper) and checked at `/report.html` and
`/api/v1/proposals/capabilities`.
