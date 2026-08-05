# Plan 004 — WHMCS proposal maker, AI narrative, and native delivery

Status: DRAFT for owner review · audit/amendment 2026-08-05 · implementation not
started in this plan

This plan extends the existing Contabo Pricing addon at
whmcs-module/modules/addons/contabo_pricing/. It does not create a second WHMCS
module. The module will let an authorized WHMCS administrator select or import a
validated Contabo quotation, add managed-service and owner-margin controls,
generate an optional AI-assisted proposal narrative, review it, and deliver it
as either a WHMCS support ticket or a WHMCS-managed email.

The financial and configuration facts remain deterministic and server-owned. AI
is a bounded writing assistant only. A proposal must still be usable when AI is
disabled, unavailable, too expensive, or returns invalid content.

## 1. Outcome and non-negotiable rules

The finished addon must provide:

1. A proposal workspace in the existing addon admin area.
2. A new proposal form that can start from:
   - current WHMCS-synchronized Contabo catalogue data;
   - a report export containing proposal.snapshot.v1 JSON; or
   - a previously saved proposal version.
3. Deterministic recalculation of plan, term, region, operating system,
   storage, backup, networking, managed-service, tax, FX, FX-markup, and
   owner-markup values.
4. Per-section visibility controls:
   - show as a line item;
   - show only in a subtotal or summary;
   - silent include (count in the client total but do not mention it);
   - internal only (never sent to the client);
   - exclude (not counted);
   - calculated-only (a derived audit row, such as a tax or conversion basis,
     with no client-facing line).
5. Comparison proposals for a primary plan and up to four alternatives, with
   comparison facts and prices calculated independently and rendered according
   to the same policy.
6. An optional OpenAI-compatible provider integration with:
   - an explicit cost-efficient default model;
   - a manually configurable model override;
   - encrypted API-key storage;
   - configurable endpoint, request style, timeout, output budget, and
     retry/cost limits;
   - strict structured output validation;
   - deterministic fallback text.
7. A human review and approval gate before any delivery.
8. Delivery through WHMCS-supported internal mechanisms:
   - OpenTicket or AddTicketReply for support tickets;
   - a named WHMCS email template and WHMCS mail/queue attachment path for
     direct email.
9. Immutable proposal versions, delivery idempotency, audit records, retention,
   and retry-safe failure handling.
10. Compatibility verification against the available WHMCS 8.12.1 source and a
    WHMCS 9.x fixture/runtime before claiming production support.

The implementation must not:

- let the model set or change prices, tax, FX, markup, SLA, discount,
  provisioning, checkout, or security facts;
- send a draft or unapproved version;
- accept arbitrary client email addresses, message headers, file paths, HTML,
  Smarty, or PHP from an admin form or model response;
- write tickets directly into WHMCS core tables;
- use raw mail(), an independently bundled PHPMailer, or an external delivery
  service as a substitute for WHMCS mail delivery;
- expose Contabo credentials, generated passwords, API tokens, internal notes,
  or hidden owner margin in a client artifact;
- make a proposal an order, payment, provisioning request, or automatic
  repricing mutation.

## 2. Current state and identified gaps

The repository already contains most of the report-side foundation:

- .github/scripts/proposal_model.js defines proposal.snapshot.v1 and
  proposal.v1, deterministic quote calculations, visibility groups, managed
  services, owner markup, and an escaped renderer.
- src/api/proposals.rs provides local preview/generation routes and a
  Codex-based local narrative boundary. That process boundary is appropriate
  for the local report, but it must not be invoked as a remote process by
  production WHMCS.
- data/managed_services_catalog.json is the managed-service source, including
  the Founder Managed Track:
  Solo Managed, Growth Managed, and Business Managed, with 1, 3, and 6
  Founder hours per month respectively.
- whmcs-module/modules/addons/contabo_pricing/ already has settings, API
  client, installer/migrations through schema version 8, admin dispatch,
  templates, sync history, approval/repricing flows, and an idempotent email
  template seeder.
- whmcs-module/modules/addons/contabo_pricing/lib/ApiClient.php already avoids
  pulling in Guzzle and uses a small cURL boundary, which is the right place
  for an OpenAI-compatible HTTP adapter.

The audit found these gaps that the implementation must close:

1. The report/Rust proposal contract and the WHMCS PHP module do not yet share
   checked-in JSON schemas or golden parity fixtures.
2. The WHMCS addon has no proposal aggregate, immutable versions, artifacts,
   AI usage records, or delivery records.
3. The existing renewal MarginCalculator is intentionally for the renewal
   engine. Reusing it for report owner markup would silently change the current
   report semantics.
4. The current addon settings contain FX markup and GST settings, but no
   proposal-specific owner-markup policy, AI provider, delivery, retention, or
   security controls.
5. WHMCS SendEmail has no documented dynamic attachment parameter. Ticket APIs
   do support base64 JSON attachments, but direct email attachments require a
   tested WHMCS mail/storage compatibility adapter.
6. The module README and composer metadata disagree about the PHP floor
   (README/entrypoint say PHP 8.1; composer and code aim for PHP 7.4 syntax).
   “WHMCS 8.x and 9.x” is not a sufficient compatibility claim without a
   version matrix.
7. The existing local report uses the current Contabo taxonomy:
   Core VPS is SSD-only, Performance VPS is NVMe-only, and Max Performance VPS
   is the current name for the former VDS category. Historical aliases must be
   preserved in imports and comparisons.
8. A browser-exported payload is user-controlled input. WHMCS must validate the
   selected plan, options, managed tier, source snapshot, and all money again
   before persisting or sending.
9. A single send button could otherwise create duplicate tickets/emails,
   resend stale pricing, or send a version different from the one shown in the
   preview.
10. Existing report inputs include client notes and scraped labels. They are
    untrusted data and must not become prompt instructions, executable markup, or
    email headers.

## 3. Compatibility and evidence baseline

The execution team must first run the compatibility spike before implementing
the delivery layer. The spike must be made against the checked-in
WHMCS-8.12.1 source and a supported WHMCS 9.x fixture/container.

Evidence used for this plan:

- WHMCS addon modules provide admin pages, hooks, activation, and role-based
  access: https://developers.whmcs.com/addon-modules/
- Admin addon output is emitted through the module output function and should
  use the module link, POST actions, and WHMCS admin session:
  https://developers.whmcs.com/addon-modules/admin-area-output/
- OpenTicket supports localAPI calls and base64 JSON attachments:
  https://developers.whmcs.com/api-reference/openticket/
- AddTicketReply supports the same attachment shape:
  https://developers.whmcs.com/api-reference/addticketreply/
- SendEmail supports a named template/custom variables and related WHMCS
  entity IDs, but its documented parameters do not include dynamic
  attachments:
  https://developers.whmcs.com/api-reference/sendemail/
- WHMCS 9 requires PHP 8.2+ and removes PHP 7.4/8.1 support:
  https://docs.whmcs.com/releases/9-0/9-0-release-notes/
- WHMCS 9 system requirements:
  https://docs.whmcs.com/9-0/installation-guide/system-requirements/
- OpenAI Structured Outputs is the preferred contract when the compatible
  provider supports it:
  https://platform.openai.com/docs/guides/structured-outputs
- The OpenAI GPT-5 mini model page describes it as a cost-efficient,
  structured-output-capable model for well-defined tasks:
  https://developers.openai.com/api/docs/models/gpt-5-mini

The support target is:

- WHMCS 8.12.1 and 8.13.x on PHP 7.4/8.1/8.2 as applicable to the installed
  WHMCS release;
- WHMCS 9.0.x and later supported 9.x releases on PHP 8.2/8.3.

The module code should retain a PHP 7.4-compatible syntax baseline so the same
source can run on the supported WHMCS 8 target and PHP 8.2+ on WHMCS 9. That
does not claim support for every WHMCS 8.0–8.11/PHP 7.2 installation. If the
owner requires those older combinations, add them to the matrix and run their
fixtures before advertising them.

Avoid PHP 8-only syntax, enums, readonly properties, union types, and
framework features not already present in the addon. Keep Smarty templates
compatible with Smarty 3/4 conventions and do not introduce the legacy tags
removed in WHMCS 9.

## 4. Shared proposal contract

### 4.1 Canonical schemas

Add checked-in schemas and fixtures, then make all three producers/consumers
validate them:

- schemas/proposal.snapshot.v1.json
- schemas/proposal.v1.json
- schemas/proposal.ai-narrative.v1.json
- tests/fixtures/proposals/*.json

The existing JavaScript/Rust shape should be preserved through v1. If a
normalization is required for PHP parity, add an explicit versioned adapter
rather than silently changing v1.

The snapshot is the input/facts contract. It must contain:

- schema version and source snapshot hash;
- generated-at time and catalogue version;
- primary selection and optional comparison selections;
- plan family, canonical family, legacy aliases, region, term, OS, and option
  selections;
- normalized provider line items;
- managed-service line items;
- tax/FX/FX-markup/owner-markup calculation metadata;
- visibility policy;
- source links and warnings;
- internal provenance and validation status.

The document is the rendered commercial contract. It must contain:

- client-facing title and introduction;
- configuration and inclusion sections;
- authoritative price summaries;
- managed-service description when selected;
- comparison section when selected;
- assumptions, exclusions, warnings, validity date, and next steps;
- an internal-only rendering variant;
- a plain-text rendering for tickets and fallback email.

Persist both the normalized snapshot and the rendered document inputs. Do not
reconstruct an old proposal from the current catalogue after a price change.

### 4.2 Money and markup semantics

Use one shared calculation contract and golden fixtures. The required order is:

1. provider base price plus selected provider option deltas;
2. provider tax/GST according to the existing report contract;
3. EUR-to-INR conversion;
4. FX markup;
5. owner markup;
6. final display rounding.

Managed services are canonical INR amounts with their own GST basis and do not
receive FX conversion. Owner markup applies to managed services only when the
owner explicitly chooses provider_and_managed scope. It must not be
double-applied when a managed price already contains an owner adjustment.

Preserve the current limits unless the owner explicitly changes the contract:

- FX markup: 0–15%;
- owner markup: 0–100%;
- owner scope: provider_only or provider_and_managed.

The WHMCS implementation must not use the renewal engine's landed-cost
formula as a shortcut. Create a proposal-specific calculator or a contract
adapter and cover it with fixtures for:

- one-month and annual terms;
- EUR-only provider pricing;
- missing/zero/negative FX;
- GST enabled/disabled;
- no options and all supported options;
- Core SSD-only and Performance NVMe-only validation;
- legacy VDS/Max Performance aliases;
- each Founder Managed tier;
- owner markup hidden, shown, silent-included, and excluded;
- comparison plans with different terms;
- rounding boundaries and zero-valued optional rows.

Prefer integer minor units for INR and fixed decimal strings for provider
currency in the persisted contract. If the existing v1 payload still carries
floating values, normalize them at the PHP boundary and retain the original
source hash; never send binary floating-point calculations to the model.

### 4.3 Visibility and disclosure policy

The UI must make the policy explicit for each section and selected add-on.
Defaults should be conservative:

- configuration: show;
- provider line items: show;
- tax/FX basis: summary-only or calculated-only;
- FX markup: summary-only or internal-only;
- owner markup: internal-only;
- managed services: show when intentionally sold;
- alternatives: show only when selected;
- source links: internal-only unless enabled;
- client notes: show only after preview;
- internal notes: internal-only;
- warnings/exclusions: show when they affect the client decision.

“Silent include” is the explicit way to charge for a component while omitting
its name and line item from the client artifact. It must remain visible in the
internal audit view and in the immutable snapshot. “Exclude” means it is not
included in the total. The renderer must not infer either behavior from a
missing label.

The server must reject contradictory policies, for example:

- a child line excluded while a mandatory parent is shown as included;
- a comparison alternative silently included in the primary total;
- a tax row excluded while the contract claims a tax-inclusive total;
- an owner adjustment excluded from the total but displayed as included;
- internal notes copied into a client-visible narrative.

### 4.4 Report import and stale-data handling

The report UI should export proposal.snapshot.v1 JSON as the integration
artifact. The WHMCS module should accept that JSON through a bounded file
upload or paste/import flow. It should not treat report.html as a trusted
database or execute uploaded HTML.

On import:

1. parse and size-check the JSON;
2. validate the schema and hash;
3. validate slugs, terms, option values, canonical family, storage policy, and
   managed catalog IDs against the server-side catalogue;
4. recalculate all money on the WHMCS side;
5. mark any source-price mismatch as stale;
6. require an explicit refresh or owner confirmation before approval;
7. persist the normalized server snapshot, not the untrusted raw input alone.

Provide a server-side “create from current catalogue” path so proposals remain
usable without a browser report. Add a read-only managed-services catalogue
endpoint or a versioned packaged catalogue with a hash; do not maintain a
second hand-edited Founder tier definition in WHMCS.

## 5. WHMCS module architecture

Extend the existing namespace and dependency boundaries. The proposed classes
are illustrative names; retain the repository's current PHP 7.4-compatible
style.

Add a proposal domain under
whmcs-module/modules/addons/contabo_pricing/lib/Proposal/:

- ProposalService: orchestration, status transitions, and version creation;
- ProposalInputNormalizer: report import and admin form normalization;
- ProposalValidator: schema, catalogue, client, stale-data, and policy checks;
- ProposalQuoteCalculator: deterministic proposal money contract;
- VisibilityPolicy: validation and client/internal projection;
- ProposalRenderer: escaped HTML, plain text, JSON, and attachment manifest;
- ProposalRepository: Capsule queries for module-owned tables;
- ProposalAudit: actor, correlation, status, and non-secret event records.

Add AI boundaries under lib/Proposal/Ai/:

- ProviderInterface;
- OpenAiCompatibleProvider;
- RequestStyleAdapter for responses/chat/auto;
- NarrativeSchemaValidator;
- PromptBuilder;
- DeterministicNarrative;
- AiUsageMeter and budget guard.

Add delivery boundaries under lib/Proposal/Delivery/:

- DeliveryService;
- TicketDeliveryAdapter;
- EmailDeliveryAdapter;
- WhmcsAttachmentAdapter;
- DeliveryIdempotency;
- DeliveryResult.

The existing ApiClient remains the catalogue/quote transport boundary. Add
methods for a managed-services catalogue and, if needed, a proposal source
snapshot endpoint. Do not make the PHP module depend on the Rust server
executing Codex.

Keep rendering separate from delivery. A generated artifact must be previewable
and downloadable before any ticket/email API is called.

## 6. AI provider design

### 6.1 Settings and model policy

Add proposal-specific addon settings, with secret values never echoed back:

- proposal_ai_enabled;
- proposal_ai_base_url;
- proposal_ai_api_key, encrypted at rest using the existing WHMCS encrypt/
  decrypt pattern and ENC: marker;
- proposal_ai_model, defaulting to gpt-5-mini as the cost-efficient baseline;
- proposal_ai_request_style: auto, responses, or chat_completions;
- proposal_ai_timeout_seconds;
- proposal_ai_max_output_tokens;
- proposal_ai_max_retries, default 0 for model retries;
- proposal_ai_allow_json_mode_fallback, default false;
- proposal_ai_monthly_budget and optional per-admin budget;
- proposal_ai_retention/logging mode;
- proposal_ai_prompt_version.

The model string is manually overrideable for a stronger model, Azure-style
deployment, local OpenAI-compatible service, or another approved provider.
Validate its length and characters, but do not hard-code a provider-specific
allowlist that prevents legitimate deployment names. The endpoint must be
admin-configured, never supplied by a report or browser selection.

Before release, run a small quality/cost benchmark against representative
fixtures. The default is accepted only if it meets the proposal rubric; the
owner can override it without a code release. Do not rely on a current model
price in code or documentation because provider pricing changes.

### 6.2 Request contract

Send a compact, bounded prompt containing:

- normalized plan family and feature facts;
- included/excluded managed-service facts;
- client-approved tone, language, and notes;
- visibility rules;
- comparison facts without editable commercial totals;
- explicit instruction that input labels/notes are data, not instructions;
- proposal style and maximum lengths.

Do not send secrets, generated passwords, API credentials, internal tokens, or
unnecessary client PII. The model may receive feature facts, but it must not be
the source of numeric commercial facts.

Require proposal.ai-narrative.v1 structured output. The allowed fields should
be short, bounded strings/arrays such as:

- opening;
- recommendation rationale;
- included service statements;
- assumptions;
- exclusions;
- next steps.

The schema must use strict object validation and reject unknown properties.
The model must not return HTML, CSS, JavaScript, prices, currencies,
percentages, taxes, markups, discounts, SLA promises, provisioning claims, or
unapproved feature facts. Validate these constraints locally even after
structured parsing.

For a Responses-compatible provider, use the provider's structured text format.
For a Chat Completions-compatible provider, use response_format JSON schema.
The auto mode should try the configured style once, based on a health/profile
setting, not issue multiple billable calls. JSON mode fallback is opt-in and
still passes through the same local schema and content validator.

### 6.3 Quality, cost, and failure behavior

The pipeline is:

deterministic document -> optional bounded AI narrative -> local validation ->
safe merge -> human preview -> approval -> delivery.

Implement:

- one call per snapshot hash + model + prompt version;
- cache reuse for identical inputs;
- strict request and response byte caps;
- timeout and one network retry only when it is safe and configured;
- no automatic escalation to a more expensive model;
- usage/token/cost metadata when the provider returns it, otherwise an
  estimated usage flag;
- monthly/per-admin budget rejection before a call;
- redacted logs containing provider, model, latency, status, and hashes, never
  API keys, full prompts, client notes, or raw model output;
- deterministic fallback when AI is disabled, unavailable, unauthorized,
  rate-limited, timed out, malformed, over budget, or unsafe.

The UI must show “deterministic narrative” or “AI narrative pending review”
and the exact warning needed for an administrator to decide whether to proceed.
There is no silent downgrade from an AI failure to a misleading claim.

## 7. Persistence, versioning, and retention

Advance Installer::SCHEMA_VERSION from 8 with additive, idempotent migrations.
Do not drop or rewrite existing pricing/sync/repricing tables. Prefer a single
proposal migration version containing these module-owned tables:

### mod_contabo_proposal

Aggregate row:

- numeric id and public non-sequential identifier;
- WHMCS client/contact/service IDs where applicable;
- title and status;
- current version ID;
- created/updated timestamps;
- creating/updating admin IDs;
- last error code and correlation ID.

### mod_contabo_proposal_version

Immutable business version:

- proposal ID and monotonically increasing version;
- schema/profile/prompt versions;
- normalized snapshot JSON and document JSON;
- snapshot hash and catalogue hash/time;
- rendered HTML/plain-text references or bounded bodies;
- AI provider/model/status/warning;
- token/estimated-cost/latency metadata;
- created, approved, and approved-by fields;
- stale/source state.

### mod_contabo_proposal_delivery

One attempted logical delivery:

- proposal version ID;
- channel ticket or email;
- draft/queued/sending/sent/failed status;
- unique idempotency key;
- client/contact/department/ticket IDs;
- template name and subject hash;
- attachment manifest hash;
- WHMCS result/code and sanitized error;
- attempt count and timestamps.

### mod_contabo_proposal_asset

Generated attachment metadata:

- proposal version and logical asset name;
- WHMCS storage key, never an absolute filesystem path;
- safe display filename and MIME;
- byte size and SHA-256;
- retention/cleanup state and timestamps.

Use varchar statuses rather than database ENUMs for portability. Avoid foreign
keys to WHMCS core tables so addon migrations remain portable across supported
installations. Add indexes for client, status, public ID, version, and
idempotency key. Apply length limits to all JSON/text inputs before writing.

Status transitions must be explicit:

draft -> previewed -> generated -> edited -> approved -> sending -> sent
or failed.

Regeneration and edits create a new immutable version. A delivery references
one exact version and never “latest”. Approval is invalidated when the source
catalogue hash or commercial selection changes. Only the approved version can
be sent.

Proposal purge must be explicit or retention-driven through the existing
DailyCronJob hook. It may delete only proposal-owned assets/rows and must retain
a minimal delivery audit summary if configured. Deactivation must retain data.
No proposal table may contain API keys, passwords, or raw model prompts by
default.

## 8. Admin UI and actions

Add a Proposals sidebar entry and follow the existing AdminController dispatch,
module link, Smarty layout, CSRF token, redirect, and audit conventions.
Expected actions:

- proposals;
- proposal-create;
- proposal-import;
- proposal-preview;
- proposal-generate;
- proposal-edit;
- proposal-approve;
- proposal-delivery-preview;
- proposal-send;
- proposal-retry;
- proposal-archive;
- proposal-download;
- proposal-test-email.

Use the existing standalone AJAX route only for bounded JSON operations and
apply check_token() to every mutation. Every POST must use POST/redirect/GET;
do not put proposal JSON or secrets in query strings.

The proposal form must include:

- WHMCS client and optional contact;
- primary plan/profile/immutable catalogue version;
- term, region, OS, storage, backup, networking, and add-ons;
- managed tier, quantity, founder-hours disclosure, and review flags;
- comparison plans (maximum four);
- FX markup, owner markup percentage, and owner scope;
- per-section/line visibility policy;
- client-facing notes and separate internal notes;
- delivery channel, new/existing ticket, department, subject, notification
  choice, email template, and attachment checkboxes.

Show a side-by-side internal preview:

- client artifact;
- internal artifact;
- deterministic/AI narrative status;
- authoritative subtotal/tax/FX/owner/managed breakdown;
- source hash, catalogue freshness, warnings, and attachment manifest.

The UI must require explicit confirmation of:

- stale data;
- silent-included items;
- hidden owner margin;
- managed-tier review flags;
- ticket notification behavior;
- direct recipient resolved from the selected WHMCS client/contact;
- exact attachments and byte sizes.

Do not offer arbitrary recipient or From fields. Client/contact resolution must
come from WHMCS IDs and the selected delivery adapter.

## 9. WHMCS-native delivery

### 9.1 Support tickets

Use localAPI with the documented WHMCS commands:

- OpenTicket for a new ticket;
- AddTicketReply for an existing ticket.

Validate that the client/contact owns the selected service/ticket and that the
department is valid. Do not insert directly into tbltickets or ticket
attachment tables.

Use a plain-text or narrowly supported Markdown ticket body. HTML and JSON
artifacts are attachments, not arbitrary HTML inserted into the ticket body.
Use the documented attachment shape containing a safe name and base64 data.
Honor the administrator's explicit noemail choice; on WHMCS versions where a
parameter is unavailable, show the compatibility warning and use the supported
behavior rather than silently assuming notification suppression.

Attach only generated, hashed assets with an allowlist such as:

- proposal.html;
- proposal.txt;
- proposal.json.

The MVP should cap attachments at two or three files, two MB per file, and five
MB total. Enforce the cap before base64 encoding to avoid memory surprises.

### 9.2 Direct email

Seed one named WHMCS email template idempotently through the existing
EmailTemplateSeeder pattern, preserving admin edits. The template must provide
HTML and plain-text bodies, a proposal subject variable, and an escaped
proposal-body merge variable.

For a body without dynamic attachments, use the documented localAPI SendEmail
path with the client ID and custom variables. It must resolve a WHMCS client or
contact, not accept an arbitrary address.

Because SendEmail does not document a dynamic attachment field, implement and
test a versioned WhmcsAttachmentAdapter. The preferred path is:

1. construct the named WHMCS template message;
2. stage generated bytes through WHMCS email attachment storage using a
   collision-safe storage key;
3. pass the attachment manifest through the WHMCS mail/message or queue
   mechanism used by that installed version;
4. queue/send through WHMCS;
5. remove temporary storage only after a confirmed queue/send result, while
   retaining proposal-owned artifact metadata for audit/retry.

The adapter may use existing WHMCS internal mail/storage classes only behind
the compatibility boundary. It must not hard-code a storage filesystem path or
replace WHMCS mail transport. If the attachment path differs between 8.x and
9.x, implement separate adapters selected by detected WHMCS capabilities and
cover both with fixtures. If no safe attachment path can be proven on a target
version, fail closed with an administrator-facing error; do not fall back to
raw mail.

Email HTML must be self-contained, escaped, sanitized, and free of scripts,
Smarty tags, remote tracking pixels, arbitrary forms, or untrusted links.
Generate a plain-text alternative. Safe filenames must not contain CR/LF,
slashes, user-controlled paths, or secrets.

### 9.3 Retry and duplicate control

Compute an idempotency key from proposal public ID, immutable version, channel,
recipient entity, ticket ID if applicable, and attachment manifest hash.
Persist it with a unique index before calling WHMCS. Repeated clicks return the
existing result instead of creating another ticket/email.

If WHMCS returns an ambiguous timeout after a ticket may have been created,
mark the attempt for reconciliation instead of blindly retrying. Offer an
administrator retry only after showing the last known result. Email queue
retries must reference the same artifact/version and must not regenerate AI.

Record ticket ID, message ID if exposed, queue ID if exposed, and sanitized
WHMCS error text. Never log full email bodies or attachment bytes.

## 10. Security and abuse controls

Implement all of the following before enabling delivery:

- addon role access plus action-level capabilities:
  proposal.view, proposal.generate, proposal.edit, proposal.approve,
  proposal.send, proposal.configure_ai, proposal.purge;
- CSRF validation on every state-changing admin action;
- server-side client, contact, ticket, department, service, catalogue, and
  option validation;
- HTTPS-only AI endpoints, with explicit loopback/local exceptions for
  development only;
- hostname allowlist and private/link-local IP blocking after DNS resolution
  to prevent SSRF;
- endpoint, model, prompt, response, filename, body, and attachment size caps;
- encrypted API keys with rotation and redacted settings display;
- no API key in URLs, logs, snapshots, prompt text, or error pages;
- prompt-injection defense by treating report labels, client notes, and model
  input as data;
- strict JSON schema validation and allowlist HTML rendering;
- HTML escaping in Smarty and PHP, including client names and notes;
- no arbitrary recipient, headers, From, Reply-To, or attachment path fields;
- MIME sniffing and extension allowlists;
- SHA-256 asset hashes and cleanup of temporary attachment storage;
- budget checks before model calls and no unbounded retry loop;
- audit records with actor/correlation IDs but without secrets or full PII;
- retention and purge controls documented to the operator.

Generated proposals should exclude checkout passwords and the requested
Contabo checkout test values unless the owner explicitly turns a non-secret
configuration fact on. The Asia (India), one-month, paid-storage, Auto Backup,
and Private Networking combination remains a scraper/quote validation fixture,
not a credential or client proposal default.

## 11. Implementation phases and review gates

### Phase 0 — compatibility spike and contract freeze

Scope:

- verify localAPI OpenTicket/AddTicketReply/SendEmail behavior;
- verify ticket attachment encoding;
- verify named-template custom variables;
- verify email attachment queue/storage behavior on WHMCS 8.12.1 and 9.x;
- resolve the PHP-floor/documentation contradiction;
- add schemas and golden fixtures;
- decide whether managed-services data is served by an API endpoint or a
  packaged hashed catalogue.

Gate:

- a fixture can create a test ticket and a test email artifact without touching
  a real client;
- attachment cleanup and duplicate detection are demonstrated;
- no proposal implementation proceeds if direct email attachments require an
  unsafe undocumented path.

### Phase 1 — PHP proposal domain and persistence

Scope:

- migration v9;
- repositories, normalization, validator, calculator, visibility policy,
  renderer, hashes, immutable versions, and status machine;
- report snapshot import and current-catalogue creation;
- Founder Managed catalog synchronization;
- deterministic proposal preview and downloadable artifacts.

Gate:

- JS/Rust/PHP golden fixtures agree on totals, line IDs, visibility, aliases,
  managed tiers, and hashes;
- invalid/stale imports cannot be approved;
- deterministic generation works with AI disabled.

### Phase 2 — AI adapter and review UI

Scope:

- settings and encrypted key handling;
- Responses/Chat Completions adapters;
- strict narrative schema and safe merge;
- model override, cost budgets, caching, timeouts, and fallback;
- generate/edit/approve UI and audit events.

Gate:

- mocked success, 401, 403, 429, 5xx, timeout, malformed JSON, schema
  violation, unsafe narrative, oversized response, and over-budget fixtures
  all produce the expected safe state;
- the model cannot alter a price or hidden visibility state;
- default and manual model override are both tested.

### Phase 3 — native ticket and email delivery

Scope:

- ticket adapter;
- email template seeder;
- version-specific email attachment adapter;
- storage cleanup, manifest hashes, idempotency, retry/reconciliation;
- delivery preview and test-send UI.

Gate:

- new ticket, existing ticket reply, inline email, and email with attachments
  pass in both target WHMCS versions using test entities;
- duplicate submit does not duplicate delivery;
- a failed or ambiguous request is recoverable without sending the wrong
  version.

### Phase 4 — report bridge and operational hardening

Scope:

- report JSON export/import documentation and UI affordance;
- optional managed catalogue API;
- report/API/module contract versioning;
- role matrix, retention cron, health diagnostics, operator docs, and upgrade
  runbook;
- CI matrix and deployment packaging.

Gate:

- a report-created snapshot round-trips through WHMCS and produces the same
  client artifact;
- taxonomy aliases and storage policies remain intact;
- fresh install and v8-to-v9 additive upgrade pass;
- production deployment is still explicitly separate from this implementation.

## 12. Test matrix

### PHP/unit

Add PHPUnit coverage for:

- migration idempotence and v8-to-v9 upgrade;
- settings validation and API-key encryption marker;
- OpenAI URL/host/SSRF/model/timeout validation;
- Responses and Chat Completions request/response fixtures;
- strict schema, forbidden content, length, and HTML sanitizer behavior;
- deterministic fallback and cache key;
- decimal/minor-unit money calculations and rounding;
- FX/tax/owner/managed formulas and markup scope;
- all visibility policies and contradictory policy rejection;
- Core SSD/Performance NVMe/Max Performance alias validation;
- Founder Managed tiers and review flags;
- import stale-hash behavior;
- attachment size, MIME, filename, hash, and cleanup;
- idempotency and status transitions.

### WHMCS integration

Using local test entities and disabled/test mail transport:

- activate fresh and migrate v8 to v9;
- addon role visibility and action authorization;
- create/import, preview, generate, edit, approve;
- OpenTicket with and without attachments;
- AddTicketReply to an existing ticket;
- SendEmail with a named template and custom variables;
- direct email with HTML/text/JSON attachments in both compatibility adapters;
- client/contact resolution and invalid ownership rejection;
- notification/noemail behavior supported by each WHMCS version;
- timeout/ambiguous result/retry behavior;
- purge retention without touching unrelated WHMCS files or tables.

### Existing project gates

Run, without weakening existing gates:

- composer lint and PHPUnit for the addon;
- npm test for proposal/report JavaScript;
- cargo test --all-targets;
- cargo build --release;
- generated report and API smoke tests;
- direct-binary route checks;
- schema and JS/Rust/PHP parity fixtures.

Never test delivery by sending to a real client, production support queue, or
the live WHMCS public surface. Follow the existing testing scope, which permits
only the addon/standalone AJAX surface when a live smoke test is later
authorized.

## 13. Documentation and delivery workflow

Update, after implementation:

- whmcs-module/modules/addons/contabo_pricing/README.md with the real PHP and
  WHMCS support matrix;
- addon settings/admin guide for AI, markup, visibility, approval, and
  delivery;
- docs/PROPOSAL-GENERATION.md with the report-to-WHMCS import contract;
- a WHMCS proposal delivery runbook covering mail attachments, retention,
  retries, and incident recovery;
- schema/version notes and migration instructions;
- a privacy/security note explaining what is and is not sent to the model.

Use logical cherry-pickable commits:

1. contract schemas and golden fixtures;
2. module migration/domain/calculator;
3. AI adapter/settings/validation;
4. UI/review/approval;
5. ticket/email delivery and idempotency;
6. docs, CI, and packaging.

Branch before implementation, commit locally, run all gates, push the feature
branch, and open/update a PR into the repository's configured base. Do not
merge without an explicit operator instruction and green checks. Keep this
plan-only amendment separate from source implementation if execution is
approved later.

## 14. Stop conditions

Stop and report instead of improvising when:

- WHMCS 8.x/9.x API or mail behavior differs from the compatibility fixture;
- dynamic email attachments cannot be delivered through a safe WHMCS-owned
  path;
- the configured AI endpoint is non-HTTPS or fails SSRF/allowlist checks;
- the provider cannot return a locally validated structured narrative;
- the model attempts to return commercial facts or unsafe markup;
- source catalogue/hash is stale and cannot be refreshed;
- money parity differs across JS, Rust, and PHP;
- a migration is not additive/idempotent;
- a ticket/email call times out after an ambiguous external result;
- a test would contact a real client or modify production data;
- a requested feature would become provisioning, billing, checkout, or
  repricing work rather than proposal generation.

## 15. Done-when checklist

This plan is complete only when:

- a deterministic proposal is generated and delivered with AI disabled;
- the configured default cost-efficient model and a manual override both work;
- AI failures fall back safely and budget limits are enforced;
- no AI output can modify facts, prices, markup, visibility, or attachments;
- report JSON imports and round-trips with server-side recalculation;
- provider, managed-service, tax, FX, FX-markup, owner-markup, silent-include,
  internal-only, exclude, and comparison semantics are covered by fixtures;
- Core VPS/SSD, Performance VPS/NVMe, and Max Performance/former-VDS aliases
  remain correct;
- new/existing support tickets and direct email work through WHMCS mechanisms
  on the supported WHMCS 8 and 9 matrix;
- HTML, plain text, and JSON attachments are safe, bounded, hashed, and
  retryable;
- duplicate send protection, audit, retention, and failure recovery work;
- fresh install and additive upgrade pass;
- all project tests/lints/builds pass;
- documentation, security posture, and operator runbook are updated;
- the worktree is clean and the PR is reviewable.
