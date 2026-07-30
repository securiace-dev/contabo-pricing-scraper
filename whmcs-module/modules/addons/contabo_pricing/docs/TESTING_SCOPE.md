# SecuriAce VPS test-surface policy

This repository's automated and manual verification targets local fixtures,
containerized development WHMCS installations, or an explicitly designated
staging installation. It does not authorize production access or mutation.

## Default permitted surfaces

- PHPUnit/FakeCapsule unit and contract suites.
- Rust unit, integration, formatting, and Clippy checks.
- PHP syntax lint on supported versions.
- Local package build and archive inspection.
- Containerized WHMCS 8.13.x and 9.x schema/integration smoke tests.
- Staging catalog import, checkout, provisioning, reconciliation, and lifecycle
  tests using staging-only credentials and provider resources.
- Sanitized, read-only fixture imports that contain no production credentials,
  session tokens, one-time secrets, or personal data.

## Production is out of scope by default

No test or release command in this repository may:

- connect to the production WHMCS host;
- reuse production provider or Rust API credentials;
- invoke production WHMCS pages, APIs, hooks, or cron;
- write production WHMCS or provider data;
- create, alter, suspend, or delete a production VPS;
- copy production cookies, database dumps, logs, or secrets into the repository.

A production inspection or rollout is a separate operator-controlled task. It
requires explicit current-task authorization, an exact target and read/write
scope, a backup/recovery plan, a provider-write switch state, and a reviewed
runbook. General implementation approval is not production authorization.

## Environment separation

Development, staging, and production must use separate:

- WHMCS installations and installation identities;
- provider accounts or credentials;
- Rust API credentials and data sources;
- resource naming/tag prefixes;
- catalog publications and mapping states;
- feature flags and write permissions;
- payment gateways or gateway modes;
- mail destinations and templates.

Staging callbacks must not be able to address a production provider account.
Tests assert environment identity as part of deterministic command IDs.

## Destructive test rules

Provider-write tests run only against allowlisted staging accounts, products,
services, and regions. Each run needs a unique correlation prefix and a cleanup
inventory. A timeout or partial cleanup creates a reconciliation finding; test
code must not blindly repeat create or delete.

## Data and secret handling

- Use generated customers, services, invoices, and credentials.
- Redact nested provider payloads and verify redaction in tests.
- Never record plaintext root passwords, access tokens, OAuth secrets, cookies,
  payment identifiers, or personal contact data.
- One-time secret tests use deterministic fake ciphertext and never real keys.
- Screenshots and artifacts must use seeded, synthetic records.

## Required evidence

The release evidence pack contains:

1. unit/contract test results and assertion counts;
2. PHP version lint matrix;
3. WHMCS 8.13.x and 9.x integration results or a clearly recorded unavailable
   environment;
4. Rust format, test, check, and Clippy results;
5. package manifest and checksum;
6. migration install/upgrade/idempotency/rollback evidence;
7. concurrency, unknown-outcome, ownership, billing, and redaction tests;
8. accessibility and Hallmark 58-gate review;
9. GitHub CI status for the review branch;
10. an explicit statement that production was not touched.

Missing environment access is reported as unverified, never inferred as green.
