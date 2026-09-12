# Four business decisions — source and QA notes

## Reporting job

- **Question:** What are the controlling operating answers for Founder time,
  tax/Sum 9, Object Storage tenancy, and cancellation/reassignment?
- **Audience:** Product stakeholders — founder and WHMCS operators.
- **Decision supported:** Encode the four answers in Proposal Studio,
  provisioning, billing, and inventory workflows without relying on operator
  memory.
- **Scope and time:** Securiace/Contabo evidence reviewed through August 5,
  2026.
- **Success criterion:** Each answer states its effective rule, conflicting or
  superseded evidence, implementation control, release gate, and caveat.

## Executive-report structure mapping

| Required role | Visible report section |
|---|---|
| Title | Four Business Decisions for Managed Infrastructure |
| Executive summary | Executive Summary |
| Findings and evidence | Decision-gate chart plus one or more sections per decision |
| Recommended next steps | Recommended Next Steps |
| Further questions | Further Questions |
| Caveats and assumptions | Caveats and Assumptions |

No required role was omitted or merged away.

## Evidence inventory

1. **Founder entitlement and overage**
   - `MANAGED-SERVICES-RUNBOOK.md:12` records the older automatic 1.5×
     rollover rule.
   - `MANAGED-SERVICES-RUNBOOK.md:105-106` records ₹2,500/hour separately
     billable overage.
   - `MANAGED-SERVICES-RUNBOOK.md:168-170` shows the older invoice example;
     `:216` confirms month-end budget reset.
   - `plans/005-canonical-whmcs-proposal-managed-policy.md:18-33` records the
     approved superseding rule: monthly expiry, discretionary dated and
     expiring non-contractual credit, written approval, and bounded emergency
     stabilization.

2. **Tax and Sum 9**
   - `PRICING-DECISIONS-FINAL.md:1-5` identifies the document as founder-approved;
     `:26-35` describes Sum 9 as a founder-personal framework rather than a
     tested conversion lever; `:50-58` records the managed tier prices.
   - `terms-of-service.draft.md:1` marks the newer launch terms as unpublished
     and awaiting owner/counsel review; `:37-39` says Securiace is not currently
     GST-registered and charges no separate GST; `:69-70` says prices are
     all-inclusive.
   - `plans/005-canonical-whmcs-proposal-managed-policy.md:35-49` reconciles the
     conflict by failing closed: output GST off until evidence and matching
     WHMCS configuration exist; provider tax remains separate landed-cost
     provenance.

3. **Object Storage tenancy and billing**
   - `plans/003-new-categories-and-scraper-resilience.md:103-125` records that
     provider cost follows purchased capacity, egress is free, management stats
     are not a sound per-customer billing basis, and WHMCS should bill committed
     capacity.
   - `plans/003-new-categories-and-scraper-resilience.md:127-129` identifies the
     pooled-versus-dedicated tenancy fork.
   - `plans/005-canonical-whmcs-proposal-managed-policy.md:51-59` resolves that
     fork: pooled regional resources by default, isolated per-service identity,
     namespace, policy, and quota, with a dedicated exception and canary gates.

4. **Cancellation and reassignment**
   - `legal/01-terms-of-service.md:80-93` states cancellation stops future
     renewals, paid fees are normally not refunded, and clients typically have
     14 days to export before active deletion with up to 30 days of backup
     retention.
   - `legal/05-refund-policy.md:35-41` says a paid renewal starts the service
     period and future-renewal cancellation becomes effective at the end of the
     current paid period.
   - `plans/005-canonical-whmcs-proposal-managed-policy.md:61-74` and `:412-429`
     define separate entitlement/provider/asset/inventory state, no reassignment
     before export and retention obligations end, quarantine, sanitization
     proof, and no dual lease.

## Chart map

- **Section:** The decisions are resolved; enabling gates remain.
- **Question:** How many explicit validations remain before each decision's
  exception or automation can be enabled?
- **Form:** Comparison / vertical bar chart.
- **Rows:** Four decisions; one reviewed row per decision.
- **Fields:** `decision`, `remaining_gates`, `gate_summary`,
  `operating_default`.
- **Takeaway:** Tax and Object Storage remain the most strongly gated; no
  decision itself is left undefined.
- **Palette:** Single-root preferred; no redundant series or legend.
- **Reproducibility:** `decision-readiness.sql` materializes the reviewed gate
  register used in `artifact.json`.

No additional quantitative visual is justified. The remaining evidence is
qualitative policy and conflicting-document interpretation, which is clearer as
answer-first narrative than as arbitrary scores.

## Delivery QA

The portable report builder returned:

- validation: passed;
- packaging: passed;
- browser verification: passed;
- viewports: 1440 px and 390 px;
- source dialog: passed through keyboard menu interaction;
- blocks: 16; charts: 1; tables: 0.

