---
type: "query"
date: "2026-06-04T06:36:03.529704+00:00"
question: "How is the catalog/renewal write boundary (tblpricing vs tblhosting) structured and enforced?"
contributor: "graphify"
source_nodes: ["Catalog (tblpricing) and renewal (tblhosting) kept separate", "SyncEngine", "ServicePriceWriter", "WhmcsConfigOptionsAdapter", "tblpricing", "tblhosting", "ServiceRevenueResolver"]
---

# Q: How is the catalog/renewal write boundary (tblpricing vs tblhosting) structured and enforced?

## Answer

Design node 'Catalog (tblpricing) and renewal (tblhosting) kept separate' has rationale_for edges to both SyncEngine and RenewalEngine. The graph found a repeated one-chokepoint-per-table-family pattern (ServicePriceWriter semantically_similar_to WhmcsConfigOptionsAdapter): SyncEngine is sole writer of tblpricing (catalog/new orders), ServicePriceWriter is sole writer of tblhosting.recurringamount (renewal/existing), WhmcsConfigOptionsAdapter is sole writer of tblproduct*. Enforcement is a static-grep gate test (testStaticGrepGateHeaderPresent) that fails the suite if writes happen outside the chokepoint - this boundary is hard-guarded, unlike the fallback rule and GST placement which are convention-only. ServicePriceWriter write ladder: updateRecurringAmount -> writeViaLocalApiOrFallback (WHMCS localAPI) -> rawUpdate (direct column) -> logFallback; gated by repricing_phase (observe/notify/enforce), Approval queue, mod_contabo_service_policy, and testDisabledInPhaseAWritesNothing. Reads also distrust recurringamount: ServiceRevenueResolver computes true revenue (recurringamount is a drifting snapshot). The split exists for outage-safety: catalog path calls the Rust API for FX/prices, renewal path makes no API call (reads immutable profile_version + ServiceConfigSnapshot), so existing customers keep billing if the API is down.

## Source Nodes

- Catalog (tblpricing) and renewal (tblhosting) kept separate
- SyncEngine
- ServicePriceWriter
- WhmcsConfigOptionsAdapter
- tblpricing
- tblhosting
- ServiceRevenueResolver