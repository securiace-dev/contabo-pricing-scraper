---
type: "query"
date: "2026-06-04T06:33:28.389320+00:00"
question: "How does the per-cycle source fallback rule connect catalog and renewal pricing?"
contributor: "graphify"
source_nodes: ["Per-cycle source fallback rule", "SyncEngine", "RenewalEngine", "period_prices_json", "ProfileVersionInput", "ServicePriceWriter", "sourceRateForMonths"]
---

# Q: How does the per-cycle source fallback rule connect catalog and renewal pricing?

## Answer

The fallback rule (Source(M) = effective_monthly of the longest scraped period with months <= M) is one invariant implemented at three mirrored sites the graph linked via conceptually_related_to edges despite no shared call graph: SyncEngine::nearestSourceRate/periodPriceVectorFromPlan/resolveCycleSourceEur (catalog -> tblpricing), RenewalEngine::resolveCycleEurMonthly (renewal -> tblhosting via ServicePriceWriter), and assets/app.js sourceRateForMonths (admin preview). All three read the same period_prices_json per-cycle EUR vector and run the same ProfileVersionInput::toLocalMonthly EUR->local conversion (GST then FX). So catalog and renewal share one cost-basis spine and diverge only at destination table and cycle gate. When period_prices_json is NULL (legacy version) both paths degrade to the single finalMonthly fallback (testLegacyVersionWithoutVectorFallsBackToFinalMonthly). This is the 'one rule, everywhere' / 'change one engine's basis, change the other' invariant.

## Source Nodes

- Per-cycle source fallback rule
- SyncEngine
- RenewalEngine
- period_prices_json
- ProfileVersionInput
- ServicePriceWriter
- sourceRateForMonths