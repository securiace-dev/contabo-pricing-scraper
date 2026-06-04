---
type: "query"
date: "2026-06-04T06:34:39.247308+00:00"
question: "How does the EUR to local conversion (toLocalMonthly) work and what depends on it?"
contributor: "graphify"
source_nodes: ["ProfileVersionInput::toLocalMonthly", "Pricing calc model (GST then FX conversion)", "TaxModeEngine", "POST /api/v1/quote", "GET /api/v1/fx", "MarginCalculator", "Rounding modes"]
---

# Q: How does the EUR to local conversion (toLocalMonthly) work and what depends on it?

## Answer

ProfileVersionInput::toLocalMonthly is the single EUR->local conversion site, order is GST then FX. The graph anchors three rationale_for notes on this node: 'GST on cost basis by owner decision', 'GST placement on cost basis kept as-is', and 'Shared cost basis keeps catalog and renewal consistent' (linking it to the per-cycle fallback rule). It exists in two languages: PHP ProfileVersionInput::toLocalMonthly (stored price on version save) and the Rust API POST /api/v1/quote + GET /api/v1/fx (live quote, FX source), with TaxModeEngine (8 modes) driving GST. Invariants pinned by tests: EUR currency short-circuits FX (testComputedEurPathIgnoresFxRateWhenCurrencyIsEur, quote_endpoint_without_gst); FX markup+source are stored for audit (testComputedStoresFxMarkupAndSource, mod_contabo_catalog_audit); changing currency or base EUR forces a new immutable profile_version (testDiffersFromReturnsTrueWhenCurrencyChanges/BaseEurChanges). Downstream the catalog path adds MarginCalculator then Rounding modes then tblpricing. The GST placement is an owner decision encoded in one function, not enforced elsewhere - moving it would silently change both engines.

## Source Nodes

- ProfileVersionInput::toLocalMonthly
- Pricing calc model (GST then FX conversion)
- TaxModeEngine
- POST /api/v1/quote
- GET /api/v1/fx
- MarginCalculator
- Rounding modes