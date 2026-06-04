---
type: "query"
date: "2026-06-04T06:31:49.772822+00:00"
question: "What connects catalog_cycles_mask to published_cycles_mask?"
contributor: "graphify"
source_nodes: ["catalog_cycles_mask", "published_cycles_mask", "Mapping", "Profile", "SyncEngine", "CycleSet", "renewal_cycles_mask"]
---

# Q: What connects catalog_cycles_mask to published_cycles_mask?

## Answer

No direct code link — only an INFERRED semantically_similar_to edge (both are 6-bit CycleSet bitmasks). The real connection is structural via the Profile(SOURCE)->Mapping(CUSTOMER) two-layer split: published_cycles_mask lives on the Profile/profile_version as the offered superset (default 63 = all six cycles); catalog_cycles_mask lives on the Mapping as the customer-facing subset that SyncEngine writes to tblpricing. So catalog is a subset of published — the Mapping narrows the Profile's published superset down to what reaches checkout. A third sibling, renewal_cycles_mask, gates RenewalEngine writes to tblhosting. The three masks (published->catalog->renewal) implement the catalog/renewal separation, all over the same CycleSet 6-bit bitmask.

## Source Nodes

- catalog_cycles_mask
- published_cycles_mask
- Mapping
- Profile
- SyncEngine
- CycleSet
- renewal_cycles_mask