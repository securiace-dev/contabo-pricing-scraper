# Upgrade runbook: add-on 1.1.5

Applies to an installation on 1.1.4. No schema change (`Installer::SCHEMA_VERSION` stays 16).
No new setting. Do not deploy without an explicit go.

## What it fixes

1. **Re-quote used the current mapping.** "Re-quote a paid order" (1.1.4) rebuilt the snapshot
   from the CURRENT published mapping, so after a republish the SKU, region, image and renewal
   could differ from what the customer was quoted.
2. **A different paying invoice stranded the order.** If the customer paid a Mass Pay, merged
   or re-issued invoice instead of the order's own, `OrderPaid` did not seal, the gate saw no
   Paid own invoice, and re-quote refused.

## Steps

1. Back up `mod_securiacevps_order_snapshots` and keep a hashed copy of the previous add-on tree.
2. Copy the 1.1.5 files and open the add-on admin page once (records the version and the new
   `InvoicePaid` hook is loaded from `hooks.php`; no WHMCS hook cache needs clearing beyond
   the usual).
3. Open **Maintenance**, panel **Re-quote a paid order** and read the Status column of every row.

## Maintenance panel statuses

| Status | Meaning | Action |
|---|---|---|
| Ready (recorded mapping `mapping-vN`) | The draft's own mapping publication is available and agrees with the row | **Re-quote** |
| Needs confirmation | The publication is gone; the verified draft's own SKU, region, image and renewal can be reused, but the SKU is not hashed and nothing corroborates it | **Review**: check the SKU, Region, Image, Renewal shown, then **Confirm: seal the recorded quote**. Logged as a WARNING with your admin id |
| Needs override | Nothing verifiable was recorded (tampered or missing earlier row, or the recorded mapping failed its hash or disagrees with the row) | **Review**: compare old and new SKU, Region, Image, Renewal, then **Confirm override with the current mapping**. Logged with your admin id and the diff |
| Refused | A Paid invoice exists but is not linked to this order or belongs to another client | Fix the invoice in WHMCS. Cannot be overridden here |

A service with no Paid linked invoice is not listed. The Paid invoice column shows which
invoice settled the order and how it links (`order`, `mass_pay`, `merged`, `reinvoice`).

## Which invoices settle an order

| Link | Condition | WHMCS source |
|---|---|---|
| `order` | the draft's (else the order's) own invoice is Paid | `tblorders.invoiceid` |
| `mass_pay` | Paid invoice has an item type `Invoice` with `relid` = the order's invoice | Mass Pay creates one combined invoice of such items |
| `merged` | Paid invoice has an item type `Hosting` or `Setup` with `relid` = the service id, and the draft's invoice no longer exists | Merge Invoices moves line items into the higher invoice and deletes the other |
| `reinvoice` | same item, and the draft's invoice still exists | a new invoice raised for the same service |

Every link requires the invoice to belong to the service owner; the order's own invoice may also belong to the client who placed the order (WHMCS Move Product/Service changes `tblhosting.userid` only). The check was
designed from the WHMCS documentation and tested against a fake database; it has not been run
against a live WHMCS. Verify on staging (below) before relying on it.

## Behaviour

| Situation | Result |
|---|---|
| Re-quote, draft verifies, recorded mapping available | rebuilt from that mapping version, even after a republish |
| Re-quote, publication gone, draft verifies | needs an explicit logged confirmation, then sealed from the draft's own recorded fields |
| Recorded publication fails its payload hash, or disagrees with the row | not trusted; override onto the current mapping only |
| Re-quote, nothing verifiable recorded | refused (`needs_override`); explicit override shows and logs the diff |
| Override token missing, wrong or stale | refused (`diff_changed`) |
| Mass Pay / merged / re-issued invoice Paid, draft verifies | sealed at the quoted terms, `WARNING: ... sealed via a linked invoice` logged |
| Paid invoice not linked, or another client's | not sealed, reason logged and listed, not overridable |
| Draft or provider copy tampered | marked `invalid`, `REFUSED to seal`, same as 1.1.4 |
| Original invoice cancelled while a replacement invoice is live | draft kept so the replacement can seal |

## Verify on staging

1. Mass Pay: raise two orders, Mass Pay them, pay the combined invoice, confirm both rows are
   `sealed` with a `mass_pay` warning line and Create is not aborted.
2. Merge two unpaid invoices, pay the survivor, confirm `merged` (or `order` if WHMCS repoints
   `tblorders.invoiceid`) and Create works.
3. Republish the mapping, invalidate a draft by hand, mark its invoice Paid, and check that the
   panel says Ready (recorded mapping) and the sealed payload keeps the old SKU.

## Known limit

`provider.sku_id` is not covered by a snapshot hash (a schema change would be needed). The
recorded-mapping source cross-checks it against the draft's hash-verified mapping publication,
which must belong to the same product and management code and have been in effect when the draft
was created (`effective_at` and the `supersedes_mapping_version` chain). The renewal amount is
cross-checked only when the publication payload records `renewal_amount` (current publications
may not). Someone who can edit both the draft row and `mapping_version` AND the publication
timeline is outside this protection. When that publication is gone, the row's own value is used only after the admin confirms the
exact SKU, Region, Image and Renewal shown.

## Rollback

Restore the previous add-on tree. Snapshots sealed under 1.1.5 are ordinary sealed rows.
