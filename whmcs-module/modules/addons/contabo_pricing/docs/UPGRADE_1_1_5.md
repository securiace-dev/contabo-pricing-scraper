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
| Ready (recorded snapshot) | The publication is gone; the verified draft's own SKU, region, image and renewal are reused | **Re-quote** |
| Needs override | Nothing verifiable was recorded (tampered or missing earlier row, or the recorded mapping disagrees with it) | **Review diff**: compare old and new SKU, Region, Image, Renewal, then **Confirm override with the current mapping**. Logged with your admin id and the diff |
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

Every non-`order` link also requires the invoice to belong to the service owner. The check was
designed from the WHMCS documentation and tested against a fake database; it has not been run
against a live WHMCS. Verify on staging (below) before relying on it.

## Behaviour

| Situation | Result |
|---|---|
| Re-quote, draft verifies, recorded mapping available | rebuilt from that mapping version, even after a republish |
| Re-quote, publication gone, draft verifies | rebuilt from the draft's own recorded fields |
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
recorded-mapping source cross-checks it against the draft's mapping publication; when that
publication is gone, the recorded-snapshot source trusts the row's own value.

## Rollback

Restore the previous add-on tree. Snapshots sealed under 1.1.5 are ordinary sealed rows.
