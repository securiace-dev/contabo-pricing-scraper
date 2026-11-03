# Upgrade runbook: add-on 1.1.4

Applies to an installation on 1.1.3. No schema change (`Installer::SCHEMA_VERSION` stays 16).
Do not deploy without an explicit go.

## What it fixes

Under 1.1.3 a checkout draft expired after 1800 s. If the invoice was paid later (bank
transfer, pay on the due date) `sealOrder` marked the draft `invalid`, `ProvisioningGate`
refused Create, and nothing ever made a new draft, so a paying customer was stranded.

## Steps

1. Back up `mod_securiacevps_order_snapshots` and keep a hashed copy of the previous add-on tree.
2. Copy the 1.1.4 files and open the add-on admin page once (records the version).
3. Optional: set `mod_contabo_settings.snapshot.quote_ttl_seconds` (seconds, 60 to 2592000,
   default 1800). It is how long an unpaid checkout counts as on time. The effective window is
   the later of this and the end of the invoice due date. It only changes the late-payment
   label; a late payment still seals.
4. Open **Maintenance**, panel **Re-quote a paid order**. Any paid service listed there was
   invalidated or left unsealed under 1.1.3. Press **Re-quote** for each, then run Create on the
   service again. You can also enter a service ID by hand.
5. Verify (staging first): create an order with a bank-transfer-style invoice, let
   `quote_expires_at` pass (set it in the past), mark the invoice paid, and confirm the row is
   `sealed`, the Activity Log shows `Contabo late payment sealed`, and Create is not aborted.

## Behaviour

| Situation | Result |
|---|---|
| Invoice paid within the quote window | sealed (unchanged) |
| Invoice paid after the quote window or due date | sealed at the quoted terms, `WARNING` logged |
| Quoted amounts differ from the invoice or service at seal time | sealed at the quoted amounts, drift logged |
| Draft hashes or order/invoice/service binding do not verify | marked `invalid`, `REFUSED to seal` logged, Create aborted |
| Invoice unpaid, or order Fraud/Cancelled | not sealed, Create aborted |
| Invoice cancelled | drafts marked `invalid`; paying later needs the admin re-quote |
| OrderPaid did not seal, invoice Paid, draft verifies | gate seals it, `sealed_at_gate_paid_invoice` `WARNING`, Create allowed |
| Existing `invalid` or tampered row, invoice Paid | admin re-quote on Maintenance |

## Recover a stuck order by hand

Maintenance, Re-quote a paid order, press Re-quote on the service (or enter its ID). The new
snapshot uses what WHMCS billed on the paid invoice and the currently published mapping. If the
mapping was changed since checkout, the Activity Log line names the new mapping version; check
it before running Create. (Superseded by 1.1.5: re-quote now uses the draft's own recorded
mapping or snapshot, and the current mapping only through a logged override. See
`UPGRADE_1_1_5.md`.) Re-quote refuses an unpaid invoice and a Fraud or Cancelled order.

## Rollback

Restore the previous add-on tree. Snapshots sealed under 1.1.4 are ordinary sealed rows.
