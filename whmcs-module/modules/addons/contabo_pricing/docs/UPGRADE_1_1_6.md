# Upgrade runbook: add-on 1.1.6

Applies to an installation on 1.1.5. No schema change (`Installer::SCHEMA_VERSION` stays 16).
No new secret. One optional setting. Do not deploy without an explicit go.

## What it fixes (WHMCS-Prod review of 1.1.5)

1. **B1, payment bypass.** A Paid same-client invoice with a Hosting or Setup line for the service
   counted as a re-issued invoice while the order's own invoice still existed and was Unpaid, and
   the amount was never checked against the quote. The gate, the grandfather path and re-quote
   could provision without the order being paid.
2. **B2, re-quote fidelity.** Re-quote rebuilt options, setup and recurring from the live service
   records and tax and total from the bound invoice, while the diff and token covered only SKU,
   region, image and renewal, and the Ready path showed no diff.

## Linking rules now enforced

| Link | Accepted only when | Must cover |
|---|---|---|
| `order` | the draft's (else the order's) own invoice is Paid | the existing seal-time price-drift check, which only logs a WARNING |
| `mass_pay` | Paid invoice has `Invoice` items whose `relid` is the original invoice (bound or the order's) | those items sum to at least the quoted total due (`tblinvoices.total` is already net of credit; it is not subtracted again) |
| `reinvoice` | the original invoice is Cancelled | Hosting + Setup + promo lines for the service sum to at least the quoted first payment |
| `merged` | the original invoice no longer exists | same as `reinvoice` |

If the original invoice has any other status (Unpaid, Collections, Payment Pending and so on) the
link is refused ("still <status>"). A shortfall is refused and is not overridable. The invoice
owner must be the service owner (or the ordering client for the order's own invoice and for a
Mass Pay or merge made by the original owner).

**Assumption to check on staging:** WHMCS creates a hosting product's lines as `Hosting`, `Setup`
and a negative `PromoHosting` line, and their sum ex tax equals the service's first payment amount.
If the live data differs (for example a custom line type), a legitimately merged or re-issued
invoice will be refused rather than accepted: safe, but it blocks recovery until the rule is adjusted.

## Re-quote behaviour

| Situation | Result |
|---|---|
| Recorded mapping verifies, WHMCS matches the recorded quote | `Ready`: the panel shows the full diff, **Re-quote** seals exactly the recorded quote |
| Recorded mapping verifies, WHMCS has diverged (options, amounts, tax, total, discount, product, cycle, domain, currency, or invoice gone/unreadable) | `Needs confirmation` (`recorded_deviation`): deviations listed, recorded quote is what is sealed, confirmation logged as a WARNING |
| Publication gone | `Needs confirmation` (`recorded_snapshot`), as 1.1.5 |
| Nothing verifiable recorded | `Needs override` (`current_mapping`), as 1.1.5 |

A confirmation needs a permitted role (below) and a token that matches the diff exactly. A change in
the service, the invoices, the draft row, the mapping or WHMCS values after the diff was shown
refuses with `diff_changed`. If the invoice total itself has drifted from the quote, the Total
due row shows it as a deviation (credit applied after checkout moves money from total to credit
and is not a deviation; the gross is compared). The seal-time price-drift check only logs a
WARNING, so the admin's confirmation is the control. A renamed option label also shows as a
deviation; this is conservative (a confirmation, never a silent seal).

Credit applied to the original invoice before a Mass Pay, so that the Mass Pay's `Invoice` items
fall short of the quoted total due, is refused (safe); the order stays on the Maintenance list.

## Confirm token and key

The token is an HMAC-SHA256 of the canonical diff context. Its key is derived from WHMCS's own
`$cc_encryption_hash` (from `configuration.php`), namespaced to this feature and installation. No
secret is stored in the repository, the database or this add-on. If the value is unavailable the
confirmation is refused (`token_unavailable`); the Ready path does not need it. Rotating
`$cc_encryption_hash` would also break WHMCS stored cards, so it is unlikely; if it does change,
only confirmation forms that are already open stop working.

## Who may confirm

Default: admin role id 1 (Full Administrator). The admin must exist and not be disabled. To allow
other roles add a row to `mod_contabo_settings`: key `snapshot.requote_role_ids`, value a
comma-separated list of role ids (for example `1,2`); the list replaces the default. A refused
admin sees no confirm button, and a direct POST is refused (`not_permitted`, logged).
The plain Ready re-quote is not gated (it seals only the verified recorded quote).

## Steps

1. Back up `mod_securiacevps_order_snapshots` and keep a hashed copy of the previous tree.
2. Copy the 1.1.6 files and open the add-on admin page once.
3. Maintenance, **Re-quote a paid order**: **Review** each row and read the diff.

## Verify on staging

1. Raise an order, leave its invoice Unpaid, pay a second invoice that has a Hosting line for the
   service: Create must still be aborted and the row listed as refused.
2. Mass Pay two orders and pay it: both seal. Remove or reduce an `Invoice` line: refused.
3. Merge invoices and pay the survivor: seals only if the Hosting + Setup lines cover the quote.
4. Edit the live service recurring after checkout, then Review a re-quote: the deviation is listed
   and a non-Full-Administrator sees no confirm button.

## Known limits

- `provider.sku_id` is still not covered by a snapshot hash (it would need a schema change); it is
  cross-checked against the verified mapping publication as in 1.1.5.
- The `InvoicePaid` hook is unchanged and still runs on every paid invoice site-wide; it costs one
  indexed items query and one indexed orders query and returns at once for other invoices.
- The invoice link rules were designed from the WHMCS documentation and tested against a fake
  database. Run the staging checks first.

## Rollback

Restore the previous add-on tree. Snapshots sealed under 1.1.6 are ordinary sealed rows.
