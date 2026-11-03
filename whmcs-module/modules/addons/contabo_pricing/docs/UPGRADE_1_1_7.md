# Upgrade runbook: add-on 1.1.7

Applies to an installation on 1.1.6. No schema change (`Installer::SCHEMA_VERSION` stays 16). No new
secret. No new setting. Do not deploy without an explicit go.

## What it fixes (WHMCS-Prod review of 1.1.6)

### Blockers

1. **Product id on the recorded path.** The replay used the LIVE `tblhosting.packageid` for the
   sealed row. A service moved to another native product would have sealed the wrong product id
   after an admin confirmed. **Decision: refuse.** A recorded SKU, region and image are valid only
   for the product they were recorded on; sealing the live id under them would match neither the
   customer's quote nor the other product's mapping. `requotePlan` returns `product_changed`
   (not overridable, not confirmable) when the recorded payload product, the draft row's product
   column and the live product are not all the same, on the recorded-mapping and the
   recorded-snapshot paths. `createDraft` throws the same way as defence in depth. Recovery: move
   the service back in WHMCS (Move Product/Service), or place a new order.
2. **PromoHosting pinned.** `Hosting`, `Setup` and `PromoHosting` are the counted service line
   types. New tests: Hosting 1999.00 plus PromoHosting -500.00 on a re-issue is a shortfall
   (without PromoHosting counted it would wrongly pass); a promo line alone never makes a
   re-issue. Both were mutation-checked (removing `PromoHosting` from the counted types fails the
   first, treating a promo line as a service line fails the second).

### Findings

| Finding | 1.1.7 behaviour |
|---|---|
| Manual "Re-quote service" box sealed a Ready plan blind | Every seal needs the shown diff, `override=1`, a permitted role and the keyed token. A Ready plan now carries `override_kind=recorded_mapping` and a token; `requote()` without them returns `needs_override` and seals nothing. The box is "Review re-quote" and only opens the diff (the controller redirects `needs_override`, `diff_changed` and `not_permitted` to the preview). An admin without a permitted role sees no confirm button. Confirmed Ready seals are logged ("re-quote CONFIRMED by admin ..."). |
| Malformed `snapshot.requote_role_ids` locked out every admin | Strict parse: a list with any non-digit, zero or negative token, or an empty list, falls back to `[1]` and logs `WARNING: Contabo setting snapshot.requote_role_ids is malformed ...`. A valid value (`3`, `1,3`, spaces allowed) behaves as before. |
| No-draft path compared gross against the net Mass Pay line | Net against net. `tblinvoices.total` is already net of credit and a Mass Pay line equals the original's balance, so the draftless terms use the net total. The draft path already used the recorded net total due. Gross against gross stays only in the diff's Total due row. |
| Currency not compared | A linked (non-order) invoice must belong to a client whose currency code equals the quoted currency; a mismatch or an unknown currency refuses. The order's own invoice is the quote itself and is not checked. |
| Tax-exempt re-issue settled a taxed quote | See "Currency and tax-exempt decision". |
| "WHMCS now" read the cancelled original | The replay branches read the invoice that paid (re-issue, merge) or, for Mass Pay, the Mass Pay line for the original (with the recorded credit, so the gross comparison reduces to line against recorded total). The current-mapping override still reads the bound invoice because that is what it would seal. |
| No proof that the key resolves | Maintenance shows "Confirmation key check: key resolves: yes/NO, source: ...". The key is never output. |

## Currency and tax-exempt decision

- **Currency mismatch refuses** (not overridable): WHMCS does not convert a client's currency, so
  an invoice in another currency cannot have paid the quoted amount. An unknown currency is
  treated as a mismatch.
- **Gross coverage on a merge or re-issue.** The invoice's gross (total plus credit applied) must
  be at least the quoted gross (recorded total due plus recorded credit). The line coverage
  (Hosting, Setup, PromoHosting at least the quoted first payment) stays as in 1.1.6. A gross
  below the quote means tax or a discount differs (for example a tax-exempt re-issue of a taxed
  quote):
  - **Automatic paths** (InvoicePaid and OrderPaid hooks, cron, the provisioning gate) refuse and
    log "totals X but the quote was Y ... Re-quote service #N from Maintenance". The order stays on
    the Maintenance list.
  - **Admin re-quote** links the invoice anyway but the diff's Total due row shows recorded
    against WHMCS now, the plan becomes `recorded_deviation`, and the admin must confirm. The
    sealed contract is still the recorded quote (the customer was quoted that total). Without a
    verified recorded quote (`current_mapping`) nothing would show the difference, so that path
    stays strict.
- A higher gross (a merge with other services) is not a shortfall.

## Key diagnostic

Maintenance, "Confirmation key check". Yes means the add-on can sign confirmations; the source is
one of `php global $cc_encryption_hash`, `WHMCS application config (DI config, array access)`,
`WHMCS application config (DI config, property)`. NO means confirmations (including the Ready
path) are refused with `token_unavailable`. It prints no secret, only the yes/no and the source.

## Steps

1. Back up `mod_securiacevps_order_snapshots` and keep a hashed copy of the previous tree.
2. Copy the 1.1.7 files and open the add-on admin page once.
3. Maintenance: read the **Confirmation key check**. It must say yes.

## Verify on staging (WHMCS 8.13.7)

1. Confirmation key check says `key resolves: yes` and names the source. If NO, stop: nothing can
   be confirmed and no order can be re-quoted until the key is readable.
2. Re-quote Review of a Ready service now shows **Confirm: seal exactly this quote**; the manual
   box says **Review re-quote** and seals nothing by itself.
3. Move a stuck service to another native product and Review it: `product_changed`, no confirm.
4. Set `snapshot.requote_role_ids` to `abc`: role 1 can still confirm and the activity log has
   the WARNING. Restore the value.
5. Re-issue a taxed quote's invoice as tax-exempt: the hook leaves it on the list, Review shows a
   Total due deviation and needs a confirmation.
6. Mass Pay an invoice that had credit applied: it seals.

## Known limits

- `provider.sku_id` is still not covered by a snapshot hash; it is cross-checked against the
  verified mapping publication (as in 1.1.5 and 1.1.6).
- The link rules were tested against a fake database. Run the staging checks first.

## Rollback

Restore the previous add-on tree. Snapshots sealed under 1.1.7 are ordinary sealed rows.
