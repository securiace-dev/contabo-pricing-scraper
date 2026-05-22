# Phase A.6 Preflight — Configurable-Options Behaviour (Amendment #4)

> **Purpose.** Empirically document how WHMCS *actually* handles configurable-option
> changes through checkout → renewal → upgrade/downgrade, so the A.6.3 apply-mode and the
> §11 lifecycle tree can be built on observed facts. Every number below was measured on a
> **local disposable** WHMCS install; nothing here is theory.
>
> **Stack tested.** `http://localhost:8013` = **WHMCS 8.13.1-release.1** (container
> `securiace-vps-platform-whmcs8-php-1`, **PHP 8.3.27** — *not* PHP 7.4 as the runbook note
> said). Spot-checked `http://localhost:8090` = **WHMCS 9.0.1-release.1** (PHP 8.3) for
> parity. **No production was touched; no `mcp__whmcs__*` tool was used.**
>
> **Currency note (correction).** On this local 8.13 install **currency id 1 = USD**
> (the base/default currency), *not* INR as the runbook said. All amounts below are USD.
> The schema/behaviour is currency-agnostic, so the findings transfer directly to an
> INR-base production install.

---

## 0. Runbook corrections discovered during preflight

These contradict notes in the task runbook / CLAUDE.md and should be propagated:

| Runbook said | Reality on local stack |
|---|---|
| whmcs8 container is PHP 7.4 | It is **PHP 8.3.27** |
| currency id 1 = INR (base) | currency id 1 = **USD** (base/default) |
| `optiontype` `0=dropdown,1=radio,2=yes/no,3=qty,4=text` | **WRONG.** Real WHMCS values are **`1=dropdown(select), 2=radio, 3=yes/no(checkbox), 4=quantity`** (confirmed from `templates/orderforms/standard_cart/configureproduct.tpl` lines 160/173/190/205, identical in 8.13 and 9.0). There is no `0`; an option stored as `optiontype=0` **does not render** in the order form. |
| `tblproductgroups` has `orderfrm` column | Column is **`orderfrmtpl`** |

The `optiontype` correction is **load-bearing** — `WhmcsConfigOptionsAdapter` / `ImageOptionNormalizer` must key off `1/2/3/4`, and the qty multiplier only applies to **type 4**.

---

## 1. Setup — exactly what was created (and how)

**Method:** built directly via **Capsule inserts** through `docker exec … php -r '… require "init.php" …'`
(deterministic + scriptable). Orders were placed with the **`AddOrder` localAPI** and the
upgrade/downgrade tests were driven through the **real client-area `upgrade.php` flow**
(via `CreateSsoToken` SSO). Pricing/qty rendering was verified against the **live
`standard_cart` order form** (browser + raw `curl`). UI-vs-Capsule per item is noted inline.

### Global settings enabled (LOCAL only)
```
ProrataBilling = on ; ProrataDate = 1 ; ProrataChargeNextMonth = 15
EnableConfigOptionUpgrades = on ; CreditOnDowngrade = on (default)
TaxEnabled = '' (tax OFF — proration math shown tax-free)
```
Also: activated the `mailin` (Mail In Payment) gateway so orders/invoices could be raised,
and temporarily set `SystemURL=http://localhost:8013/` for browser order-form testing
(it was `https://localhost` which bounced the browser to an unreachable HTTPS host —
**an environment quirk, not a WHMCS bug**; restored afterward).

### Product
- **Product id = 2** "Preflight VPS", type `other`, group **id = 3** "Preflight".
- Base `tblpricing` (type=product, currency=1): **monthly 10.00, annually 100.00** (other cycles `-1` = disabled).
- `proratabilling=1`, `paytype=recurring`, `tax=1`, later `configoptionsupgrade=1` (required to expose the client-area config-option upgrade path — see §6).

### Configurable-option group (id = 1 "Preflight Options"), linked to product 2
All built via Capsule. Final state (`tblpricing` keyed by `relid = tblproductconfigoptionssub.id`):

| opt id | name | type | sub id | sub name | monthly | annually | msetup |
|---|---|---|---|---|---|---|---|
| 1 | Image | **1 dropdown** | 1 | Ubuntu *(default)* | 2.00 | 20.00 | 0 |
| | | | 2 | Debian | **0.00** | 0.00 | 0 |
| | | | 3 | Windows | 5.00 | 50.00 | **20.00 setup** |
| | | | 8 | Alpine(neg) | **−3.00** | **−30.00** | 0 |
| 2 | Storage | **2 radio** | 4 | SSD *(default)* | 3.00 | 30.00 | 0 |
| | | | 5 | NVMe | 6.00 | 60.00 | 0 |
| 3 | Auto Backup | **3 yes/no** | 6 | Auto Backup | 4.00 | 40.00 | 0 |
| 4 | Extra IPv4 | **4 quantity** | 7 | Extra IPv4 | 1.50 / unit | 15.00 / unit | 0 |

- **Negative-delta fixtures (amendment #1):** Debian priced *below* the Ubuntu default
  ($0 < $2), plus an explicitly **negative** sub-option Alpine(neg) at **−3.00/mo**.
- **Setup fee** on a sub-option: Windows = $20 setup.
- Extra IPv4 `qtyminimum=0, qtymaximum=5`, per-unit $1.50.

### Client / orders
- **Client id = 1** (`preflight@example.test`) via `AddClient` localAPI.
- **Service 1** (order 1, invoice 1): non-default + setup (Windows / NVMe / Backup / IPv4) — used for upgrade/downgrade + renewal tests.
- **Service 2** (order 2, invoice 3): isolated IPv4 qty test.
- **Service 3** (order 3, invoice 4): negative-delta order (Alpine).
- Invoices 1–8 generated across the tests (initial, renewal, 3× upgrade/downgrade, drift-renewal).

---

## 2. Findings (one section per question)

### Q1 — Negative delta *(LOAD-BEARING, amendment #1)*

**Can a sub-option be priced cheaper than / below the default without breaking anything? — YES, with caveats.**

Evidence:
- **DB level:** WHMCS stores a literal negative `tblpricing.monthly = -3.00` / `annually = -30.00` verbatim (read back unchanged). Note `-1` remains the *disabled-cycle* sentinel; `-3.00` is a real price.
- **Order form (`standard_cart`):** selecting Alpine(neg) computes the running total correctly. Live cart summary (`/tmp/cb_preflight_negdelta_cart.png`):
  ```
  » Image: Alpine(neg)   $-3.00 USD
  Monthly: $14.00 USD   ( = 10 base − 3 + 3 SSD + 4 Backup )
  Total Due Today: $18.52 USD (prorated)
  ```
  The negative is **subtracted correctly**; the form does **not** break.
- **Invoice (Service 3 / invoice 4):** `recurringamount = 14.00`, invoice total `18.52`, status Unpaid — valid, no breakage, no negative line item.
- **Upgrade/downgrade view:** negatives are shown explicitly as deltas, e.g. `Debian $-2.00 USD`, `Alpine(neg) $-5.00 USD` (delta from the then-current Ubuntu).

**Caveat A — silent price hiding on the order form.** In the *new-order* config form, a
sub-option priced **$0 or negative renders with NO price label** (Debian shows just
"Debian"; Alpine(neg) shows just "Alpine(neg)"). The negative only appears once selected,
in the right-hand Order Summary. A customer scanning the dropdown sees no indication that
Alpine is cheaper/credited. **Misleading UX, not a math bug.**

**Caveat B — downgrades become CREDITS, not negative charges.** When a negative delta is
applied mid-term (see Q2), WHMCS issues an account **Credit** rather than a negative
invoice line. Net of a +$3 upgrade and a −$2 downgrade in one transaction = `$3 subtotal,
$2 credit, $1.00 total` (real invoice #5, `/tmp/cb_preflight_upgrade_invoice.png`).

**Verdict input for A.6:** WHMCS itself tolerates negative config-option prices end-to-end
(form, cart, invoice, upgrade, downgrade, renewal — no exceptions, no NaN, correct
arithmetic). The risks are **presentation** (hidden price label) and **semantics**
(downgrade-as-credit interacts with `CreditOnDowngrade`), not engine breakage.

---

### Q2 — Mid-term config-option change pricing (admin & client paths)

**WHMCS behaviour = (a) prorate + raise an immediate invoice.** Confirmed via the real
client-area `upgrade.php?type=configoptions` flow.

Worked example (Service 1, ~1 month remaining; Image Ubuntu→Debian = −2, Storage SSD→NVMe = +3):
- Upgrade *review* page: `Subtotal $1.00, Total Due Today $1.00` (`/tmp/cb_preflight_upgrade_summary.png`).
- Generated **invoice #5**: line `Storage: SSD => NVMe (22/05–22/06) $3.00`, **Sub Total $3.00, Credit $2.00, Total $1.00** (`/tmp/cb_preflight_upgrade_invoice.png`).
- Proration window = the remaining cycle (here a full month → full delta). The upgrade
  portion is invoiced for that window; the downgrade portion is credited.

**Apply timing is split (critical for §11 lifecycle):**
- **Downgrade applied IMMEDIATELY** — `tblupgrades` row `status=Completed` the moment the change is requested; `tblhostingconfigoptions.optionid` flips and `recurringamount` drops *before* payment.
- **Upgrade applied ONLY ON INVOICE PAYMENT** — `tblupgrades` row stays `status=Pending`; the selection and `recurringamount` do **not** change until the proration invoice is paid, at which point the row → `Completed` and `recurringamount` recomputes.

Observed `tblupgrades` rows: `type=configoptions`, `originalvalue` like `2=>4` /
`1=>1`, `newvalue` = chosen sub id (or qty), `status ∈ {Pending, Completed}`,
`orderid` links the proration order.

There is no separate "admin changes it for free" path that prorates — the admin
**Products/Services** edit page lets you change selections and recalc, but that path
rewrites `recurringamount` without raising a proration invoice (it's an out-of-band edit).
The **client-facing, invoice-raising** path is `upgrade.php`.

---

### Q3 — Quantity change (type-4 option)

**Prorated upgrade invoice; `tblhostingconfigoptions.qty` updates on payment.**

> First a gotcha: a quantity option **only renders as a quantity field when
> `optiontype = 4`**. Built initially as `3` it rendered as a yes/no **checkbox** and was
> only ever charged for **1 unit** regardless of stored qty — the source of an apparent
> "qty pricing bug". After correcting to type 4 it renders `<input type=text … class="form-control-qty">`
> (or a number/slider when `qtymaximum` is set) and multiplies correctly.

Cart math with IPv4 qty=3 (`/tmp/cb_preflight_qty_cart.png`):
```
» Extra IPv4: 3   $4.50 USD   ( = 3 × 1.50 )
Monthly: $19.50 USD
```
Mid-term change via `upgrade.php` (Service 1, IPv4 1→4):
- Review: `Extra IPv4: 1 => 4 x Extra IPv4  $4.50 USD` (delta 3 units × $1.50), `Total Due Today $4.50`.
- Invoice #6 raised; **before payment** `qty` stays 1 and `recurringamount` unchanged;
  **after payment** `tblhostingconfigoptions.qty = 4` and `recurringamount` recomputes
  ($21.50 → $26.00, i.e. +$4.50). Decreases work symmetrically (credit, like Q2).

---

### Q4 — Setup fee on a config option

**Setup fee is charged ONLY at the initial order. It is NOT re-charged on any
upgrade/downgrade or config change.**

- **Initial order (Service 1):** invoice 1 contained a separate `type=Setup` line
  `Preflight VPS Setup Fee  $20.00` (the Windows sub-option's $20 setup), plus the
  prorated Hosting line. So setup fees from selected config options *are* billed at signup.
- **Mid-term change to Windows (Debian→Windows via `upgrade.php`):** review showed only
  `Image: Debian => Windows $5.00` (the recurring delta). Invoice #7 had **no Setup line**
  — `Total $5.00`, recurring delta only. Programmatic check: `SETUP FEE re-charged = NO`.

Implication: setup fees are a **first-order-only** event. Renewal-margin / lifecycle code
must not expect a setup fee on subsequent option changes.

---

### Q5 — Renewal summing *(amendment #5)*

**CONFIRMED: the renewal invoice is a single Hosting line equal to the stored
`tblhosting.amount` (`recurringamount`) — and that stored value is a SNAPSHOT that DRIFTS.
Therefore revenue must be summed from selections × current prices, never read from
`recurringamount`.**

Two proofs:

1. **Renewal structure.** A cron-generated renewal invoice (`--CreateInvoices` +
   `--RunJobsQueue`) is **one** `type=Hosting` line whose amount **= `recurringamount`**,
   with the selected options listed only as description bullets (e.g.
   `Image: Windows / Storage: NVMe / Auto Backup: No / Extra IPv4 …`). It does **not**
   re-price the options at renewal time; it trusts the stored figure.

2. **`recurringamount` drifts.** When selections are in sync, `recurringamount` *does*
   equal `base + Σ(config-option recurring × qty)`:
   ```
   recurringamount 31.00 = 10 base + 5 Win + 6 NVMe + 4 Backup + (4 × 1.50 IPv4)   ✓ match
   ```
   But changing a config-option **price** after the service exists does **not** update
   `recurringamount`:
   ```
   admin sets NVMe 6.00 → 8.00   ⇒  true sum = 33.00, but recurringamount stays 31.00
   renewal invoice #8 generated  ⇒  total = 31.00   (UNDERCHARGES by 2.00)
   ```
   `recurringamount` is only recomputed on service-level events (order accept, paid
   upgrade), never on a bare price change.

**Verdict input:** `ServiceRevenueResolver` **must** compute
`base_recurring + Σ(tblhostingconfigoptions row → tblpricing[suboptionid].<cycle> × chargeable_qty)`
from **current** pricing, **not** `tblhosting.recurringamount`. The qty rule per type:
type 1/2 → ×1 (the selected sub); **type 3 (yes/no)** → ×1 **iff `qty=1`, else $0**;
**type 4 (quantity)** → ×`qty`. (Addons + discounts then layer on as the amendment states.)

---

### Q6 — Upgrade vs downgrade path (client-area exposure)

**WHMCS DOES expose config-option changes through the client-area
`upgrade.php?type=configoptions` flow — but only when gated correctly.**

Gates observed:
- **Per-product `configoptionsupgrade=1` is required** (global `EnableConfigOptionUpgrades`
  alone is insufficient). With it `0`, the page renders nothing useful.
- **Blocked while a renewal invoice is outstanding:** the page shows *"You cannot currently
  upgrade or downgrade this product because an invoice has already been generated for the
  next renewal. … please first pay the outstanding invoice."* — i.e. you cannot stack a
  proration on top of an unpaid renewal. (Reproduced; cleared by cancelling the dangling
  renewal invoice + pushing the due date out of the 14-day `CreateInvoiceDaysBefore`
  window.)

Upgrade vs downgrade differences:
- The form shows **deltas relative to the current selection** (`No Change*` for current;
  `+$x` / `−$x` for others). Negatives are shown here (unlike the new-order form).
- **Upgrade (net positive):** proration invoice raised; selection + `recurringamount`
  change **on payment** (`tblupgrades` `Pending`→`Completed`).
- **Downgrade (net negative):** applied **immediately**, surfaced as an account **Credit**
  (driven by `CreditOnDowngrade=on`); no negative invoice line.
- **Mixed:** netted into one invoice (`Sub Total` = upgrades, `Credit` = downgrades,
  `Total` = net).
- **"Not allowed" cases:** outstanding-renewal block (above); a yes/no option that is
  already the only state offers just `No Change`.

---

### Q7 — Storage of selections (what a §12 snapshot must capture)

Selections live in **`tblhostingconfigoptions`** (`relid` = service id, `configid` =
option id, `optionid` = chosen sub-option id, `qty`), and the price for each lives in
**`tblpricing`** rows `type='configoptions'` keyed by `relid = tblproductconfigoptionssub.id`.

**Per-type semantics (must be encoded in the snapshot/resolver):**

| optiontype | meaning | `optionid` | `qty` | chargeable? |
|---|---|---|---|---|
| 1 dropdown | the selected sub | the chosen sub id | unused (0) | yes, ×1 |
| 2 radio | the selected sub | the chosen sub id | unused (0) | yes, ×1 |
| 3 yes/no | the (single) sub | always that sub id | **1 = YES, 0 = NO** | only if `qty=1` |
| 4 quantity | the (single) sub | always that sub id | **the unit count** | ×`qty` |

> Note for type 3/4: a `tblhostingconfigoptions` row **always exists** even when the option
> is "off" — `qty=0` is the signal for "not charged". A resolver that counts every row
> blindly will over-bill yes/no-off and qty-0 options.

A snapshot (§12 `config_option_price_snapshot`) must therefore capture, **per selected option**:
`configid, optionname, optiontype, optionid (chosen sub), sub optionname, qty,
unit price per cycle (the 6 `tblpricing` cycle columns), setup fee, currency` — plus the
**base product price** and `tax_mode`. Capturing the price *value* (not just the id) is
essential because `tblpricing` drifts (Q5).

---

## 3. Verdicts on the binding amendments

### Amendment #1 — negative-delta clamp
- **WHMCS engine is SAFE with negatives** end-to-end (form, cart, invoice, upgrade,
  downgrade, renewal, tax-off all computed correctly; DB stores negatives verbatim).
- **But two real hazards** argue for keeping the clamp in **A.6 v1**:
  1. **Order-form hides $0/negative price labels** → customers can't see the discount/credit (trust/legibility risk).
  2. **Downgrades manifest as account credits** (via `CreditOnDowngrade`), which complicates
     margin/landed-cost reasoning and can produce balances the addon must account for.
- **Recommendation:** **KEEP `clamp ≥ 0` for A.6 v1** as a *policy* default (it is not
  forced by a WHMCS limitation). Document that the platform *can* support negatives, and
  gate any relaxation behind an explicit `service_config_price_policy` opt-in plus a UI fix
  for the hidden-label issue. Not a v1 blocker either way.

### Amendment #5 — `ServiceRevenueResolver`
- **CONFIRMED — must sum, must NOT trust `recurringamount`.** `recurringamount` equals the
  true sum *only* until any option price changes, after which it drifts and the WHMCS
  renewal silently under/over-charges. Resolver must compute from
  `tblhostingconfigoptions × current tblpricing` with the per-type qty rules in Q7
  (+ addons + discounts).

### §11 lifecycle tree — "post-provision / proration" assumptions
- **Hold, with refinements:**
  - Mid-term changes **do** prorate + raise an immediate invoice (assumption holds).
  - **Apply timing is asymmetric:** downgrades apply *immediately* (pre-payment, as a
    credit); upgrades apply *on payment*. The lifecycle/provisioning hook must key off
    `tblupgrades.status` flipping to `Completed` (and the paid proration order), **not** off
    the upgrade *request*, or it will provision an upgrade the customer hasn't paid for.
  - **Setup fees are first-order-only** — no setup re-charge on option changes.
  - **Quantity** changes are first-class upgrades (per-unit × Δqty) — provisioning must read
    the new `qty` only after `Completed`.

---

## 4. A.6.3 go / no-go recommendation

**GO** for A.6.3, building on these facts:

**Safe to implement now**
- Reading selections from `tblhostingconfigoptions` + prices from `tblpricing` (schema
  identical on 8.13 and 9.0).
- `ServiceRevenueResolver` summing base + per-option (with the type-1/2/3/4 qty rules) from
  *current* pricing — this is the correct and necessary design (Q5).
- Snapshot (§12) capturing per-option id + **price value** + qty + setup + base + tax mode (Q7).
- Treating setup fees as first-order-only (Q4).

**Must guard**
- **Negative-delta clamp ≥ 0 stays on in v1** (policy, not platform limit). Any future
  relaxation needs the order-form label fix + credit-accounting handling (Amendment #1).
- **Per-type qty semantics:** `optiontype` is `1/2/3/4` (runbook's `0..4` is wrong);
  type-3 charges only when `qty=1`; type-4 multiplies by `qty`; type-0 rows don't render.
  Mis-mapping here directly corrupts revenue.
- **Lifecycle apply-timing:** provision/recompute on `tblupgrades.status=Completed` (paid),
  and remember downgrades land immediately as credits.
- **`recurringamount` is untrusted** — never use it as the revenue source of truth.

**Blockers**
- **None.** No WHMCS behaviour observed prevents A.6.3. The only "can't test purely in UI"
  item was deep cron-internal invoice generation; we drove it via `crons/cron.php do
  --CreateInvoices --RunJobsQueue` and confirmed via DB.

---

## 5. WHMCS 8.13 vs 9.0 spot-check

| Aspect | 8.13.1 | 9.0.1 | Same? |
|---|---|---|---|
| `tblpricing` columns | 6 cycle + 6 setup, `relid`-keyed | identical | ✅ |
| `tblhostingconfigoptions` columns | `id,relid,configid,optionid,qty` | identical | ✅ |
| `optiontype` map in `configureproduct.tpl` | `1/2/3/4` at lines 160/173/190/205 | identical lines | ✅ |
| `CreditOnDowngrade` default | `on` | `on` | ✅ |
| currency id 1 | USD | USD | ✅ |

No 8.13-vs-9.0 differences found in any tested area; findings transfer to both.

---

## Appendix — evidence index

- Fixtures: product 2, group 3, configgroup 1, options 1–4 (subs 1–8), client 1, services 1–3, invoices 1–8 (LOCAL whmcs8 DB).
- Screenshots:
  - `/tmp/cb_preflight_admin_service.png` — admin service page (config-option controls; note radio→select, qty→checkbox-when-type-3).
  - `/tmp/cb_preflight_negdelta_cart.png` — **Q1** order-form cart with Alpine(neg) → Monthly $14.00.
  - `/tmp/cb_preflight_upgrade_summary.png` — **Q2** upgrade review (Ubuntu→Debian −2 / SSD→NVMe +3 → Due $1.00).
  - `/tmp/cb_preflight_upgrade_invoice.png` — **Q2/Q1** invoice #5 (Sub Total $3.00, Credit $2.00, Total $1.00).
  - `/tmp/cb_preflight_qty_cart.png` — **Q3** cart with IPv4 qty=3 → Extra IPv4 $4.50, Monthly $19.50.
- Key invoices: #1 initial (Setup $20 + prorated Hosting), #5 upgrade/credit, #6 qty upgrade, #7 Windows change (no setup), #8 renewal with drifted price (undercharge proof).
