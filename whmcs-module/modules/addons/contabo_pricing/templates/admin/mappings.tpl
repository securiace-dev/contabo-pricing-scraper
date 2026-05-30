<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Mappings admin page — Phase A.5 rewrite.
 *
 * Replaces the legacy 3-checkbox cycle form with a 6-row cycle table backed
 * by bitmasks (the legacy boolean columns were dropped in schema v3). The new
 * form posts:
 *
 *   profile_id, product_id, currency_id            (selects)
 *   catalog_cycles_mask        (hidden, JS-maintained int)
 *   renewal_cycles_mask        (hidden, JS-maintained int)
 *   markup_overrides_json      (hidden, JS-maintained JSON map)
 *   respect_disabled_cycles    (checkbox, default on)
 *   overwrite_free_cycles      (checkbox, default off)
 *   sync_setup_fees            (checkbox, default off)
 *   rounding_mode              (select)
 *
 * The cycles table is rendered empty on first paint and populated client-side
 * via GET /ajax-product-cycles?product_id=…&currency_id=… as soon as a product
 * (and optionally a currency) is picked. See assets/app.js → wireMappingForm().
 *
 * Expected data:
 *   $profiles            list of profile rows
 *   $whmcs_products      list of {id, name, gid}
 *   $whmcs_currencies    list of {id, code, prefix}
 *   $default_currency_id int — WHMCS base currency id
 *   $mappings            list of mapping rows (now with bitmask columns)
 *   $flash               optional success message
 *   $module_link         base URL
 *   $esc                 escaping closure
 */

$cb_default_currency_id = isset($default_currency_id) ? (int) $default_currency_id : 0;
$cb_currencies = isset($whmcs_currencies) && is_array($whmcs_currencies) ? $whmcs_currencies : [];

// Canonical six-cycle ordering shared with CycleSet / CyclePricingMap.
$cb_cycles = [
    ['cycle' => 'Monthly',       'short' => 'mo',   'months' => 1,  'bit' => 0, 'rec_col' => 'monthly',     'setup_col' => 'msetupfee'],
    ['cycle' => 'Quarterly',     'short' => '3 mo', 'months' => 3,  'bit' => 1, 'rec_col' => 'quarterly',   'setup_col' => 'qsetupfee'],
    ['cycle' => 'Semi-Annually', 'short' => '6 mo', 'months' => 6,  'bit' => 2, 'rec_col' => 'semiannually','setup_col' => 'ssetupfee'],
    ['cycle' => 'Annually',      'short' => '12 mo','months' => 12, 'bit' => 3, 'rec_col' => 'annually',    'setup_col' => 'asetupfee'],
    ['cycle' => 'Biennially',    'short' => '24 mo','months' => 24, 'bit' => 4, 'rec_col' => 'biennially',  'setup_col' => 'bsetupfee'],
    ['cycle' => 'Triennially',   'short' => '36 mo','months' => 36, 'bit' => 5, 'rec_col' => 'triennially', 'setup_col' => 'tsetupfee'],
];

$cb_rounding_modes = [
    'exact_2_decimals' => 'Exact (2 decimals)',
    'nearest_rupee'    => 'Nearest rupee',
    'nearest_9'        => 'Nearest .9 (₹99, ₹199…)',
    'nearest_99'       => 'Nearest 99 (₹199, ₹299…)',
    'nearest_100'      => 'Nearest 100',
];
?>

<header style="margin:6px 0 18px;">
  <h2 class="display" style="margin:0 0 4px;">Mappings</h2>
  <p class="cb-card-sub" style="margin:0; max-width:78ch;">
    Link Contabo profiles to WHMCS products and choose, per billing cycle, whether
    SyncEngine writes to the catalog (<code class="mono">tblpricing</code>) and whether
    RenewalEngine considers it for existing services.
  </p>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<div style="display:grid; grid-template-columns:minmax(420px, 1.1fr) minmax(0, 1fr); gap:16px; align-items:flex-start;">

  <!-- ───────── Left card: add / update mapping ───────── -->
  <section class="cb-card" data-cb-mapping-form-scope>
    <h3 class="cb-card-title" style="margin:0 0 4px;">Add or update mapping</h3>
    <p class="cb-card-sub" style="margin:0 0 14px;">Pick a profile, product, currency, then per-cycle catalog/renewal flags.</p>

    <form method="post" action="<?= $esc($module_link) ?>" data-cb-form="mapping-save">
      <input type="hidden" name="action" value="mapping-save">
      <?= generate_token() ?>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
        <div class="cb-field">
          <label for="cb-map-profile">Contabo profile</label>
          <select id="cb-map-profile" name="profile_id" required data-cb-mapping-profile>
            <option value="">— pick a profile —</option>
            <?php foreach ($profiles as $p):
              $pid  = (int) ($p['id'] ?? 0);
              $slug = (string) ($p['slug'] ?? '');
              $name = (string) ($p['name'] ?? '');
            ?>
              <option value="<?= $pid ?>"><?= $esc($slug) ?> — <?= $esc($name) ?></option>
            <?php endforeach; ?>
          </select>
        </div>

        <div class="cb-field">
          <label for="cb-map-product">WHMCS product</label>
          <select id="cb-map-product" name="product_id" required data-cb-product-id>
            <option value="">— pick a product —</option>
            <?php foreach ($whmcs_products as $wp):
              $wid   = (int) ($wp['id'] ?? 0);
              $wname = (string) ($wp['name'] ?? '');
            ?>
              <option value="<?= $wid ?>"><?= $esc($wname) ?> (#<?= $wid ?>)</option>
            <?php endforeach; ?>
          </select>
        </div>
      </div>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
        <div class="cb-field">
          <label for="cb-map-currency">Currency</label>
          <select id="cb-map-currency" name="currency_id" data-cb-currency-id>
            <?php if (empty($cb_currencies)): ?>
              <option value="<?= (int) $cb_default_currency_id ?>">(base currency)</option>
            <?php else: ?>
              <?php foreach ($cb_currencies as $cu):
                $cid    = (int) ($cu['id'] ?? 0);
                $ccode  = (string) ($cu['code'] ?? '');
                $cpref  = (string) ($cu['prefix'] ?? '');
                $isDef  = ($cid === $cb_default_currency_id);
              ?>
                <option value="<?= $cid ?>"<?= $isDef ? ' selected' : '' ?>><?= $esc($ccode) ?><?= $cpref !== '' ? ' (' . $esc($cpref) . ')' : '' ?><?= $isDef ? ' — base' : '' ?></option>
              <?php endforeach; ?>
            <?php endif; ?>
          </select>
        </div>

        <div class="cb-field">
          <label for="cb-map-rounding">Rounding mode</label>
          <select id="cb-map-rounding" name="rounding_mode" data-cb-rounding-mode>
            <?php foreach ($cb_rounding_modes as $rk => $rl): ?>
              <option value="<?= $esc($rk) ?>"<?= $rk === 'exact_2_decimals' ? ' selected' : '' ?>><?= $esc($rl) ?></option>
            <?php endforeach; ?>
          </select>
        </div>
      </div>

      <?php /* Hidden mask + json fields — JS owns these. */ ?>
      <input type="hidden" name="catalog_cycles_mask"   value="0" data-cb-catalog-mask>
      <input type="hidden" name="renewal_cycles_mask"   value="0" data-cb-renewal-mask>
      <input type="hidden" name="markup_overrides_json" value="{}" data-cb-markup-overrides-json>
      <?php /* v8: optional per-product per-cycle SOURCE basis override (EUR/mo).
               Empty {} = use the profile's source vector. Reserved for a future
               inline editor; persisted as-is today. */ ?>
      <input type="hidden" name="source_overrides_json" value="{}" data-cb-source-overrides-json>

      <div class="cb-field">
        <label>Billing cycles</label>
        <div class="cb-card" style="margin:6px 0; padding:0; background:var(--bg-elev); overflow:hidden;">
          <table class="cb-table" data-cb-cycles-table>
            <thead>
              <tr>
                <th>Cycle</th>
                <th class="right">Source (from profile)</th>
                <th class="right">Catalog price</th>
                <th style="text-align:center">Catalog sync</th>
                <th style="text-align:center">Renewal reprice</th>
                <th>Markup override</th>
              </tr>
            </thead>
            <tbody data-cb-cycles-tbody>
              <?php foreach ($cb_cycles as $row): ?>
                <tr data-cb-cycle-row="<?= $esc($row['cycle']) ?>"
                    data-cb-cycle-bit="<?= (int) $row['bit'] ?>"
                    data-cb-cycle-months="<?= (int) $row['months'] ?>"
                    data-cb-cycle-status="absent">
                  <td>
                    <div style="font-weight:600"><?= $esc($row['cycle']) ?></div>
                    <div class="muted mono" style="font-size:11px"><?= $esc($row['short']) ?> · <?= $esc($row['rec_col']) ?></div>
                  </td>
                  <td class="right" data-cb-cycle-source>
                    <span class="muted" style="font-size:11px">—</span>
                  </td>
                  <td class="right mono price" data-cb-cycle-current-price>
                    <span class="muted" style="font-size:11.5px">—</span>
                  </td>
                  <td style="text-align:center">
                    <input type="checkbox"
                           data-cb-cycle-bit-catalog="<?= (int) $row['bit'] ?>"
                           data-cb-cycle="<?= $esc($row['cycle']) ?>"
                           disabled
                           aria-label="Catalog sync for <?= $esc($row['cycle']) ?>">
                  </td>
                  <td style="text-align:center">
                    <input type="checkbox"
                           data-cb-cycle-bit-renewal="<?= (int) $row['bit'] ?>"
                           data-cb-cycle="<?= $esc($row['cycle']) ?>"
                           aria-label="Renewal reprice for <?= $esc($row['cycle']) ?>">
                  </td>
                  <td>
                    <button type="button" class="cb-btn ghost"
                            data-cb-markup-toggle="<?= $esc($row['cycle']) ?>"
                            style="padding:3px 8px; font-size:11.5px;">+ Override</button>
                    <div data-cb-markup-editor="<?= $esc($row['cycle']) ?>" hidden style="margin-top:6px; display:flex; gap:4px; align-items:center;">
                      <select data-cb-markup-strategy="<?= $esc($row['cycle']) ?>" style="padding:3px 6px; font-size:12px;">
                        <option value="inherit" selected>inherit</option>
                        <option value="cost_plus_pct">cost+%</option>
                        <option value="cost_plus_amount">cost+amt</option>
                        <option value="fixed">fixed</option>
                      </select>
                      <input type="number" step="0.01"
                             data-cb-markup-value="<?= $esc($row['cycle']) ?>"
                             placeholder="value"
                             style="width:80px; padding:3px 6px; font-size:12px;">
                    </div>
                  </td>
                </tr>
              <?php endforeach; ?>
            </tbody>
          </table>
          <div data-cb-cycles-loading style="padding:8px 14px; font-size:11.5px; color:var(--muted);">
            Pick a product to load catalog prices…
          </div>
        </div>
      </div>

      <div class="cb-field">
        <label>Catalog write guards</label>
        <div style="display:flex; flex-direction:column; gap:6px; padding:6px 0;">
          <label style="display:flex; gap:6px; align-items:flex-start; cursor:pointer; font-size:12.5px;">
            <input type="checkbox" name="respect_disabled_cycles" value="1" checked data-cb-respect-disabled>
            <span>Honour WHMCS's <code class="mono">-1.00</code> disabled-cycle sentinel
              <span class="muted" style="display:block; font-size:11.5px;">When on, cycles marked disabled in <code class="mono">tblpricing</code> stay disabled — catalog sync is blocked for them.</span>
            </span>
          </label>
          <label style="display:flex; gap:6px; align-items:flex-start; cursor:pointer; font-size:12.5px;">
            <input type="checkbox" name="overwrite_free_cycles" value="1" data-cb-overwrite-free>
            <span>Allow overwriting cycles currently priced at <code class="mono">0.00</code>
              <span class="muted" style="display:block; font-size:11.5px;">Most free cycles are intentional promos. Off by default.</span>
            </span>
          </label>
          <label style="display:flex; gap:6px; align-items:flex-start; cursor:pointer; font-size:12.5px;">
            <input type="checkbox" name="sync_setup_fees" value="1" data-cb-sync-setup-fees>
            <span>Sync setup-fee columns too
              <span class="muted" style="display:block; font-size:11.5px;">Affects only new orders. Renewal repricing never touches setup fees.</span>
            </span>
          </label>
        </div>
      </div>

      <div class="cb-card" style="margin:12px 0; background:#faf6f1;" data-cb-mapping-preview>
        <div class="cb-card-sub" style="margin-bottom:4px">Summary</div>
        <div data-cb-mapping-preview-body>
          <span class="muted" style="font-size:12.5px">Pick a profile + product to preview which cycles will sync.</span>
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; gap:8px;">
        <button type="submit" class="cb-btn">Save mapping</button>
      </div>
    </form>
  </section>

  <!-- ───────── Right card: existing mappings ───────── -->
  <section class="cb-card" style="padding:0;">
    <header style="padding:16px 20px 8px; display:flex; justify-content:space-between; align-items:flex-end; gap:12px;">
      <div>
        <h3 class="cb-card-title" style="margin:0 0 4px;">Existing mappings</h3>
        <p class="cb-card-sub" style="margin:0;">All profile → product links currently configured.</p>
      </div>
      <div class="cb-pill grey"><?= (int) count($mappings) ?> total</div>
    </header>

    <?php if (empty($mappings)): ?>
      <div class="cb-empty" style="margin:8px 16px 20px;">
        <div class="display" style="font-size:18px; margin-bottom:6px;">No mappings yet</div>
        <p class="cb-card-sub" style="margin:0;">Map a profile to a product on the left to start syncing prices.</p>
      </div>
    <?php else: ?>
      <?php
      $byProfile = [];
      foreach ($profiles as $pp) { $byProfile[(int) ($pp['id'] ?? 0)] = $pp; }
      $byProduct = [];
      foreach ($whmcs_products as $wp2) { $byProduct[(int) ($wp2['id'] ?? 0)] = $wp2; }
      ?>
      <table class="cb-table" data-cb-table="mappings">
        <thead>
          <tr>
            <th data-cb-sort="profile_slug">Profile <span class="arr"></span></th>
            <th data-cb-sort="product_name">WHMCS product <span class="arr"></span></th>
            <th>Cycles managed</th>
            <th data-cb-sort="updated_at">Updated <span class="arr"></span></th>
            <th data-cb-sort="active">Status <span class="arr"></span></th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($mappings as $m):
            $mid       = (int)  ($m['id'] ?? 0);
            $profileId = (int)  ($m['profile_id'] ?? 0);
            $productId = (int)  ($m['product_id'] ?? 0);
            $pRow      = isset($byProfile[$profileId]) ? $byProfile[$profileId] : null;
            $wpRow     = isset($byProduct[$productId]) ? $byProduct[$productId] : null;
            $pSlug     = $pRow ? (string) ($pRow['slug'] ?? '') : ('#' . $profileId);
            $pName     = $pRow ? (string) ($pRow['name'] ?? '') : '';
            $wpName    = $wpRow ? (string) ($wpRow['name'] ?? '') : '';
            $isActive  = (bool) ($m['active'] ?? false);
            $updated   = (string) ($m['updated_at'] ?? '');

            // Schema v3+: cycle masks are authoritative. The legacy per-cycle
            // boolean columns were dropped by migrateTo3() and must never be
            // read at runtime (see MappingRepository / LegacyFieldGrepTest).
            $catalogMask = isset($m['catalog_cycles_mask']) ? (int) $m['catalog_cycles_mask'] : 0;
            $renewalMask = isset($m['renewal_cycles_mask']) ? (int) $m['renewal_cycles_mask'] : 0;
          ?>
          <tr
            data-cb-mapping-row
            data-cb-mapping-id="<?= $mid ?>"
            data-cb-mapping-active="<?= $isActive ? 1 : 0 ?>"
            data-cb-mapping-catalog-mask="<?= (int) $catalogMask ?>"
            data-cb-mapping-renewal-mask="<?= (int) $renewalMask ?>"
            data-cb-sort-profile_slug="<?= $esc(strtolower($pSlug)) ?>"
            data-cb-sort-product_name="<?= $esc(strtolower($wpName)) ?>"
            data-cb-sort-updated_at="<?= $esc($updated) ?>"
            data-cb-sort-active="<?= $isActive ? 1 : 0 ?>"
          >
            <td>
              <div class="mono" style="font-weight:600"><?= $esc($pSlug) ?></div>
              <?php if ($pName !== ''): ?>
                <div class="muted" style="font-size:11px"><?= $esc($pName) ?></div>
              <?php endif; ?>
            </td>
            <td>
              <?php if ($wpRow): ?>
                <div><?= $esc($wpName) ?></div>
                <div class="muted mono" style="font-size:11px">#<?= $productId ?></div>
              <?php else: ?>
                <span class="muted">#<?= $productId ?> (missing)</span>
              <?php endif; ?>
            </td>
            <td>
              <?php
              $anyChip = false;
              foreach ($cb_cycles as $cyclerow) {
                  $bit = (int) $cyclerow['bit'];
                  $inCatalog = ($catalogMask & (1 << $bit)) !== 0;
                  $inRenewal = ($renewalMask & (1 << $bit)) !== 0;
                  if (!$inCatalog && !$inRenewal) continue;
                  $anyChip = true;
                  $tone = ($inCatalog && $inRenewal) ? 'good' : ($inCatalog ? 'warn' : 'grey');
                  $label = strtolower($cyclerow['cycle']);
                  $suffix = ($inCatalog && $inRenewal)
                      ? ''
                      : ($inCatalog ? ' · catalog' : ' · renewal');
                  echo '<span class="cb-pill ' . $esc($tone) . '" title="catalog=' . ($inCatalog ? 'on' : 'off') . ', renewal=' . ($inRenewal ? 'on' : 'off') . '">'
                      . $esc($label . $suffix) . '</span> ';
              }
              if (!$anyChip): ?>
                <span class="cb-pill grey">none</span>
              <?php endif; ?>
            </td>
            <td class="mono"><?= $esc(substr($updated, 0, 16)) ?></td>
            <td>
              <?php if ($isActive): ?>
                <span class="cb-pill good">active</span>
              <?php else: ?>
                <span class="cb-pill bad">inactive</span>
              <?php endif; ?>
            </td>
            <td>
              <button type="button" class="cb-btn ghost" style="padding:3px 8px; font-size:11.5px;"
                      data-cb-edit-mapping-id="<?= $mid ?>"
                      data-cb-edit-profile-id="<?= (int) $profileId ?>"
                      data-cb-edit-product-id="<?= (int) $productId ?>">Edit</button>
            </td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    <?php endif; ?>
  </section>

</div>

</div>
