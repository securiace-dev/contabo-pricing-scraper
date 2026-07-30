<?php
/**
 * Price decisions — full audit log.
 *
 * Filter pills + date-range inputs + free-text search wire to app.js the
 * same way `sync_history.tpl` does. Sorting is wired by `data-cb-table`.
 * CSV export is a POST form (CSRF token + filter pass-through).
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var list<array<string,mixed>> $rows
 * @var list<string> $available_policies
 * @var list<string> $available_skip_reasons
 * @var string $filter_from
 * @var string $filter_to
 * @var string $filter_policy
 * @var string $filter_skip_reason
 * @var string $filter_cycle
 */
$cb_filter_cycle = isset($filter_cycle) ? (string) $filter_cycle : '';

$cb_total = is_array($rows) ? count($rows) : 0;

// 4-tile strip — applied / skipped / approval-pending / total.
$cb_applied = 0; $cb_skipped = 0; $cb_awaiting = 0;
foreach ($rows as $cb_r) {
    if (!empty($cb_r['applied']))                   { $cb_applied++; }
    elseif (!empty($cb_r['requires_admin_approval'])) { $cb_awaiting++; }
    else                                            { $cb_skipped++; }
}
$cb_strip_data = [
    ['lbl' => 'Visible decisions', 'v' => (string) $cb_total, 'sub' => 'within current filter', 'tone' => ''],
    ['lbl' => 'Applied',           'v' => (string) $cb_applied, 'sub' => 'price write happened', 'tone' => $cb_applied > 0 ? 'good' : ''],
    ['lbl' => 'Awaiting approval', 'v' => (string) $cb_awaiting, 'sub' => 'queue + force-approve', 'tone' => $cb_awaiting > 0 ? 'warn' : ''],
    ['lbl' => 'Skipped',           'v' => (string) $cb_skipped, 'sub' => 'no price change emitted', 'tone' => ''],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Price decisions</h2>
    <p class="cb-card-sub" data-cb-u="u-8c7c145b64">
      Immutable audit log. Every decision the engine considered is recorded here, applied or not.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=repricing">&larr; Dashboard</a>
  </div>
</header>

<?php /* Filter form ------------------------------------------------------- */ ?>
<form method="get" action="<?= $esc($module_link) ?>" class="cb-toolbar" role="search" aria-label="Audit log filters">
  <input type="hidden" name="module" value="contabo_pricing">
  <input type="hidden" name="action" value="price-decisions">

  <div class="cb-field" data-cb-u="u-ff46992490">
    <label class="glabel" for="cb-pd-from" data-cb-u="u-0aa89a4011">From</label>
    <input id="cb-pd-from" type="date" name="from" data-cb-date-from
           value="<?= $esc($filter_from ?? '') ?>" data-cb-u="u-485fc5be97">
    <label class="glabel" for="cb-pd-to" data-cb-u="u-19db841668">To</label>
    <input id="cb-pd-to" type="date" name="to" data-cb-date-to
           value="<?= $esc($filter_to ?? '') ?>" data-cb-u="u-485fc5be97">
  </div>

  <div class="cb-field" data-cb-u="u-ff46992490">
    <label class="glabel" for="cb-pd-policy">Policy</label>
    <select id="cb-pd-policy" name="policy" data-cb-u="u-485fc5be97">
      <option value="">All policies</option>
      <?php foreach ($available_policies as $cb_p): ?>
        <option value="<?= $esc($cb_p) ?>"<?= ($filter_policy === $cb_p ? ' selected' : '') ?>><?= $esc($cb_p) ?></option>
      <?php endforeach; ?>
    </select>
  </div>

  <div class="cb-field" data-cb-u="u-ff46992490">
    <label class="glabel" for="cb-pd-skip">Skip reason</label>
    <select id="cb-pd-skip" name="skip_reason" data-cb-u="u-485fc5be97">
      <option value="">All</option>
      <?php foreach ($available_skip_reasons as $cb_s): ?>
        <option value="<?= $esc($cb_s) ?>"<?= ($filter_skip_reason === $cb_s ? ' selected' : '') ?>><?= $esc($cb_s) ?></option>
      <?php endforeach; ?>
    </select>
  </div>

  <div class="cb-field" data-cb-u="u-ff46992490">
    <label class="glabel" for="cb-pd-cycle">Cycle</label>
    <select id="cb-pd-cycle" name="cycle" data-cb-u="u-485fc5be97">
      <option value="">All cycles</option>
      <?php foreach (['Monthly','Quarterly','Semi-Annually','Annually','Biennially','Triennially'] as $cb_cyc): ?>
        <option value="<?= $esc($cb_cyc) ?>"<?= ($cb_filter_cycle === $cb_cyc ? ' selected' : '') ?>><?= $esc($cb_cyc) ?></option>
      <?php endforeach; ?>
    </select>
  </div>

  <div class="cb-search">
    <input type="search" placeholder="Search service id, currency, applied_via&hellip;"
           data-cb-search data-cb-search-scope="price-decisions"
           aria-label="Search audit log">
  </div>

  <div class="spacer"></div>
  <button type="submit" class="cb-btn subtle">Apply</button>
</form>

<?php /* Cycle pill filter row — client-side via data-cb-filter-cycle ------- */ ?>
<div class="cb-toolbar" role="region" aria-label="Cycle pill filter" data-cb-u="u-9dcf1dec3b">
  <span class="glabel">Cycle</span>
  <div class="cb-filter-pills" data-cb-filter-group="pd-cycle" role="group">
    <button type="button" data-cb-filter-cycle="all"           aria-pressed="true">All</button>
    <button type="button" data-cb-filter-cycle="Monthly"       aria-pressed="false">Monthly</button>
    <button type="button" data-cb-filter-cycle="Quarterly"     aria-pressed="false">Quarterly</button>
    <button type="button" data-cb-filter-cycle="Semi-Annually" aria-pressed="false">Semi-Annually</button>
    <button type="button" data-cb-filter-cycle="Annually"      aria-pressed="false">Annually</button>
    <button type="button" data-cb-filter-cycle="Biennially"    aria-pressed="false">Biennially</button>
    <button type="button" data-cb-filter-cycle="Triennially"   aria-pressed="false">Triennially</button>
  </div>
</div>

<?php /* CSV export — POST with CSRF + filter pass-through ----------------- */ ?>
<form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-619b35a9f4">
  <input type="hidden" name="action"     value="price-decisions-csv">
  <input type="hidden" name="from"       value="<?= $esc($filter_from ?? '') ?>">
  <input type="hidden" name="to"         value="<?= $esc($filter_to ?? '') ?>">
  <input type="hidden" name="policy"     value="<?= $esc($filter_policy ?? '') ?>">
  <input type="hidden" name="skip_reason" value="<?= $esc($filter_skip_reason ?? '') ?>">
  <input type="hidden" name="cycle"      value="<?= $esc($cb_filter_cycle) ?>">
  <?= generate_token() ?>
  <button type="submit" class="cb-btn ghost" data-cb-action="export-csv">Export CSV</button>
</form>

<?php if ($cb_total === 0): ?>

<div class="cb-card">
  <div class="cb-empty">
    <div class="display">No decisions match these filters</div>
    <p>Try widening the date range or clearing the policy / skip-reason filters.</p>
  </div>
</div>

<?php else: ?>

<div class="cb-card" data-cb-u="u-14f5e9e79f">
  <table class="cb-table" data-cb-table="price-decisions">
    <thead>
      <tr>
        <th data-cb-sort="id">#</th>
        <th data-cb-sort="service_id">Service</th>
        <th data-cb-sort="decided_at">Decided</th>
        <th data-cb-sort="policy_used">Policy</th>
        <th data-cb-sort="billing_cycle">Cycle</th>
        <th class="right" data-cb-sort="old_price">Old</th>
        <th class="right" data-cb-sort="proposed_new_price">Proposed</th>
        <th class="right" data-cb-sort="margin_pct">Margin %</th>
        <th data-cb-sort="tax_mode_snapshot">Tax mode</th>
        <th data-cb-sort="applied">Applied</th>
        <th data-cb-sort="skip_reason">Skip reason</th>
      </tr>
    </thead>
    <tbody>
      <?php foreach ($rows as $r):
        $rid = (int) ($r['id'] ?? 0);
        $svcId = (int) ($r['service_id'] ?? 0);
        $decidedAt = (string) ($r['decided_at'] ?? '');
        $policy = (string) ($r['policy_used'] ?? '');
        $cycle = (string) ($r['billing_cycle'] ?? '');
        $old = (float) ($r['old_price'] ?? 0);
        $new = (float) ($r['proposed_new_price'] ?? 0);
        $marginPct = isset($r['margin_pct']) ? (float) $r['margin_pct'] : null;
        $taxMode = (string) ($r['tax_mode_snapshot'] ?? '—');
        $applied = !empty($r['applied']);
        $skipReason = (string) ($r['skip_reason'] ?? '');
        $appliedVia = (string) ($r['applied_via'] ?? '');
        $currency = (string) ($r['currency'] ?? '');
        $searchText = strtolower(implode(' ', [
            'svc-' . $svcId, $policy, $cycle, $taxMode, $skipReason, $appliedVia, $currency,
        ]));
      ?>
      <tr data-cb-search-text="<?= $esc($searchText) ?>"
          data-cb-cycle="<?= $esc($cycle) ?>">
        <td class="mono">#<?= (int) $rid ?></td>
        <td class="mono">#<?= (int) $svcId ?></td>
        <td class="mono" data-cb-u="u-a49cca52be"><?= $esc(substr($decidedAt, 0, 16)) ?></td>
        <td><span class="cb-pill grey"><?= $esc($policy !== '' ? $policy : '—') ?></span></td>
        <td class="mono" data-cb-u="u-a49cca52be"><?= $esc($cycle) ?></td>
        <td class="right mono price"><?= $esc(number_format($old, 2)) ?></td>
        <td class="right mono price"><?= $esc(number_format($new, 2)) ?></td>
        <td class="right mono">
          <?= $marginPct === null ? '<span data-cb-u="u-eac7694072">—</span>' : $esc(number_format($marginPct, 2) . '%') ?>
        </td>
        <td><code class="mono" data-cb-u="u-bd299c8ad6"><?= $esc($taxMode) ?></code></td>
        <td>
          <?php if ($applied): ?>
            <span class="cb-pill good"><span class="dot"></span>yes</span>
            <?php if ($appliedVia !== ''): ?>
              <div data-cb-u="u-209747f06d"><?= $esc($appliedVia) ?></div>
            <?php endif; ?>
          <?php else: ?>
            <span class="cb-pill grey">no</span>
          <?php endif; ?>
        </td>
        <td><code class="mono" data-cb-u="u-bd299c8ad6"><?= $esc($skipReason) ?></code></td>
      </tr>
      <?php endforeach; ?>
    </tbody>
  </table>
</div>

<p data-cb-u="u-e4401e441a">
  Rows are paginated server-side; refine the date range to load older history.
</p>

<?php endif; ?>

</div>
