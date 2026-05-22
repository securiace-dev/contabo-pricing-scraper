<?php
/**
 * Exposure editor — curate which configurable-option dimensions are exposed to
 * customers per profile (the `expose_to_customer` / `hidden` flags on each
 * option-link). Saving records intent only; the admin must re-run Apply on the
 * preview screen to push the flags to the live WHMCS product.
 *
 * @var \Closure $esc
 * @var string   $module_link
 * @var array<string,mixed> $profile
 * @var list<array<string,mixed>> $option_links
 * @var string   $flash
 */

$cb_links  = isset($option_links) && is_array($option_links) ? $option_links : [];
$cb_pid    = (int) ($profile['id'] ?? 0);
$cb_typeLabel = [1 => 'Dropdown', 2 => 'Radio', 3 => 'Yes/No', 4 => 'Quantity'];

$cb_exposed_n = 0;
$cb_hidden_n  = 0;
foreach ($cb_links as $cb_l) {
    if (!empty($cb_l['expose_to_customer'])) { $cb_exposed_n++; }
    if (!empty($cb_l['hidden'])) { $cb_hidden_n++; }
}

$cb_strip_data = [
    ['lbl' => 'Options', 'v' => (string) count($cb_links), 'sub' => 'option-links for this profile', 'tone' => ''],
    ['lbl' => 'Exposed', 'v' => (string) $cb_exposed_n, 'sub' => 'shown to customers', 'tone' => ''],
    ['lbl' => 'Hidden', 'v' => (string) $cb_hidden_n, 'sub' => 'force-hidden', 'tone' => $cb_hidden_n > 0 ? 'warn' : ''],
    ['lbl' => 'Effect', 'v' => 'on Apply', 'sub' => 'flags push when re-applied', 'tone' => ''],
];

require __DIR__ . '/_layout_open.tpl';
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Exposure editor</h2>
    <p class="cb-card-sub" style="margin:0; max-width:64ch;">
      Choose which configurable options are exposed to customers for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . $cb_pid)) ?></strong>
      (<code class="mono"><?= $esc($profile['plan_slug'] ?? '') ?></code>).
      Saving records your choices — you must re-run <em>Apply</em> to push them to the live product.
    </p>
  </div>
  <div style="display:flex; gap:8px;">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">&larr; Back to preview</a>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<div class="cb-card" style="padding:12px 16px; margin-bottom:14px; border-left:3px solid var(--accent);">
  <strong>Saving here does not change the live product yet.</strong> These flags are
  recorded on the option-links. Re-run <em>Apply</em> on the
  <a href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">preview screen</a>
  to push the updated visibility to WHMCS.
</div>

<?php if ($cb_links === []): ?>
  <div class="cb-card"><div class="cb-empty">
    <div class="display">No option-links yet</div>
    <p>
      This profile has no configurable-option links to curate. They're created the first
      time you <strong>Apply</strong> the profile's options to a mapped product.
    </p>
    <p style="margin-top:10px;">
      <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">Go to preview &amp; Apply</a>
    </p>
  </div></div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" style="padding:0; overflow:hidden;">
    <input type="hidden" name="action" value="config-exposure-save">
    <input type="hidden" name="id" value="<?= $cb_pid ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>

    <div style="overflow-x:auto;">
      <table class="cb-table" style="min-width:600px;">
        <thead>
          <tr>
            <th>Dimension</th>
            <th>Type</th>
            <th style="text-align:center;">Expose to customer</th>
            <th style="text-align:center;">Hidden</th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($cb_links as $cb_l):
              $cb_dim  = (string) ($cb_l['dimension_key'] ?? '');
              $cb_type = (int) ($cb_l['optiontype'] ?? 0);
          ?>
          <tr>
            <td><code class="mono"><?= $esc($cb_dim) ?></code></td>
            <td><span class="cb-pill grey"><?= $esc($cb_typeLabel[$cb_type] ?? ('type ' . $cb_type)) ?></span></td>
            <td style="text-align:center;">
              <input type="checkbox"
                     name="expose_to_customer[<?= $esc($cb_dim) ?>]"
                     value="1"<?= !empty($cb_l['expose_to_customer']) ? ' checked' : '' ?>>
            </td>
            <td style="text-align:center;">
              <input type="checkbox"
                     name="hidden[<?= $esc($cb_dim) ?>]"
                     value="1"<?= !empty($cb_l['hidden']) ? ' checked' : '' ?>>
            </td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>

    <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; padding:14px 16px; border-top:1px solid var(--border);">
      <button type="submit" class="cb-btn">Save exposure</button>
      <span class="muted" style="font-size:12px;">
        Records intent only — re-run <em>Apply</em> afterwards to push these flags to the live product.
      </span>
    </div>
  </form>
<?php endif; ?>
