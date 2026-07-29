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

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55">Exposure editor</h2>
    <p class="cb-card-sub" data-cb-u="u-1beb43d512">
      Choose which configurable options are exposed to customers for
      <strong><?= $esc($profile['name'] ?? $profile['plan_slug'] ?? ('#' . $cb_pid)) ?></strong>
      (<code class="mono"><?= $esc($profile['plan_slug'] ?? '') ?></code>).
      Saving records your choices — you must re-run <em>Apply</em> to push them to the live product.
    </p>
  </div>
  <div data-cb-u="u-b887bfd543">
    <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">&larr; Back to preview</a>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<div class="cb-card" data-cb-u="u-e9341aa696">
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
    <p data-cb-u="u-726e8b317f">
      <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $cb_pid ?>">Preview &amp; apply</a>
    </p>
  </div></div>
<?php else: ?>
  <form class="cb-card" method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-14f5e9e79f">
    <input type="hidden" name="action" value="config-exposure-save">
    <input type="hidden" name="id" value="<?= $cb_pid ?>">
    <?php if (function_exists('generate_token')) { echo generate_token(); } ?>

    <div data-cb-u="u-fbff25e9fc">
      <table class="cb-table" data-cb-u="u-0292bc7fc0">
        <thead>
          <tr>
            <th>Dimension</th>
            <th>Type</th>
            <th data-cb-u="u-5a2dfef236">Expose to customer</th>
            <th data-cb-u="u-5a2dfef236">Hidden</th>
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
            <td data-cb-u="u-5a2dfef236">
              <input type="checkbox"
                     name="expose_to_customer[<?= $esc($cb_dim) ?>]"
                     value="1"<?= !empty($cb_l['expose_to_customer']) ? ' checked' : '' ?>>
            </td>
            <td data-cb-u="u-5a2dfef236">
              <input type="checkbox"
                     name="hidden[<?= $esc($cb_dim) ?>]"
                     value="1"<?= !empty($cb_l['hidden']) ? ' checked' : '' ?>>
            </td>
          </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>

    <div data-cb-u="u-5651eaf98d">
      <button type="submit" class="cb-btn">Save exposure</button>
      <span class="muted" data-cb-u="u-c42f23b8ec">
        Records intent only — re-run <em>Apply</em> afterwards to push these flags to the live product.
      </span>
    </div>
  </form>
<?php endif; ?>
