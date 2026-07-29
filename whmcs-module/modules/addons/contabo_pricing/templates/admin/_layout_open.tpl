<?php /** @var \Closure $esc */ /** @var string $module_link */ ?>
<link rel="stylesheet"
      href="/modules/addons/contabo_pricing/assets/app.css?v=<?= $esc(rawurlencode(isset($cb_addon_version) ? (string) $cb_addon_version : '1.0.0')) ?>">
<?php
/**
 * Asset URL note: WHMCS admin URLs may be served under a custom admin slug
 * (e.g. `/shriram/addonmodules.php`), but module static files live at
 * `<host>/modules/addons/...` — i.e. the WHMCS install root, NOT under the
 * slug. Use a host-absolute path so the script loads regardless of the
 * admin slug configuration.
 */
$cb_assets_url = '/modules/addons/contabo_pricing/assets/app.js?v='
    . rawurlencode(isset($cb_addon_version) ? (string) $cb_addon_version : '1.0.0');
?>
<script src="<?= $esc($cb_assets_url) ?>" defer></script>
<div class="cb-wrap">
<?php if (!empty($cb_strip_data) && is_array($cb_strip_data)): ?>
  <div class="cb-strip">
    <?php foreach ($cb_strip_data as $cb_tile): ?>
      <?php
        $cb_tone = isset($cb_tile['tone']) ? (string) $cb_tile['tone'] : '';
        $cb_tone_class = in_array($cb_tone, array('good', 'warn', 'bad'), true) ? ' ' . $cb_tone : '';
      ?>
      <div class="cb-stat<?= $cb_tone_class ?>">
        <div class="lbl"><?= $esc(isset($cb_tile['lbl']) ? (string) $cb_tile['lbl'] : '') ?></div>
        <div class="v"><?= $esc(isset($cb_tile['v']) ? (string) $cb_tile['v'] : '—') ?></div>
        <?php if (!empty($cb_tile['sub'])): ?>
          <div class="sub"><?= $esc((string) $cb_tile['sub']) ?></div>
        <?php endif; ?>
      </div>
    <?php endforeach; ?>
  </div>
<?php endif; ?>
