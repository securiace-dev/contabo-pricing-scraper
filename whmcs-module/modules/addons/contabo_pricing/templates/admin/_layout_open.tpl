<?php /** @var \Closure $esc */ /** @var string $module_link */ ?>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Instrument+Serif&display=swap');

:root {
  --bg:          #faf7f1;
  --bg-elev:     #fefcf7;
  --panel:       #ffffff;
  --panel-2:     #f5efe2;
  --border:      #e6dfd0;
  --border-soft: #efe9d9;
  --fg:          #1a1d24;
  --fg-strong:   #000;
  --muted:       #5c6373;
  --muted-soft:  #8b91a0;
  --accent:      #b45309;
  --accent-soft: rgba(180,83,9,.10);
  --accent-2:    #0d9488;
  --good:        #15803d;
  --bad:         #b91c1c;
  --warn:        #a16207;
  --chip:        #f0e9d8;
  --row-hover:   #faf7f1;
  --price:       #b45309;
  --shadow-sm:   0 1px 2px rgba(63,42,0,.05), 0 2px 6px -2px rgba(63,42,0,.06);
  --shadow-lg:   0 14px 30px -16px rgba(63,42,0,.18), 0 4px 12px -6px rgba(63,42,0,.08);
  --radius:      10px;
  --radius-sm:   6px;
}

.cb-wrap { font: 14px/1.55 "IBM Plex Sans", -apple-system, BlinkMacSystemFont, sans-serif; color: var(--fg); }
.cb-wrap .mono { font-family: "IBM Plex Mono", ui-monospace, Menlo, monospace; font-variant-numeric: tabular-nums; }
.cb-wrap .display { font-family: "Instrument Serif", Georgia, serif; letter-spacing: -.01em; }

/* status strip — 4 KPI tiles inline at the top of every page */
.cb-strip { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin: 0 0 18px; }
.cb-strip .cb-stat { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 12px 14px; }
.cb-strip .cb-stat .lbl { font-size: 10.5px; color: var(--muted-soft); text-transform: uppercase; letter-spacing: .08em; font-weight: 600; }
.cb-strip .cb-stat .v   { font-family: "IBM Plex Mono", monospace; font-size: 22px; font-weight: 600; color: var(--accent); margin-top: 4px; font-variant-numeric: tabular-nums; }
.cb-strip .cb-stat .sub { font-size: 11px; color: var(--muted); margin-top: 2px; }
.cb-strip .cb-stat.bad  .v { color: var(--bad); }
.cb-strip .cb-stat.warn .v { color: var(--warn); }
.cb-strip .cb-stat.good .v { color: var(--good); }

/* cards / panels */
.cb-card { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px 20px; margin: 14px 0; box-shadow: var(--shadow-sm); }
.cb-card > h3:first-child { margin: 0 0 12px; font-size: 14px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); font-weight: 600; }
.cb-card-title { font-family: "Instrument Serif", serif; font-size: 22px; color: var(--fg-strong); margin: 0 0 6px; }
.cb-card-sub { color: var(--muted); font-size: 13px; margin: 0 0 14px; }

/* tables */
.cb-table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
.cb-table th, .cb-table td { padding: 10px 12px; text-align: left; vertical-align: top; }
.cb-table thead th { background: var(--panel-2); border-bottom: 1px solid var(--border); font-size: 10.5px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); font-weight: 600; cursor: pointer; user-select: none; }
.cb-table thead th[data-sort] .arr { color: var(--accent); margin-left: 4px; font-size: 10px; }
.cb-table tbody tr { border-bottom: 1px solid var(--border-soft); transition: background .12s ease; }
.cb-table tbody tr:hover { background: var(--row-hover); }
.cb-table td .price { font-family: "IBM Plex Mono", monospace; font-weight: 500; color: var(--price); font-variant-numeric: tabular-nums; }
.cb-table td .muted { color: var(--muted); }
.cb-table td.right, .cb-table th.right { text-align: right; }
.cb-table .sparkline { display: block; height: 22px; width: 80px; }

/* badges */
.cb-pill { display: inline-flex; align-items: center; gap: 5px; font-size: 11.5px; padding: 3px 9px; border-radius: 999px; border: 1px solid var(--border); background: var(--chip); color: var(--fg); }
.cb-pill.good { background: rgba(21,128,61,.10); color: var(--good); border-color: rgba(21,128,61,.2); }
.cb-pill.warn { background: rgba(161,98,7,.10); color: var(--warn); border-color: rgba(161,98,7,.2); }
.cb-pill.bad  { background: rgba(185,28,28,.10); color: var(--bad);  border-color: rgba(185,28,28,.2); }
.cb-pill.grey { background: var(--chip); color: var(--muted); }
.cb-pill .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }
/* legacy pill aliases (kept until other templates migrate to good/warn/bad) */
.cb-pill.green  { background: rgba(21,128,61,.10);  color: var(--good); border-color: rgba(21,128,61,.2); }
.cb-pill.yellow { background: rgba(161,98,7,.10);   color: var(--warn); border-color: rgba(161,98,7,.2); }
.cb-pill.red    { background: rgba(185,28,28,.10);  color: var(--bad);  border-color: rgba(185,28,28,.2); }
/* legacy stats grid alias */
.cb-stats { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin: 14px 0; }
.cb-stats .cb-stat { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 12px 14px; }
.cb-stats .cb-stat .lbl { font-size: 10.5px; color: var(--muted-soft); text-transform: uppercase; letter-spacing: .08em; font-weight: 600; }
.cb-stats .cb-stat .v   { font-family: "IBM Plex Mono", monospace; font-size: 22px; font-weight: 600; color: var(--accent); margin-top: 4px; font-variant-numeric: tabular-nums; }

/* filter pills */
.cb-filter-pills { display: flex; flex-wrap: wrap; gap: 6px; }
.cb-filter-pills button { background: var(--chip); border: 1px solid var(--border); color: var(--fg); padding: 5px 12px; border-radius: 999px; font-size: 12.5px; cursor: pointer; transition: all .12s ease; }
.cb-filter-pills button:hover { border-color: var(--accent); }
.cb-filter-pills button[aria-pressed=true] { background: var(--accent); border-color: var(--accent); color: #fff; }

/* segmented control */
.cb-seg { display: inline-flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; background: var(--panel); }
.cb-seg button { background: transparent; color: var(--muted); border: 0; padding: 6px 14px; font-size: 12.5px; cursor: pointer; font-family: inherit; }
.cb-seg button + button { border-left: 1px solid var(--border); }
.cb-seg button:hover { color: var(--fg); }
.cb-seg button[aria-pressed=true] { background: var(--accent); color: #fff; }

/* buttons */
.cb-btn { display: inline-flex; align-items: center; gap: 6px; padding: 7px 14px; background: var(--accent); color: #fff; border: 1px solid var(--accent); border-radius: var(--radius-sm); font-size: 13px; font-family: inherit; font-weight: 500; cursor: pointer; text-decoration: none; transition: all .12s ease; }
.cb-btn:hover { filter: brightness(1.08); color: #fff; text-decoration: none; }
.cb-btn.ghost { background: transparent; color: var(--accent); }
.cb-btn.ghost:hover { background: var(--accent-soft); }
.cb-btn.danger { background: var(--bad); border-color: var(--bad); }
.cb-btn.subtle { background: var(--panel); color: var(--fg); border-color: var(--border); }
.cb-btn.subtle:hover { background: var(--panel-2); }
.cb-btn[disabled], .cb-btn.disabled { opacity: .5; cursor: not-allowed; }

/* form fields */
.cb-field { display: flex; flex-direction: column; gap: 4px; margin: 10px 0; }
.cb-field label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); font-weight: 600; }
.cb-field input[type=text], .cb-field input[type=password], .cb-field input[type=number], .cb-field select, .cb-field textarea {
  background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 8px 10px; font: inherit; font-size: 13.5px; color: var(--fg); transition: border-color .12s ease;
}
.cb-field input:focus, .cb-field select:focus, .cb-field textarea:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-soft); }

/* search input */
.cb-search { position: relative; display: inline-block; }
.cb-search input { width: 280px; padding: 7px 12px 7px 32px; background: var(--panel); border: 1px solid var(--border); border-radius: 999px; font: inherit; font-size: 13px; }
.cb-search::before { content: "\2384"; position: absolute; left: 11px; top: 50%; transform: translateY(-50%); color: var(--muted); font-size: 14px; }

/* toolbar */
.cb-toolbar { display: flex; flex-wrap: wrap; gap: 10px 16px; align-items: center; margin: 0 0 14px; padding: 12px 16px; background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius-sm); }
.cb-toolbar .glabel { font-size: 10.5px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted-soft); font-weight: 600; }
.cb-toolbar .spacer { flex: 1; }
.cb-toolbar .count { font-size: 12px; color: var(--muted); }

/* flash + error */
.cb-flash { padding: 10px 14px; background: rgba(21,128,61,.10); border: 1px solid rgba(21,128,61,.3); border-radius: var(--radius-sm); margin: 10px 0; color: var(--good); font-size: 13.5px; }
.cb-error { padding: 10px 14px; background: rgba(185,28,28,.10); border: 1px solid rgba(185,28,28,.3); border-radius: var(--radius-sm); margin: 10px 0; color: var(--bad); font-size: 13.5px; }

/* empty state */
.cb-empty { padding: 40px 24px; text-align: center; color: var(--muted); }
.cb-empty .display { font-size: 18px; color: var(--fg); margin-bottom: 4px; }

/* drawer (right-side slide-in for diff / detail) */
.cb-drawer { position: fixed; top: 0; right: 0; bottom: 0; width: min(640px, 90vw); background: var(--panel); border-left: 2px solid var(--accent); transform: translateX(105%); transition: transform .22s cubic-bezier(.2,.8,.2,1); box-shadow: var(--shadow-lg); padding: 22px 24px; overflow-y: auto; z-index: 9000; }
.cb-drawer.open { transform: translateX(0); }
.cb-drawer .close { position: absolute; top: 14px; right: 14px; }

/* modal */
.cb-modal { position: fixed; inset: 0; background: rgba(0,0,0,.4); display: none; align-items: flex-start; justify-content: center; z-index: 9100; padding: 60px 16px; overflow-y: auto; backdrop-filter: blur(2px); }
.cb-modal.open { display: flex; }
.cb-modal .sheet { background: var(--panel); border-radius: var(--radius); width: 100%; max-width: 600px; padding: 24px; box-shadow: var(--shadow-lg); }
.cb-modal .sheet h3 { font-family: "Instrument Serif", serif; font-size: 22px; margin: 0 0 14px; color: var(--fg-strong); }

/* keyboard shortcut display */
.kbd { display: inline-block; padding: 1px 6px; background: var(--chip); border: 1px solid var(--border); border-radius: 4px; font-size: 10.5px; font-family: "IBM Plex Mono", monospace; color: var(--muted); }

/* toast feedback for AJAX actions */
.cb-toast { position: fixed; top: 16px; right: 16px; z-index: 9200; padding: 8px 14px; border-radius: var(--radius-sm); box-shadow: var(--shadow-lg); animation: cbToastIn .2s ease; }
@keyframes cbToastIn { from { opacity:0; transform: translateY(-4px); } to { opacity:1; transform: translateY(0); } }
</style>
<?php
/**
 * Asset URL note: WHMCS admin URLs may be served under a custom admin slug
 * (e.g. `/shriram/addonmodules.php`), but module static files live at
 * `<host>/modules/addons/...` — i.e. the WHMCS install root, NOT under the
 * slug. Use a host-absolute path so the script loads regardless of the
 * admin slug configuration.
 */
$cb_assets_url = '/modules/addons/contabo_pricing/assets/app.js?v='
    . rawurlencode(isset($cb_addon_version) ? (string) $cb_addon_version : '0.2.0');
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
