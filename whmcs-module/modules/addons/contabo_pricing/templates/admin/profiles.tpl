<?php require __DIR__ . '/_layout_open.tpl'; ?>

<?php
/**
 * Profiles admin page
 *
 * Expected data:
 *   $profiles         — list of profile rows
 *   $available_plans  — list of API plans (product_slug, product_name, family)
 *   $flash            — optional success message
 *   $cb_profile_conflict — optional duplicate-conflict payload (see below).
 *                          When present, the conflict chooser is shown.
 *                          Shape (set by AdminController::profileCreate on a
 *                          'conflict' status):
 *                            [
 *                              'slug'              => string,  attempted slug
 *                              'existing_id'       => int,     existing profile id
 *                              'existing_name'     => string,
 *                              'suffix_suggestion' => string,  free '-N' slug
 *                              'submit'            => array,   original POST
 *                                  fields to re-submit (name, plan_slug,
 *                                  period_months, region, os, options,
 *                                  tags, sync_strategy)
 *                            ]
 *   $module_link      — base URL
 *   $esc              — escaping closure
 */
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;">Profiles</h2>
    <p class="cb-card-sub" style="margin:0; max-width:62ch;">
      Versioned pricing templates that map Contabo plans to your WHMCS products.
    </p>
  </div>
  <div>
    <button type="button" class="cb-btn" data-cb-open-modal="profile-create">+ New profile</button>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash"><?= $esc($flash) ?></div>
<?php endif; ?>

<!-- ───────────────────── Duplicate-conflict chooser ───────────────────── -->
<?php
/**
 * Shown only when AdminController::profileCreate() detected a slug collision
 * with a DIFFERENT configuration (status 'conflict'). The admin picks one of
 * four resolutions. Hidden entirely when $cb_profile_conflict is absent.
 */
if (!empty($cb_profile_conflict) && is_array($cb_profile_conflict)):
    $cf        = $cb_profile_conflict;
    $cfSlug    = (string) ($cf['slug'] ?? '');
    $cfExId    = (int) ($cf['existing_id'] ?? 0);
    $cfExName  = (string) ($cf['existing_name'] ?? $cfSlug);
    $cfSuffix  = (string) ($cf['suffix_suggestion'] ?? '');
    $cfSubmit  = is_array($cf['submit'] ?? null) ? $cf['submit'] : [];
?>
  <div class="cb-card cb-conflict" data-cb-profile-conflict
       style="border-color:var(--warn,#caa45a); background:#fdf8ef; padding:16px 18px; margin-bottom:18px;">
    <div style="display:flex; align-items:flex-start; gap:12px;">
      <span class="cb-pill warn" style="margin-top:2px;">conflict</span>
      <div style="flex:1 1 auto;">
        <h3 class="display" style="margin:0 0 4px; font-size:18px;">A profile with this slug already exists</h3>
        <p class="cb-card-sub" style="margin:0 0 12px; max-width:64ch;">
          The slug <code class="mono"><?= $esc($cfSlug) ?></code> is already taken by
          <strong><?= $esc($cfExName) ?></strong> (profile #<?= $cfExId ?>), but the
          configuration you just submitted is <em>different</em>. Nothing was changed.
          Choose how to proceed:
        </p>

        <div style="display:flex; flex-wrap:wrap; gap:8px;">
          <!-- (1) Open existing -->
          <a class="cb-btn subtle"
             href="<?= $esc($module_link) ?>&amp;action=profile-diff&amp;id=<?= $cfExId ?>">
            Open existing
          </a>

          <!-- (2) Create duplicate with numeric suffix -->
          <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
            <input type="hidden" name="action" value="profile-create">
            <input type="hidden" name="slug" value="<?= $esc($cfSuffix) ?>">
            <?php foreach ($cfSubmit as $fk => $fv): if ($fk === 'slug' || $fk === 'action') { continue; } ?>
              <input type="hidden" name="<?= $esc((string) $fk) ?>" value="<?= $esc(is_array($fv) ? (string) json_encode($fv) : (string) $fv) ?>">
            <?php endforeach; ?>
            <?= generate_token() ?>
            <button type="submit" class="cb-btn">
              Create duplicate as <code class="mono"><?= $esc($cfSuffix) ?></code>
            </button>
          </form>

          <!-- (3) Update existing -->
          <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
            <input type="hidden" name="action" value="profile-save">
            <input type="hidden" name="id" value="<?= $cfExId ?>">
            <?php foreach ($cfSubmit as $fk => $fv): if ($fk === 'slug' || $fk === 'action' || $fk === 'id') { continue; } ?>
              <input type="hidden" name="<?= $esc((string) $fk) ?>" value="<?= $esc(is_array($fv) ? (string) json_encode($fv) : (string) $fv) ?>">
            <?php endforeach; ?>
            <?= generate_token() ?>
            <button type="submit" class="cb-btn ghost">Update existing #<?= $cfExId ?></button>
          </form>

          <!-- (4) Cancel -->
          <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">Cancel</a>
        </div>
      </div>
    </div>
  </div>
<?php endif; ?>

<!-- ───────────────────── Toolbar ───────────────────── -->
<div class="cb-toolbar" role="toolbar" aria-label="Profile filters">
  <div class="cb-filter-pills" role="group" aria-label="Status filter">
    <span class="glabel">Show</span>
    <button type="button" data-cb-filter="all"      aria-pressed="true">All</button>
    <button type="button" data-cb-filter="active"   aria-pressed="false">Active</button>
    <button type="button" data-cb-filter="drifted"  aria-pressed="false">Drifted</button>
    <button type="button" data-cb-filter="inactive" aria-pressed="false">Inactive</button>
  </div>

  <span class="glabel">Search</span>
  <div class="cb-search" role="search">
    <input data-cb-search type="text" placeholder="Search profiles…" autocomplete="off">
  </div>

  <span class="glabel">Sort</span>
  <div class="cb-seg" role="group" aria-label="Sort by">
    <button type="button" data-cb-sort="name"        aria-pressed="true">Name</button>
    <button type="button" data-cb-sort="updated_at"  aria-pressed="false">Recently changed</button>
    <button type="button" data-cb-sort="plan_slug"   aria-pressed="false">Plan slug</button>
  </div>

  <div style="flex:1 1 auto"></div>

  <div class="cb-pill grey" data-cb-row-count title="Visible rows">
    <span data-cb-visible-count><?= (int) count($profiles) ?></span> / <?= (int) count($profiles) ?>
  </div>
</div>

<!-- ───────────────────── Bulk-action bar ───────────────────── -->
<div class="cb-card cb-bulk-bar" data-cb-bulk-toolbar hidden style="align-items:center; gap:10px; padding:10px 14px;">
  <strong><span data-cb-bulk-count>0</span> selected</strong>
  <span style="opacity:.5">·</span>
  <button type="button" class="cb-btn subtle" data-cb-bulk-action="apply-latest">Apply latest version</button>
  <button type="button" class="cb-btn ghost"  data-cb-bulk-action="disable">Disable</button>
  <div style="flex:1 1 auto"></div>
  <button type="button" class="cb-btn ghost" data-cb-bulk-cancel>Cancel</button>
</div>

<!-- ───────────────────── Profiles table ───────────────────── -->
<?php if (empty($profiles)): ?>
  <div class="cb-empty">
    <div class="display" style="font-size:22px; margin-bottom:8px;">No profiles yet</div>
    <p class="cb-card-sub" style="margin:0 0 14px;">Create your first profile to start tracking Contabo pricing for a WHMCS product.</p>
    <button type="button" class="cb-btn" data-cb-open-modal="profile-create">+ New profile</button>
  </div>
<?php else: ?>
  <div class="cb-card" style="padding:0; overflow:hidden;">
    <table class="cb-table" data-cb-table="profiles">
      <thead>
        <tr>
          <th style="width:32px"><input type="checkbox" data-cb-bulk-all aria-label="Select all"></th>
          <th data-cb-sort="name">Plan name <span class="arr"></span></th>
          <th data-cb-sort="plan_slug">Plan slug <span class="arr"></span></th>
          <th data-cb-sort="period_months">Period <span class="arr"></span></th>
          <th>Region / OS</th>
          <th data-cb-sort="sync_strategy">Strategy <span class="arr"></span></th>
          <th data-cb-sort="active">Status <span class="arr"></span></th>
          <th data-cb-sort="latest_version_id">Latest version <span class="arr"></span></th>
          <th style="width:120px">Trend</th>
          <th style="width:160px; text-align:right">Actions</th>
        </tr>
      </thead>
      <tbody>
        <?php foreach ($profiles as $p):
          $pid     = (int) ($p['id'] ?? 0);
          $active  = (bool) ($p['active'] ?? false);
          $drifted = !empty($p['drifted']) ? 1 : 0; // controller may set; otherwise JS computes
          $latest  = isset($p['latest_version_id']) ? (string) $p['latest_version_id'] : '';
          $name    = (string) ($p['name'] ?? '');
          $slug    = (string) ($p['slug'] ?? '');
          $plan    = (string) ($p['plan_slug'] ?? '');
          $period  = (int)    ($p['period_months'] ?? 0);
          $region  = trim((string) ($p['region'] ?? ''));
          $os      = trim((string) ($p['os'] ?? ''));
          $strat   = (string) ($p['sync_strategy'] ?? '');
          $updated = (string) ($p['updated_at'] ?? '');
          $stratPill = ($strat === 'auto-apply') ? 'warn' : (($strat === 'manual') ? 'grey' : 'good');
        ?>
        <tr
          data-cb-profile-row
          data-cb-profile-id="<?= $pid ?>"
          data-cb-profile-name="<?= $esc($slug) ?>"
          data-cb-active="<?= $active ? 1 : 0 ?>"
          data-cb-drifted="<?= $drifted ?>"
          data-cb-open-drawer="profile"
          data-cb-search-haystack="<?= $esc(strtolower($slug . ' ' . $name . ' ' . $plan . ' ' . $region . ' ' . $os)) ?>"
          data-cb-sort-name="<?= $esc(strtolower($name)) ?>"
          data-cb-sort-plan_slug="<?= $esc(strtolower($plan)) ?>"
          data-cb-sort-period_months="<?= $period ?>"
          data-cb-sort-sync_strategy="<?= $esc($strat) ?>"
          data-cb-sort-active="<?= $active ? 1 : 0 ?>"
          data-cb-sort-latest_version_id="<?= $esc($latest) ?>"
          data-cb-sort-updated_at="<?= $esc($updated) ?>"
        >
          <td data-cb-stop>
            <input type="checkbox" data-cb-bulk value="<?= $pid ?>" aria-label="Select profile <?= $esc($slug) ?>">
          </td>
          <td>
            <div style="font-weight:600"><?= $esc($name !== '' ? $name : $slug) ?></div>
            <div class="muted mono" style="font-size:11px; opacity:.7"><?= $esc($slug) ?></div>
          </td>
          <td class="mono"><?= $esc($plan) ?></td>
          <td><?= $period ?> mo</td>
          <td>
            <?= $region !== '' ? $esc($region) : '<span class="muted">—</span>' ?>
            <?php if ($os !== ''): ?>
              <div class="muted" style="font-size:11px"><?= $esc($os) ?></div>
            <?php endif; ?>
          </td>
          <td><span class="cb-pill <?= $esc($stratPill) ?>"><?= $esc($strat) ?></span></td>
          <td>
            <?php if ($active): ?>
              <span class="cb-pill good">active</span>
            <?php else: ?>
              <span class="cb-pill bad">inactive</span>
            <?php endif; ?>
            <?php if ($drifted): ?>
              <span class="cb-pill warn" title="Latest version differs from API">drifted</span>
            <?php endif; ?>
          </td>
          <td class="mono"><?= $esc($latest !== '' ? ('v' . $latest) : '—') ?></td>
          <td>
            <svg class="sparkline" data-cb-sparkline data-cb-profile-id="<?= $pid ?>" width="100" height="22" viewBox="0 0 100 22" preserveAspectRatio="none" aria-hidden="true"></svg>
          </td>
          <td style="text-align:right" data-cb-stop>
            <button type="button" class="cb-btn subtle"
                    data-cb-open-modal="profile-create"
                    data-cb-profile-edit-id="<?= $pid ?>">Edit</button>
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profile-diff&amp;id=<?= $pid ?>">History</a>
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $pid ?>" title="Preview the WHMCS configurable options this profile would create">Config preview</a>
            <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
              <input type="hidden" name="action" value="profile-toggle">
              <input type="hidden" name="id" value="<?= $pid ?>">
              <?= generate_token() ?>
              <button class="cb-btn subtle" type="submit"><?= $active ? 'Disable' : 'Enable' ?></button>
            </form>
          </td>
        </tr>
        <?php endforeach; ?>
      </tbody>
    </table>
  </div>
<?php endif; ?>

<!-- ───────────────────── Modal: create / edit profile ───────────────────── -->
<!--
  One modal handles both create and edit. The Edit buttons in the table flip
  the form's hidden action to `profile-save` and inject a hidden id. The
  configurator block hydrates from /ajax-configurator?plan_slug=… and
  populates per-dimension <select>s. Each select carries its option metadata
  (monthly + setup deltas) inline so JS can recompute totals without
  re-fetching the configurator.
-->
<style>
  .cb-cfg-grid { display:grid; grid-template-columns: minmax(120px, 0.8fr) 1.4fr; gap:8px 14px; align-items:center; margin-top:8px; }
  .cb-cfg-grid label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); font-weight: 600; }
  .cb-cfg-grid select { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 6px 8px; font: inherit; font-size: 12.5px; color: var(--fg); }
  .cb-osum { margin-top: 12px; padding: 10px 12px; background: #faf6f1; border: 1px solid var(--border); border-radius: var(--radius-sm); font-size: 13px; }
  .cb-osum .ln { display:flex; justify-content:space-between; gap:10px; padding: 3px 0; }
  .cb-osum .ln.muted { color: var(--muted); }
  .cb-osum .ln.chg .v { color: var(--bad); font-family: "IBM Plex Mono", monospace; }
  .cb-osum .ln.chg.up .v { color: var(--good); }
  .cb-osum .ln.tot { border-top: 1px dashed var(--border); margin-top: 4px; padding-top: 6px; font-weight: 600; }
  .cb-osum .ln.tot .v { color: var(--price); font-family: "IBM Plex Mono", monospace; }
  .cb-osum .sel { margin-top: 6px; padding-top: 6px; border-top: 1px solid var(--border-soft); font-size: 11.5px; color: var(--muted); }
  .cb-cfg-empty { padding: 14px; text-align: center; color: var(--muted); font-size: 12.5px; background: #faf6f1; border: 1px dashed var(--border); border-radius: var(--radius-sm); }
</style>
<div class="cb-modal" id="cb-modal-profile-create" hidden>
  <div class="sheet" role="dialog" aria-labelledby="cb-modal-profile-create-title" aria-modal="true" style="max-width:780px;">
    <header style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
      <h3 id="cb-modal-profile-create-title" class="display" style="margin:0" data-cb-modal-title>Create profile</h3>
      <button type="button" class="cb-btn ghost" data-cb-close-modal aria-label="Close">×</button>
    </header>

    <form method="post" action="<?= $esc($module_link) ?>" data-cb-form="profile-create" data-cb-configurator-form>
      <input type="hidden" name="action" value="profile-create" data-cb-form-action>
      <input type="hidden" name="id" value="" data-cb-form-id disabled>
      <?= generate_token() ?>

      <div class="cb-field">
        <label for="cb-pc-name">Display name</label>
        <input id="cb-pc-name" type="text" name="name" required placeholder="e.g. Cloud VPS 10 — EU — Ubuntu 24 — 12 mo">
      </div>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
        <div class="cb-field">
          <label for="cb-pc-plan">Plan</label>
          <select id="cb-pc-plan" name="plan_slug" required data-cb-quote-plan data-cb-cfg-plan>
            <option value="">— pick a plan —</option>
            <?php foreach ($available_plans as $ap): ?>
              <option value="<?= $esc($ap['product_slug']) ?>"><?= $esc($ap['product_name']) ?> (<?= $esc($ap['family']) ?>)</option>
            <?php endforeach; ?>
          </select>
        </div>

        <div class="cb-field">
          <label for="cb-pc-period">Period</label>
          <select id="cb-pc-period" name="period_months" data-cb-quote-period data-cb-cfg-period>
            <option value="1">1 month</option>
            <option value="3">3 months</option>
            <option value="6" selected>6 months</option>
            <option value="12">12 months</option>
          </select>
        </div>
      </div>

      <!-- Configurator: hydrated by assets/app.js once plan_slug + period are set. -->
      <div class="cb-card" style="margin-top:14px; padding:14px 16px;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div class="cb-card-sub" style="margin:0;">Configure &amp; price</div>
            <div class="muted" style="font-size:11px">Per-dimension dropdowns mirror the upstream Contabo order page.</div>
          </div>
          <button type="button" class="cb-btn ghost" data-cb-cfg-reset style="font-size:12px;padding:4px 10px;" hidden>Reset defaults</button>
        </div>
        <div data-cb-configurator>
          <div class="cb-cfg-empty">Pick a plan to load configuration options…</div>
        </div>
        <div data-cb-summary class="cb-osum" hidden></div>
      </div>

      <!-- Configurator selections (JSON) — populated by JS on every change. -->
      <input type="hidden" name="options" value="" data-cb-options-json>
      <!-- Legacy hidden region/os overrides — derived server-side from Image:OS + Region. -->
      <input type="hidden" name="region" value="" data-cb-region-fallback>
      <input type="hidden" name="os"     value="" data-cb-os-fallback>

      <div class="cb-field">
        <label for="cb-pc-strategy">Sync strategy</label>
        <select id="cb-pc-strategy" name="sync_strategy">
          <option value="manual">manual — admin reviews diffs in UI</option>
          <option value="notify" selected>notify — email admin on drift</option>
          <option value="auto-apply">auto-apply — push price updates immediately</option>
        </select>
      </div>

      <div class="cb-field">
        <label for="cb-pc-tags">Tags <span class="muted">(comma-separated)</span></label>
        <input id="cb-pc-tags" type="text" name="tags" placeholder="production, billing-team">
      </div>

      <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:14px;">
        <button type="button" class="cb-btn subtle" data-cb-close-modal>Cancel</button>
        <button type="submit" class="cb-btn" data-cb-submit-label>Create profile</button>
      </div>
    </form>
  </div>
</div>

<!-- ───────────────────── Drawer: profile detail ───────────────────── -->
<div class="cb-drawer" id="cb-drawer-profile" hidden aria-hidden="true" role="dialog" aria-labelledby="cb-drawer-profile-title">
  <header style="display:flex; justify-content:space-between; align-items:flex-start; gap:12px;">
    <h3 id="cb-drawer-profile-title" class="display" data-cb-drawer-title style="margin:0">—</h3>
    <button type="button" class="cb-btn ghost close" data-cb-close-drawer aria-label="Close drawer">Close</button>
  </header>
  <div data-cb-drawer-body style="margin-top:14px">
    <p class="muted">Loading…</p>
  </div>
</div>

</div>
