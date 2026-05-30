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
// Normalise optional vars so both the normal + trash render paths are safe
// under extract(EXTR_SKIP) (undefined template vars would otherwise warn).
$trash_mode  = isset($trash_mode) ? (bool) $trash_mode : false;
$trashed     = isset($trashed) && is_array($trashed) ? $trashed : [];
$trash_count = isset($trash_count) ? (int) $trash_count : 0;
$undo_id     = isset($undo_id) ? (int) $undo_id : 0;
$cb_purge_phrase = isset($cb_purge_phrase) ? (string) $cb_purge_phrase : '';
?>

<header style="display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin:6px 0 18px;">
  <div>
    <h2 class="display" style="margin:0 0 4px;"><?= $trash_mode ? 'Profiles — Trash' : 'Profiles' ?></h2>
    <p class="cb-card-sub" style="margin:0; max-width:62ch;">
      <?= $trash_mode
        ? 'Soft-deleted profiles. Restore brings one back; permanent purge is guarded and irreversible.'
        : 'Versioned pricing templates that map Contabo plans to your WHMCS products.' ?>
    </p>
  </div>
  <div style="display:flex; gap:8px; align-items:center;">
    <?php if ($trash_mode): ?>
      <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles">← Back to profiles</a>
    <?php else: ?>
      <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profiles-trash" title="Soft-deleted profiles">
        Trash<?= $trash_count > 0 ? ' (' . $trash_count . ')' : '' ?>
      </a>
      <button type="button" class="cb-btn" data-cb-open-modal="profile-create">+ New profile</button>
    <?php endif; ?>
  </div>
</header>

<?php if (!empty($flash)): ?>
  <div class="cb-flash" style="display:flex; align-items:center; gap:12px;">
    <span style="flex:1 1 auto"><?= $esc($flash) ?></span>
    <?php if ($undo_id > 0): ?>
      <form method="post" action="<?= $esc($module_link) ?>" style="margin:0">
        <input type="hidden" name="action" value="profile-restore">
        <input type="hidden" name="id" value="<?= $undo_id ?>">
        <?= generate_token() ?>
        <button type="submit" class="cb-btn subtle" style="white-space:nowrap">↩ Undo</button>
      </form>
    <?php endif; ?>
  </div>
<?php endif; ?>

<?php /* ───────────────────── Trash view ───────────────────── */ ?>
<?php if ($trash_mode): ?>
  <?php if (empty($trashed)): ?>
    <div class="cb-empty">
      <div class="display" style="font-size:22px; margin-bottom:8px;">Trash is empty</div>
      <p class="cb-card-sub" style="margin:0 0 14px;">Deleted profiles will appear here, where you can restore or permanently purge them.</p>
      <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=profiles">Back to profiles</a>
    </div>
  <?php else: ?>
    <div class="cb-card" style="padding:0; overflow:hidden;">
      <table class="cb-table">
        <thead>
          <tr>
            <th>Profile</th>
            <th>Plan slug</th>
            <th>Deleted</th>
            <th>Purge eligibility</th>
            <th style="text-align:right; width:260px">Actions</th>
          </tr>
        </thead>
        <tbody>
          <?php foreach ($trashed as $tp):
            $tpid   = (int) ($tp['id'] ?? 0);
            $tname  = (string) ($tp['name'] ?? $tp['slug'] ?? ('#' . $tpid));
            $tslug  = (string) ($tp['slug'] ?? '');
            $tdel   = (string) ($tp['deleted_at'] ?? '');
            $assess = is_array($tp['purge'] ?? null) ? $tp['purge'] : ['allowed' => false, 'reasons' => ['unknown']];
            $allowed = !empty($assess['allowed']);
          ?>
            <tr>
              <td>
                <div style="font-weight:600"><?= $esc($tname) ?></div>
                <div class="muted mono" style="font-size:11px; opacity:.7"><?= $esc($tslug) ?></div>
              </td>
              <td class="mono"><?= $esc((string) ($tp['plan_slug'] ?? '')) ?></td>
              <td class="muted" style="font-size:12px"><?= $esc($tdel) ?></td>
              <td>
                <?php if ($allowed): ?>
                  <span class="cb-pill good">eligible</span>
                <?php else: ?>
                  <span class="cb-pill warn" title="<?= $esc(implode(' ', (array) ($assess['reasons'] ?? []))) ?>">blocked</span>
                  <div class="muted" style="font-size:11px; margin-top:2px;"><?= $esc(implode(' ', (array) ($assess['reasons'] ?? []))) ?></div>
                <?php endif; ?>
              </td>
              <td style="text-align:right">
                <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
                  <input type="hidden" name="action" value="profile-restore">
                  <input type="hidden" name="id" value="<?= $tpid ?>">
                  <?= generate_token() ?>
                  <button type="submit" class="cb-btn subtle">Restore</button>
                </form>
                <button type="button" class="cb-btn ghost"
                        data-cb-purge-open="<?= $tpid ?>"
                        <?= $allowed ? '' : 'disabled title="Resolve the blockers above first"' ?>
                        style="color:var(--bad,#b3261e)">Purge…</button>
                <?php if ($allowed): ?>
                  <div data-cb-purge-form="<?= $tpid ?>" hidden style="margin-top:8px; text-align:left; background:#fdf3f2; border:1px solid #e7b9b3; border-radius:8px; padding:10px;">
                    <form method="post" action="<?= $esc($module_link) ?>" style="margin:0"
                          onsubmit="return confirm('Permanently purge &quot;<?= $esc($tname) ?>&quot; and everything it owns? This cannot be undone.');">
                      <input type="hidden" name="action" value="profile-purge">
                      <input type="hidden" name="id" value="<?= $tpid ?>">
                      <?= generate_token() ?>
                      <label style="display:block; font-size:11.5px; margin-bottom:4px;">
                        Type <code class="mono"><?= $esc($cb_purge_phrase) ?></code> to confirm:
                      </label>
                      <input type="text" name="purge_confirmation_phrase" autocomplete="off"
                             placeholder="<?= $esc($cb_purge_phrase) ?>"
                             style="width:100%; padding:5px 8px; font-size:12px; margin-bottom:6px;">
                      <button type="submit" class="cb-btn" style="background:var(--bad,#b3261e); border-color:var(--bad,#b3261e); color:#fff;">Purge permanently</button>
                    </form>
                  </div>
                <?php endif; ?>
              </td>
            </tr>
          <?php endforeach; ?>
        </tbody>
      </table>
    </div>
  <?php endif; ?>

  <script>
  (function () {
    document.querySelectorAll('[data-cb-purge-open]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var id = btn.getAttribute('data-cb-purge-open');
        var box = document.querySelector('[data-cb-purge-form="' + id + '"]');
        if (box) { box.hidden = !box.hidden; }
      });
    });
  })();
  </script>

  </div><?php /* close .cb-wrap opened by _layout_open */ return; ?>
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
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-exposure&amp;id=<?= $pid ?>" title="Curate which configurable options are exposed to customers">Exposure</a>
            <form method="post" action="<?= $esc($module_link) ?>" style="display:inline">
              <input type="hidden" name="action" value="profile-toggle">
              <input type="hidden" name="id" value="<?= $pid ?>">
              <?= generate_token() ?>
              <button class="cb-btn subtle" type="submit"><?= $active ? 'Disable' : 'Enable' ?></button>
            </form>
            <form method="post" action="<?= $esc($module_link) ?>" style="display:inline"
                  onsubmit="return confirm('Move &quot;<?= $esc($name !== '' ? $name : $slug) ?>&quot; to Trash? You can restore it afterwards.');">
              <input type="hidden" name="action" value="profile-delete">
              <input type="hidden" name="id" value="<?= $pid ?>">
              <?= generate_token() ?>
              <button class="cb-btn ghost" type="submit" style="color:var(--bad,#b3261e)" title="Move to Trash (recoverable)">Delete</button>
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

      <div class="cb-field">
        <label for="cb-pc-mode">Profile mode</label>
        <select id="cb-pc-mode" name="profile_mode" data-cb-profile-mode>
          <option value="fixed_admin_profile" selected>Fixed admin profile — admin locks every build option; customer cannot choose</option>
          <option value="customer_configurable_product">Customer-configurable product — admin exposes options; customer picks at order time</option>
        </select>
        <div class="muted" style="font-size:11px; margin-top:4px;" data-cb-mode-hint>
          Pre-packaged plan: pick a value for every option below — that locked set becomes the SKU. Customers cannot change it.
        </div>
      </div>

      <div class="cb-field">
        <label for="cb-pc-plan">Plan</label>
        <select id="cb-pc-plan" name="plan_slug" required data-cb-quote-plan data-cb-cfg-plan>
          <option value="">— pick a plan —</option>
          <?php foreach ($available_plans as $ap): ?>
            <option value="<?= $esc($ap['product_slug']) ?>"><?= $esc($ap['product_name']) ?> (<?= $esc($ap['family']) ?>)</option>
          <?php endforeach; ?>
        </select>
      </div>

      <?php
        // Publish cycles — which billing cycles this profile SOURCES a price for.
        // The PROFILE is the source authority (all six can be derived: 1/3/6/12
        // scraped, 24/36 projected from the 12-mo rate). The MAPPING later narrows
        // these to what the customer actually sees at checkout. JS maintains the
        // hidden published_cycles_mask + a per-cycle source-price preview.
        $cb_publish_cycles = [
            ['cycle' => 'Monthly',       'months' => 1,  'bit' => 1],
            ['cycle' => 'Quarterly',     'months' => 3,  'bit' => 2],
            ['cycle' => 'Semi-Annually', 'months' => 6,  'bit' => 4],
            ['cycle' => 'Annually',      'months' => 12, 'bit' => 8],
            ['cycle' => 'Biennially',    'months' => 24, 'bit' => 16],
            ['cycle' => 'Triennially',   'months' => 36, 'bit' => 32],
        ];
      ?>
      <div class="cb-field">
        <label>Publish cycles <span class="muted" style="text-transform:none; letter-spacing:0; font-weight:400;">— which terms this profile sources</span></label>
        <!-- The single period dropdown is gone: the profile now sources a price
             per published cycle. period_months is derived server-side from the
             longest published cycle (kept for slug + identity). -->
        <input type="hidden" name="published_cycles_mask" value="63" data-cb-published-mask>
        <div data-cb-publish-cycles style="display:grid; grid-template-columns:repeat(3, 1fr); gap:6px 12px; margin-top:6px;">
          <?php foreach ($cb_publish_cycles as $pc): ?>
            <label style="display:flex; align-items:baseline; gap:6px; font-size:12.5px; cursor:pointer;">
              <input type="checkbox" data-cb-publish-cycle data-cb-bit="<?= (int) $pc['bit'] ?>" data-cb-months="<?= (int) $pc['months'] ?>" checked>
              <span>
                <?= $esc($pc['cycle']) ?>
                <?php if ((int) $pc['months'] >= 24): ?><span class="cb-pill grey" title="Not sold publicly by Contabo — projected from the 12-month rate" style="font-size:9px; padding:1px 5px;">projected</span><?php endif; ?>
                <span class="muted mono" data-cb-cycle-source="<?= (int) $pc['months'] ?>" style="display:block; font-size:11px; opacity:.75;">—</span>
              </span>
            </label>
          <?php endforeach; ?>
        </div>
        <div class="muted" style="font-size:11px; margin-top:6px;">
          Source (cost) price per cycle, in EUR/mo, shown once a plan loads. The
          <strong>mapping</strong> decides which of these the customer sees and at what markup.
        </div>
      </div>

      <!-- Hidden period kept ONLY to drive the configurator's dimension-delta
           preview (app.js reads data-cb-cfg-period). The real period is derived
           server-side from the published cycles. -->
      <input type="hidden" value="12" data-cb-cfg-period data-cb-quote-period>

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

      <!-- Configurator selections (JSON) — populated by JS on every change.
           OS + Region are derived server-side from these selections
           (AdminController::deriveOsRegionFromSelections) — the single source of
           truth, so no separate hidden region/os inputs are needed. -->
      <input type="hidden" name="options" value="" data-cb-options-json>

      <div class="cb-field">
        <label for="cb-pc-strategy">Sync strategy</label>
        <select id="cb-pc-strategy" name="sync_strategy">
          <option value="manual">manual — admin reviews diffs in UI</option>
          <option value="notify" selected>notify — email admin on drift</option>
          <option value="auto-apply">auto-apply — push price updates immediately</option>
        </select>
      </div>

      <div class="cb-field" data-cb-expose-field>
        <label style="display:flex; align-items:center; gap:8px; text-transform:none; letter-spacing:0; font-weight:600;">
          <!-- unchecked checkboxes don't POST; the hidden 0 is the fallback the checkbox overrides -->
          <input type="hidden" name="expose_configurable_options" value="0">
          <input id="cb-pc-expose" type="checkbox" name="expose_configurable_options" value="1" checked data-cb-expose-config>
          Expose configurable options to customers
        </label>
        <div class="muted" style="font-size:11px; margin-top:4px;">
          When on, “Config preview → Apply” creates the WHMCS configurable-option groups for this profile. Turn off to keep this profile’s options admin-only (Apply is skipped). Curate exactly which options show via the exposure editor.
        </div>
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
