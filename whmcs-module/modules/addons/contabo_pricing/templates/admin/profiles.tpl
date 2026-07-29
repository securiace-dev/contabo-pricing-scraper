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

<header data-cb-u="u-f266e2da93">
  <div>
    <h2 class="display" data-cb-u="u-0cbe035c55"><?= $trash_mode ? 'Profiles — Trash' : 'Profiles' ?></h2>
    <p class="cb-card-sub" data-cb-u="u-8c7c145b64">
      <?= $trash_mode
        ? 'Soft-deleted profiles. Restore brings one back; permanent purge is guarded and irreversible.'
        : 'Versioned pricing templates that map Contabo plans to your WHMCS products.' ?>
    </p>
  </div>
  <div data-cb-u="u-9c170d8708">
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
  <div class="cb-flash" data-cb-u="u-a527d03795">
    <span data-cb-u="u-7fff97ee23"><?= $esc($flash) ?></span>
    <?php if ($undo_id > 0): ?>
      <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-ab79ea2b85">
        <input type="hidden" name="action" value="profile-restore">
        <input type="hidden" name="id" value="<?= $undo_id ?>">
        <?= generate_token() ?>
        <button type="submit" class="cb-btn subtle" data-cb-u="u-eb99a4c193">↩ Undo</button>
      </form>
    <?php endif; ?>
  </div>
<?php endif; ?>

<?php /* ───────────────────── Trash view ───────────────────── */ ?>
<?php if ($trash_mode): ?>
  <?php if (empty($trashed)): ?>
    <div class="cb-empty">
      <div class="display" data-cb-u="u-84bf8f7a31">Trash is empty</div>
      <p class="cb-card-sub" data-cb-u="u-619b35a9f4">Deleted profiles will appear here, where you can restore or permanently purge them.</p>
      <a class="cb-btn" href="<?= $esc($module_link) ?>&amp;action=profiles">Back to profiles</a>
    </div>
  <?php else: ?>
    <div class="cb-card" data-cb-u="u-14f5e9e79f">
      <table class="cb-table">
        <thead>
          <tr>
            <th>Profile</th>
            <th>Plan slug</th>
            <th>Deleted</th>
            <th>Purge eligibility</th>
            <th data-cb-u="u-fc2af690d3">Actions</th>
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
                <div data-cb-u="u-6e8bcfac8d"><?= $esc($tname) ?></div>
                <div class="muted mono" data-cb-u="u-ab736dfd55"><?= $esc($tslug) ?></div>
              </td>
              <td class="mono"><?= $esc((string) ($tp['plan_slug'] ?? '')) ?></td>
              <td class="muted" data-cb-u="u-a49cca52be"><?= $esc($tdel) ?></td>
              <td>
                <?php if ($allowed): ?>
                  <span class="cb-pill good">eligible</span>
                <?php else: ?>
                  <span class="cb-pill warn" title="<?= $esc(implode(' ', (array) ($assess['reasons'] ?? []))) ?>">blocked</span>
                  <div class="muted" data-cb-u="u-4fbe14f3db"><?= $esc(implode(' ', (array) ($assess['reasons'] ?? []))) ?></div>
                <?php endif; ?>
              </td>
              <td data-cb-u="u-5f326564a5">
                <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-6b8b63d565">
                  <input type="hidden" name="action" value="profile-restore">
                  <input type="hidden" name="id" value="<?= $tpid ?>">
                  <?= generate_token() ?>
                  <button type="submit" class="cb-btn subtle">Restore</button>
                </form>
                <button type="button" class="cb-btn ghost"
                        data-cb-purge-open="<?= $tpid ?>"
                        <?= $allowed ? '' : 'disabled title="Resolve the blockers above first"' ?>
                        data-cb-u="u-5bad398266">Purge…</button>
                <?php if ($allowed): ?>
                  <div data-cb-purge-form="<?= $tpid ?>" hidden data-cb-u="u-94a0f712ff">
                    <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-ab79ea2b85"
                          data-cb-confirm="Permanently purge &quot;<?= $esc($tname) ?>&quot; and everything it owns? This cannot be undone.">
                      <input type="hidden" name="action" value="profile-purge">
                      <input type="hidden" name="id" value="<?= $tpid ?>">
                      <?= generate_token() ?>
                      <label data-cb-u="u-eb3a56761e">
                        Type <code class="mono"><?= $esc($cb_purge_phrase) ?></code> to confirm:
                      </label>
                      <input type="text" name="purge_confirmation_phrase" autocomplete="off"
                             placeholder="<?= $esc($cb_purge_phrase) ?>"
                             data-cb-u="u-f9cec146c1">
                      <button type="submit" class="cb-btn" data-cb-u="u-a979101b9e">Purge permanently</button>
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
       data-cb-u="u-b3eb6bbe30">
    <div data-cb-u="u-cd08368205">
      <span class="cb-pill warn" data-cb-u="u-d1dc646165">conflict</span>
      <div data-cb-u="u-2575e93d1e">
        <h3 class="display" data-cb-u="u-d58e3ffc06">A profile with this slug already exists</h3>
        <p class="cb-card-sub" data-cb-u="u-44f71ea1c9">
          The slug <code class="mono"><?= $esc($cfSlug) ?></code> is already taken by
          <strong><?= $esc($cfExName) ?></strong> (profile #<?= $cfExId ?>), but the
          configuration you just submitted is <em>different</em>. Nothing was changed.
          Choose how to proceed:
        </p>

        <div data-cb-u="u-26f8c9dd81">
          <!-- (1) Open existing -->
          <a class="cb-btn subtle"
             href="<?= $esc($module_link) ?>&amp;action=profile-diff&amp;id=<?= $cfExId ?>">
            Open existing
          </a>

          <!-- (2) Create duplicate with numeric suffix -->
          <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-6b8b63d565">
            <input type="hidden" name="action" value="profile-create">
            <input type="hidden" name="slug" value="<?= $esc($cfSuffix) ?>">
            <?php foreach ($cfSubmit as $fk => $fv): if ($fk === 'slug' || $fk === 'action') { continue; } ?>
              <input type="hidden" name="<?= $esc((string) $fk) ?>" value="<?= $esc(is_array($fv) ? (string) json_encode($fv) : (string) $fv) ?>">
            <?php endforeach; ?>
            <?= generate_token() ?>
            <span class="cb-text-xs">New slug: <code class="mono"><?= $esc($cfSuffix) ?></code></span>
            <button type="submit" class="cb-btn">Create duplicate</button>
          </form>

          <!-- (3) Update existing -->
          <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-6b8b63d565">
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

  <div data-cb-u="u-7fff97ee23"></div>

  <div class="cb-pill grey" data-cb-row-count title="Visible rows">
    <span data-cb-visible-count><?= (int) count($profiles) ?></span> / <?= (int) count($profiles) ?>
  </div>
</div>

<!-- ───────────────────── Bulk-action bar ───────────────────── -->
<div class="cb-card cb-bulk-bar" data-cb-bulk-toolbar hidden data-cb-u="u-5a17309b79">
  <strong><span data-cb-bulk-count>0</span> selected</strong>
  <span data-cb-u="u-443072299c">·</span>
  <button type="button" class="cb-btn subtle" data-cb-bulk-action="apply-latest">Apply latest version</button>
  <button type="button" class="cb-btn ghost"  data-cb-bulk-action="disable">Disable</button>
  <div data-cb-u="u-7fff97ee23"></div>
  <button type="button" class="cb-btn ghost" data-cb-bulk-cancel>Cancel</button>
</div>

<!-- ───────────────────── Profiles table ───────────────────── -->
<?php if (empty($profiles)): ?>
  <div class="cb-empty">
    <div class="display" data-cb-u="u-84bf8f7a31">No profiles yet</div>
    <p class="cb-card-sub" data-cb-u="u-619b35a9f4">Create your first profile to start tracking Contabo pricing for a WHMCS product.</p>
    <button type="button" class="cb-btn" data-cb-open-modal="profile-create">+ New profile</button>
  </div>
<?php else: ?>
  <div class="cb-card" data-cb-u="u-14f5e9e79f">
    <table class="cb-table" data-cb-table="profiles">
      <thead>
        <tr>
          <th data-cb-u="u-63349069ab"><input type="checkbox" data-cb-bulk-all aria-label="Select all"></th>
          <th data-cb-sort="name">Plan name <span class="arr"></span></th>
          <th data-cb-sort="plan_slug">Plan slug <span class="arr"></span></th>
          <th data-cb-sort="period_months">Period <span class="arr"></span></th>
          <th>Region / OS</th>
          <th data-cb-sort="sync_strategy">Strategy <span class="arr"></span></th>
          <th data-cb-sort="active">Status <span class="arr"></span></th>
          <th data-cb-sort="latest_version_id">Latest version <span class="arr"></span></th>
          <th data-cb-u="u-d334b57bfc">Trend</th>
          <th data-cb-u="u-299c8f0abe">Actions</th>
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
            <div data-cb-u="u-6e8bcfac8d"><?= $esc($name !== '' ? $name : $slug) ?></div>
            <div class="muted mono" data-cb-u="u-ab736dfd55"><?= $esc($slug) ?></div>
          </td>
          <td class="mono"><?= $esc($plan) ?></td>
          <td><?= $period ?> mo</td>
          <td>
            <?= $region !== '' ? $esc($region) : '<span class="muted">—</span>' ?>
            <?php if ($os !== ''): ?>
              <div class="muted" data-cb-u="u-33ee298127"><?= $esc($os) ?></div>
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
          <td data-cb-u="u-5f326564a5" data-cb-stop>
            <button type="button" class="cb-btn subtle"
                    data-cb-open-modal="profile-create"
                    data-cb-profile-edit-id="<?= $pid ?>">Edit</button>
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=profile-diff&amp;id=<?= $pid ?>">History</a>
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-preview&amp;id=<?= $pid ?>" title="Preview the WHMCS configurable options this profile would create">Config preview</a>
            <a class="cb-btn ghost" href="<?= $esc($module_link) ?>&amp;action=config-exposure&amp;id=<?= $pid ?>" title="Curate which configurable options are exposed to customers">Exposure</a>
            <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-6b8b63d565">
              <input type="hidden" name="action" value="profile-toggle">
              <input type="hidden" name="id" value="<?= $pid ?>">
              <?= generate_token() ?>
              <button class="cb-btn subtle" type="submit"><?= $active ? 'Disable' : 'Enable' ?></button>
            </form>
            <form method="post" action="<?= $esc($module_link) ?>" data-cb-u="u-6b8b63d565"
                  data-cb-confirm="Move &quot;<?= $esc($name !== '' ? $name : $slug) ?>&quot; to Trash? You can restore it afterwards.">
              <input type="hidden" name="action" value="profile-delete">
              <input type="hidden" name="id" value="<?= $pid ?>">
              <?= generate_token() ?>
              <button class="cb-btn ghost" type="submit" data-cb-u="u-5bad398266" title="Move to Trash (recoverable)">Delete</button>
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
<div class="cb-modal" id="cb-modal-profile-create" hidden>
  <div class="sheet" role="dialog" aria-labelledby="cb-modal-profile-create-title" aria-modal="true" data-cb-u="u-7fd6f74ba3">
    <header data-cb-u="u-7a9696f8e6">
      <h3 id="cb-modal-profile-create-title" class="display" data-cb-u="u-ab79ea2b85" data-cb-modal-title>Create profile</h3>
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
        <div class="muted" data-cb-u="u-64aa79933f" data-cb-mode-hint>
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
        <label>Publish cycles <span class="muted" data-cb-u="u-76b999ddce">— which terms this profile sources</span></label>
        <!-- The single period dropdown is gone: the profile now sources a price
             per published cycle. period_months is derived server-side from the
             longest published cycle (kept for slug + identity). -->
        <input type="hidden" name="published_cycles_mask" value="63" data-cb-published-mask>
        <div data-cb-publish-cycles data-cb-u="u-ee4b4558b5">
          <?php foreach ($cb_publish_cycles as $pc): ?>
            <label data-cb-u="u-d8887aa5ee">
              <input type="checkbox" data-cb-publish-cycle data-cb-bit="<?= (int) $pc['bit'] ?>" data-cb-months="<?= (int) $pc['months'] ?>" checked>
              <span>
                <?= $esc($pc['cycle']) ?>
                <?php if ((int) $pc['months'] >= 24): ?><span class="cb-pill grey" title="Not sold publicly by Contabo — projected from the 12-month rate" data-cb-u="u-c50da33819">projected</span><?php endif; ?>
                <span class="muted mono" data-cb-cycle-source="<?= (int) $pc['months'] ?>" data-cb-u="u-f6ffcfe1dc">—</span>
              </span>
            </label>
          <?php endforeach; ?>
        </div>
        <div class="muted" data-cb-u="u-8693fd01b7">
          Source (cost) price per cycle, in EUR/mo, shown once a plan loads. The
          <strong>mapping</strong> decides which of these the customer sees and at what markup.
        </div>
      </div>

      <!-- Hidden period kept ONLY to drive the configurator's dimension-delta
           preview (app.js reads data-cb-cfg-period). The real period is derived
           server-side from the published cycles. -->
      <input type="hidden" value="12" data-cb-cfg-period data-cb-quote-period>

      <!-- Configurator: hydrated by assets/app.js once plan_slug + period are set. -->
      <div class="cb-card" data-cb-u="u-ca659fbc54">
        <div data-cb-u="u-5ffb91a26b">
          <div>
            <div class="cb-card-sub" data-cb-u="u-38965f9b18">Configure &amp; price</div>
            <div class="muted" data-cb-u="u-33ee298127">Per-dimension dropdowns mirror the upstream Contabo order page.</div>
          </div>
          <button type="button" class="cb-btn ghost" data-cb-cfg-reset data-cb-u="u-a8d41863df" hidden>Reset defaults</button>
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
        <label data-cb-u="u-f8d305aab0">
          <!-- unchecked checkboxes don't POST; the hidden 0 is the fallback the checkbox overrides -->
          <input type="hidden" name="expose_configurable_options" value="0">
          <input id="cb-pc-expose" type="checkbox" name="expose_configurable_options" value="1" checked data-cb-expose-config>
          Expose configurable options to customers
        </label>
        <div class="muted" data-cb-u="u-64aa79933f">
          When on, “Config preview → Apply” creates the WHMCS configurable-option groups for this profile. Turn off to keep this profile’s options admin-only (Apply is skipped). Curate exactly which options show via the exposure editor.
        </div>
      </div>

      <div class="cb-field">
        <label for="cb-pc-tags">Tags <span class="muted">(comma-separated)</span></label>
        <input id="cb-pc-tags" type="text" name="tags" placeholder="production, billing-team">
      </div>

      <div data-cb-u="u-ca21caa3b0">
        <button type="button" class="cb-btn subtle" data-cb-close-modal>Cancel</button>
        <button type="submit" class="cb-btn" data-cb-submit-label>Create profile</button>
      </div>
    </form>
  </div>
</div>

</div>
