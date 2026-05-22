<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Routes admin-area HTTP requests to the appropriate view. Renders Smarty
 * templates with a uniform $data array.
 */
class AdminController
{
    /**
     * Single source of truth for the addon version. `contabo_pricing_config()`
     * reads this, and `render()` passes it to the layout as the asset
     * cache-buster (`app.js?v=…`) so a release always invalidates the old JS.
     */
    public const VERSION = '0.4.10';

    /** @var Settings */ private $settings;
    /** @var string */   private $templateDir;

    public function __construct(Settings $settings, string $templateDir)
    {
        $this->settings    = $settings;
        $this->templateDir = $templateDir;
    }

    /**
     * @param array<string, mixed> $req query + post merged by WHMCS
     */
    public function dispatch(array $req): void
    {
        $action = (string) ($req['action'] ?? 'dashboard');
        switch ($action) {
            case 'profiles':         $this->profiles($req); return;
            case 'profile-create':   $this->profileCreate($req); return;
            case 'profile-save':     $this->profileSave($req); return;
            case 'profile-toggle':   $this->profileToggle($req); return;
            case 'profile-diff':     $this->profileDiff($req); return;
            case 'config-preview':   $this->configPreview($req); return;
            case 'config-apply':     $this->configApply($req); return;
            case 'mappings':         $this->mappings($req); return;
            case 'mapping-save':     $this->mappingSave($req); return;
            case 'sync-history':     $this->syncHistory(); return;
            case 'sync-run':         $this->syncRun($req); return;
            case 'refresh-api':      $this->refreshApi(); return;
            case 'settings':         $this->settingsView(); return;
            case 'ajax-quote':           $this->ajaxQuote($req); return;
            case 'ajax-fx':              $this->ajaxFx(); return;
            case 'ajax-meta-probe':      $this->ajaxMetaProbe(); return;
            case 'ajax-profile-versions': $this->ajaxProfileVersions($req); return;
            case 'ajax-profile':         $this->ajaxProfile($req); return;
            case 'ajax-configurator':    $this->ajaxConfigurator($req); return;
            case 'ajax-profile-edit-form': $this->ajaxProfileEditForm($req); return;
            case 'ajax-product-cycles':  $this->ajaxProductCycles($req); return;
            // ── Renewal Pricing Policy Engine — Phase A (read-mostly UI) ─────
            case 'repricing':            $this->repricingDashboard(); return;
            case 'price-decisions':      $this->priceDecisions($req); return;
            case 'price-decisions-csv':  $this->priceDecisionsCsv($req); return;
            case 'skipped-report':       $this->skippedReport(); return;
            case 'tax-settings':         $this->taxSettings(); return;
            case 'tax-settings-save':    $this->taxSettingsSave($req); return;
            case 'ajax-policy-preview':  $this->ajaxPolicyPreview($req); return;
            // ── A.5.1 — schema maintenance ───────────────────────────────────
            case 'maintenance':          $this->maintenance(); return;
            case 'maintenance-migrate':  $this->maintenanceMigrate(); return;
            case 'maintenance-purge':    $this->maintenancePurge($req); return;
            case 'dashboard':
            default:                 $this->dashboard(); return;
        }
    }

    // ── AJAX endpoints ───────────────────────────────────────────────────────
    //
    // Each method emits JSON only and returns immediately. Read-only endpoints
    // (ajax-fx, ajax-profile-versions, ajax-profile) intentionally do NOT call
    // check_token() — they're side-effect-free and a stale token shouldn't
    // break the drawer. Mutating endpoints (ajax-quote which hits a paid API,
    // ajax-meta-probe which can leak server reachability) DO require a token.

    /** Send JSON headers + body for an OK response. Always returns void. */
    private function jsonOk(array $payload): void
    {
        if (!headers_sent()) {
            header('Content-Type: application/json; charset=utf-8');
            header('X-Content-Type-Options: nosniff');
        }
        echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /** Send JSON headers + body for a 500 error response. */
    private function jsonFail(string $msg, int $code = 500): void
    {
        if (!headers_sent()) {
            header('Content-Type: application/json; charset=utf-8');
            http_response_code($code);
        }
        echo json_encode(['error' => $msg]);
    }

    /**
     * @param array<string, mixed> $req
     */
    private function ajaxQuote(array $req): void
    {
        try {
            if (function_exists('check_token')) {
                check_token();
            }
            $planSlug = (string) ($req['plan_slug'] ?? '');
            $period   = (int) ($req['period_months'] ?? 1);
            if ($planSlug === '') {
                $this->jsonFail('plan_slug is required', 400);
                return;
            }
            $api = new ApiClient($this->settings);
            $body = [
                'plan_slug'     => $planSlug,
                'period_months' => $period,
                'currency_iso'  => $this->settings->currencyIso,
                'apply_gst'     => $this->settings->applyGst18,
                'fx_markup_pct' => $this->settings->fxMarkupPct,
            ];
            $res = $api->quote($body);
            // Surface tax / FX inputs so the UI can render the explanation line.
            if (!isset($res['gst_pct'])) {
                $res['gst_pct'] = $this->settings->applyGst18 ? 18 : 0;
            }
            if (!isset($res['fx_markup_pct'])) {
                $res['fx_markup_pct'] = $this->settings->fxMarkupPct;
            }
            if (!isset($res['currency_iso'])) {
                $res['currency_iso'] = $this->settings->currencyIso;
            }
            $this->jsonOk($res);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    private function ajaxFx(): void
    {
        try {
            $api = new ApiClient($this->settings);
            $res = $api->fx();
            // Compute a best-effort age_minutes if the FX endpoint exposes a
            // timestamp under any of the common field names.
            $ts = null;
            foreach (['asof', 'as_of', 'fetched_at', 'timestamp', 'updated_at'] as $k) {
                if (isset($res[$k]) && $res[$k] !== '') { $ts = $res[$k]; break; }
            }
            if ($ts !== null) {
                $parsed = is_numeric($ts) ? (int) $ts : strtotime((string) $ts);
                if ($parsed !== false && $parsed > 0) {
                    $res['age_minutes'] = max(0, (int) round((time() - $parsed) / 60));
                }
            }
            $this->jsonOk($res);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    private function ajaxMetaProbe(): void
    {
        try {
            if (function_exists('check_token')) {
                check_token();
            }
            $api = new ApiClient($this->settings);
            $meta = $api->meta();
            $this->jsonOk([
                'ok'              => true,
                'scraper_version' => isset($meta['scraper_version']) ? (string) $meta['scraper_version'] : (isset($meta['version']) ? (string) $meta['version'] : ''),
                'snapshot_at'     => isset($meta['snapshot_at']) ? (string) $meta['snapshot_at'] : (isset($meta['generated_at']) ? (string) $meta['generated_at'] : ''),
            ]);
        } catch (\Throwable $e) {
            // Soft-fail with 200 + ok:false so the UI can render a bad pill.
            if (!headers_sent()) {
                header('Content-Type: application/json; charset=utf-8');
            }
            echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
        }
    }

    /**
     * @param array<string, mixed> $req
     */
    private function ajaxProfileVersions(array $req): void
    {
        try {
            $id = (int) ($req['id'] ?? 0);
            if ($id <= 0) {
                $this->jsonFail('id is required', 400);
                return;
            }
            $rows = Capsule::table('mod_contabo_profile_version')
                ->where('profile_id', $id)
                ->orderByDesc('version')
                ->limit(50)
                ->get(['version', 'final_monthly', 'currency_iso', 'snapshot_generated_at'])
                ->map(static function ($r) { return (array) $r; })
                ->all();
            $this->jsonOk(['profile_id' => $id, 'versions' => $rows]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    /**
     * @param array<string, mixed> $req
     */
    private function ajaxProfile(array $req): void
    {
        try {
            $id = (int) ($req['id'] ?? 0);
            if ($id <= 0) {
                $this->jsonFail('id is required', 400);
                return;
            }
            $pm = new ProfileManager($this->settings);
            $profile = $pm->find($id);
            if ($profile === null) {
                $this->jsonFail('Profile not found', 404);
                return;
            }
            $latest = $pm->latestVersion($id);
            $this->jsonOk([
                'profile'        => $profile,
                'latest_version' => $latest,
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    /**
     * GET ajax-configurator?plan_slug=cloud-vps-10
     *
     * Fetches the raw configurator object from the upstream API and reshapes
     * its `options` map into the same `controls[]` list the report.html
     * calculator uses (one control per dimension, with Image + Networking
     * split by category). Per-period defaults are returned verbatim so the
     * frontend can anchor totals against them. Read-only — no CSRF.
     *
     * @param array<string, mixed> $req
     */
    private function ajaxConfigurator(array $req): void
    {
        try {
            $planSlug = (string) ($req['plan_slug'] ?? '');
            if ($planSlug === '') {
                $this->jsonFail('plan_slug is required', 400);
                return;
            }
            $api = new ApiClient($this->settings);
            $cfg = $api->configurator($planSlug);
            $controls = $this->buildConfiguratorControls($cfg);

            $defaultMonthly = isset($cfg['default_monthly_by_period']) && is_array($cfg['default_monthly_by_period'])
                ? $cfg['default_monthly_by_period'] : [];
            $defaultSetup = isset($cfg['default_setup_by_period']) && is_array($cfg['default_setup_by_period'])
                ? $cfg['default_setup_by_period'] : [];

            $periods = [];
            if (isset($cfg['contract_periods']) && is_array($cfg['contract_periods'])) {
                foreach ($cfg['contract_periods'] as $row) {
                    if (!is_array($row)) continue;
                    $m = isset($row['months']) ? (int) $row['months'] : 0;
                    if ($m <= 0) continue;
                    $periods[(string) $m] = [
                        'months'            => $m,
                        'is_hidden_from_ui' => !empty($row['is_hidden_from_ui']),
                        'effective_monthly' => isset($row['effective_monthly']) ? (float) $row['effective_monthly'] : 0.0,
                        'setup_fee'         => isset($row['setup_fee']) ? (float) $row['setup_fee'] : 0.0,
                        'total_period_cost' => isset($row['total_period_cost']) ? (float) $row['total_period_cost'] : 0.0,
                    ];
                }
            }

            $this->jsonOk([
                'plan_slug'                => $planSlug,
                'controls'                 => $controls,
                'default_monthly_by_period' => $defaultMonthly,
                'default_setup_by_period'   => $defaultSetup,
                'periods'                  => $periods,
                'title'                    => isset($cfg['title']) ? (string) $cfg['title'] : '',
                'family'                   => isset($cfg['family']) ? (string) $cfg['family'] : '',
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    /**
     * Reshape `configs[slug].options` into the report.html `controls[]` model.
     *
     * @param array<string, mixed> $cfg
     * @return list<array<string, mixed>>
     */
    private function buildConfiguratorControls(array $cfg): array
    {
        $controls = [];
        $opts = isset($cfg['options']) && is_array($cfg['options']) ? $cfg['options'] : [];

        $imageCats   = ['OS', 'Apps', 'Panels', 'Blockchain'];
        $imageLabel  = ['OS' => 'OS', 'Apps' => 'App', 'Panels' => 'Control Panel', 'Blockchain' => 'Blockchain'];
        $networkCats = ['Bandwidth', 'IPv4', 'Private Networking'];

        foreach ($opts as $dim => $list) {
            if (!is_array($list) || count($list) === 0) continue;

            if ($dim === 'Networking') {
                foreach ($networkCats as $cat) {
                    $sub = [];
                    foreach ($list as $o) {
                        if (is_array($o) && isset($o['category']) && (string) $o['category'] === $cat) {
                            $sub[] = $o;
                        }
                    }
                    if (count($sub) > 0) {
                        $controls[] = $this->finalizeControl('Networking:' . $cat, $cat, false, $sub);
                    }
                }
            } elseif ($dim === 'Image') {
                $seen = [];
                $allCats = $imageCats;
                foreach ($list as $o) {
                    if (is_array($o) && isset($o['category'])) {
                        $c = (string) $o['category'];
                        if (!in_array($c, $allCats, true) && !in_array($c, $seen, true)) {
                            $seen[] = $c;
                        }
                    }
                }
                $allCats = array_merge($allCats, $seen);
                foreach ($allCats as $cat) {
                    $sub = [];
                    foreach ($list as $o) {
                        if (is_array($o) && isset($o['category']) && (string) $o['category'] === $cat) {
                            $sub[] = $o;
                        }
                    }
                    if (count($sub) > 0) {
                        $label = isset($imageLabel[$cat]) ? $imageLabel[$cat] : $cat;
                        // OS is required; everything else (Apps/Panels/Blockchain/etc) is optional.
                        $controls[] = $this->finalizeControl('Image:' . $cat, $label, $cat !== 'OS', $sub);
                    }
                }
            } else {
                $optional = ($dim === 'Data Protection');
                $controls[] = $this->finalizeControl((string) $dim, (string) $dim, $optional, $list);
            }
        }
        return $controls;
    }

    /**
     * Sort options by (default first, then cheapest, then label), inject a
     * synthetic 'None' option for optional controls if missing, and pick the
     * default index. Mirrors generate_html.js → finalizeControl().
     *
     * @param list<array<string, mixed>> $raw
     * @return array<string, mixed>
     */
    private function finalizeControl(string $key, string $label, bool $optional, array $raw): array
    {
        $opts = [];
        foreach ($raw as $o) {
            if (!is_array($o)) continue;
            $opts[] = [
                'label'   => isset($o['option_label']) ? (string) $o['option_label'] : '',
                'monthly' => isset($o['monthly_price_delta']) ? (float) $o['monthly_price_delta'] : 0.0,
                'setup'   => isset($o['setup_fee_delta']) ? (float) $o['setup_fee_delta'] : 0.0,
                'isDef'   => !empty($o['is_default']),
            ];
        }
        usort($opts, static function ($a, $b) {
            if ($a['isDef'] !== $b['isDef']) {
                return $a['isDef'] ? -1 : 1;
            }
            if ($a['monthly'] !== $b['monthly']) {
                return ($a['monthly'] < $b['monthly']) ? -1 : 1;
            }
            return strcmp((string) $a['label'], (string) $b['label']);
        });
        if ($optional) {
            $hasNone = false;
            foreach ($opts as $o) {
                if ($o['label'] === 'None') { $hasNone = true; break; }
            }
            if (!$hasNone) {
                array_unshift($opts, ['label' => 'None', 'monthly' => 0.0, 'setup' => 0.0, 'isDef' => false]);
            }
        }
        $defaultIdx = -1;
        foreach ($opts as $i => $o) {
            if ($o['isDef']) { $defaultIdx = $i; break; }
        }
        if ($defaultIdx < 0 && $optional) {
            foreach ($opts as $i => $o) {
                if ($o['label'] === 'None') { $defaultIdx = $i; break; }
            }
        }
        if ($defaultIdx < 0) {
            $defaultIdx = 0;
        }
        $clean = [];
        foreach ($opts as $o) {
            $clean[] = ['label' => $o['label'], 'monthly' => $o['monthly'], 'setup' => $o['setup']];
        }
        return [
            'key'        => $key,
            'label'      => $label,
            'optional'   => $optional,
            'defaultIdx' => $defaultIdx,
            'options'    => $clean,
        ];
    }

    /**
     * GET ajax-profile-edit-form?id=42
     *
     * Returns the profile row + decoded option selections, used by the JS to
     * prefill the create/edit modal. Empty selections (legacy v0.1.x rows)
     * come back as an empty object — the JS falls back to per-control
     * defaults.
     *
     * @param array<string, mixed> $req
     */
    private function ajaxProfileEditForm(array $req): void
    {
        try {
            $id = (int) ($req['id'] ?? 0);
            if ($id <= 0) {
                $this->jsonFail('id is required', 400);
                return;
            }
            $pm = new ProfileManager($this->settings);
            $profile = $pm->find($id);
            if ($profile === null) {
                $this->jsonFail('Profile not found', 404);
                return;
            }
            $selections = [];
            if (!empty($profile['options'])) {
                $decoded = json_decode((string) $profile['options'], true);
                if (is_array($decoded)) {
                    $selections = $decoded;
                }
            }
            $this->jsonOk([
                'profile'    => $profile,
                'selections' => (object) $selections,
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    // ── Pages ────────────────────────────────────────────────────────────────

    private function dashboard(): void
    {
        // Self-heal a stale schema on first page view (non-fatal: a failed
        // migration is logged, the dashboard still renders what it can).
        SchemaHealth::assertOrMigrate();

        $api = new ApiClient($this->settings);
        $meta = []; $err = null;
        try { $meta = $api->meta(); } catch (\Throwable $e) { $err = $e->getMessage(); }

        $lastSync = Capsule::table('mod_contabo_sync_log')->orderByDesc('id')->first();
        $profileCount = (int) Capsule::table('mod_contabo_profile')->where('active', true)->count();
        $versionCount = (int) Capsule::table('mod_contabo_profile_version')->count();
        $mappingCount = (int) Capsule::table('mod_contabo_mapping')->where('active', true)->count();

        $this->render('dashboard.tpl', [
            'meta'         => $meta,
            'connect_error' => $err,
            'last_sync'    => $lastSync ? (array) $lastSync : null,
            'profile_count' => $profileCount,
            'version_count' => $versionCount,
            'mapping_count' => $mappingCount,
            'settings'      => $this->settings,
            'flash'        => (string) ($_REQUEST['flash'] ?? ''),
        ]);
    }

    private function profiles(array $req): void
    {
        $pm = new ProfileManager($this->settings);
        $api = new ApiClient($this->settings);
        $plans = [];
        try { $plans = $api->plans(); } catch (\Throwable $e) { /* read-only path tolerates API outage */ }

        $this->render('profiles.tpl', [
            'profiles' => $pm->listProfiles(false),
            'available_plans' => $plans,
            'flash' => (string) ($req['flash'] ?? ''),
        ]);
    }

    private function profileCreate(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $optionsPayload = $this->decodeOptionsPayload($req['options'] ?? null);
        // OS / Region are derived from the configurator selection when present;
        // fall back to the (now hidden) free-text overrides for legacy posts.
        $os     = (string) ($req['os'] ?? '');
        $region = (string) ($req['region'] ?? '');
        if ($optionsPayload !== null) {
            $derived = $this->deriveOsRegionFromSelections($optionsPayload);
            if ($derived['os'] !== '')     { $os = $derived['os']; }
            if ($derived['region'] !== '') { $region = $derived['region']; }
        }
        $create = [
            'slug'          => (string) ($req['slug'] ?? ''),
            'name'          => trim((string) ($req['name'] ?? '')),
            'plan_slug'     => (string) ($req['plan_slug'] ?? ''),
            'period_months' => (int) ($req['period_months'] ?? 1),
            'region'        => $region,
            'os'            => $os,
            'tags'          => (string) ($req['tags'] ?? ''),
            'sync_strategy' => (string) ($req['sync_strategy'] ?? $this->settings->defaultSyncStrategy),
        ];
        if ($optionsPayload !== null) {
            $create['options'] = $optionsPayload;
        }

        try {
            // Direct repository call so we can distinguish created from
            // loaded-existing from conflict (ProfileManager::create collapses
            // the first two to an id and throws on conflict).
            $result = (new ProfileRepository($this->settings->defaultSyncStrategy))->createOrResolve($create);
        } catch (\Throwable $e) {
            $this->renderSaveError($e, 'profiles');
            return;
        }

        $status = (string) ($result['status'] ?? 'created');
        if ($status === 'created') {
            $this->redirect('profiles', ['flash' => 'Created profile #' . (int) ($result['profile_id'] ?? 0)]);
            return;
        }
        if ($status === 'loaded_existing') {
            $this->redirect('profiles', ['flash' => 'Profile already exists. Loaded existing profile.']);
            return;
        }
        // conflict — same slug, different configuration. Render the chooser; no write happened.
        $existing = is_array($result['existing'] ?? null) ? $result['existing'] : [];
        $plans = [];
        try { $plans = (new ApiClient($this->settings))->plans(); } catch (\Throwable $e) { /* read-only path tolerates API outage */ }
        $this->render('profiles.tpl', [
            'profiles'        => (new ProfileManager($this->settings))->listProfiles(false),
            'available_plans' => $plans,
            'flash'           => '',
            'cb_profile_conflict' => [
                'slug'              => (string) ($existing['slug'] ?? ($create['slug'] !== '' ? $create['slug'] : '')),
                'existing_id'       => (int) ($existing['id'] ?? 0),
                'existing_name'     => (string) ($existing['name'] ?? ''),
                'suffix_suggestion' => (string) ($result['suffix_suggestion'] ?? ''),
                'submit'            => $create,
            ],
        ]);
    }

    private function profileSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $optionsPayload = $this->decodeOptionsPayload($req['options'] ?? null);
        $os     = isset($req['os']) ? (string) $req['os'] : null;
        $region = isset($req['region']) ? (string) $req['region'] : null;
        if ($optionsPayload !== null) {
            $derived = $this->deriveOsRegionFromSelections($optionsPayload);
            if ($derived['os'] !== '')     { $os = $derived['os']; }
            if ($derived['region'] !== '') { $region = $derived['region']; }
        }
        $patch = array_filter([
            'name'          => $req['name'] ?? null,
            'plan_slug'     => $req['plan_slug'] ?? null,
            'period_months' => isset($req['period_months']) ? (int) $req['period_months'] : null,
            'region'        => $region,
            'os'            => $os,
            'tags'          => $req['tags'] ?? null,
            'sync_strategy' => $req['sync_strategy'] ?? null,
            'options'       => $optionsPayload,
        ], static fn ($v) => $v !== null);
        $pm->update($id, $patch);
        $this->redirect('profiles', ['flash' => "Updated profile #{$id}"]);
    }

    /**
     * Decode a posted `options` field. Accepts JSON string or already-decoded
     * array. Returns null if the field is missing/empty/malformed so the
     * caller can skip it (rather than clobbering an existing row with null).
     *
     * @param mixed $raw
     * @return array<string, mixed>|null
     */
    private function decodeOptionsPayload($raw): ?array
    {
        if ($raw === null) return null;
        if (is_array($raw)) return $raw;
        if (!is_string($raw) || $raw === '') return null;
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) return null;
        return $decoded;
    }

    /**
     * Pull the OS label + Region label out of a configurator selections
     * payload. Selections are keyed by the control's report-style key
     * (`Image:OS`, `Region`). Each value is `{label,monthly,setup,...}`.
     *
     * @param array<string, mixed> $sel
     * @return array{os: string, region: string}
     */
    private function deriveOsRegionFromSelections(array $sel): array
    {
        $out = ['os' => '', 'region' => ''];
        if (isset($sel['Image:OS']) && is_array($sel['Image:OS']) && isset($sel['Image:OS']['label'])) {
            $out['os'] = (string) $sel['Image:OS']['label'];
        }
        if (isset($sel['Region']) && is_array($sel['Region']) && isset($sel['Region']['label'])) {
            $out['region'] = (string) $sel['Region']['label'];
        }
        return $out;
    }

    private function profileToggle(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $row = $pm->find($id);
        if ($row !== null) {
            $pm->setActive($id, !((bool) $row['active']));
        }
        $this->redirect('profiles');
    }

    private function profileDiff(array $req): void
    {
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $profile = $pm->find($id);
        if ($profile === null) {
            echo '<div class="errorbox">Profile not found.</div>';
            return;
        }
        $versions = $pm->listVersions($id);
        $this->render('profile_diff.tpl', [
            'profile' => $profile,
            'versions' => $versions,
        ]);
    }

    /**
     * A.6.3 — read-only PREVIEW of the WHMCS configurable options that the
     * ConfigurableOptionsSyncer WOULD create for a profile. Runs the syncer in
     * dry-run (nothing is written to WHMCS) against the profile's latest
     * version snapshot, with pricing derived from that version's landed cost +
     * markup. The apply path (write) is a separate, confirmed action.
     *
     * @param array<string,mixed> $req
     */
    private function configPreview(array $req): void
    {
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $profile = $pm->find($id);
        if ($profile === null) {
            echo '<div class="errorbox">Profile not found. '
                . '<a href="' . htmlspecialchars($this->settings->moduleLink, ENT_QUOTES, 'UTF-8')
                . '&action=profiles">Back to profiles</a>.</div>';
            return;
        }

        $version = $pm->latestVersion($id);
        if ($version === null) {
            echo '<div class="errorbox">This profile has no version yet — run a sync first so a '
                . 'configuration snapshot exists to preview.</div>';
            return;
        }

        // The configurable-option dimensions come from the plan's live Contabo
        // options map — the version's specs_snapshot holds hardware specs, not
        // the options. DimensionParser turns that map (Image/Networking/Region/
        // Storage/Data Protection) into WHMCS option specs.
        $planSlug = (string) ($profile['plan_slug'] ?? '');
        $specs    = [];
        $omitted  = [];
        $apiError = '';
        try {
            $cfg = (new ApiClient($this->settings))->configurator($planSlug);
            $optionsMap = (isset($cfg['options']) && is_array($cfg['options'])) ? $cfg['options'] : [];
            $parsed  = DimensionParser::parse($optionsMap);
            $specs   = isset($parsed['specs']) && is_array($parsed['specs']) ? $parsed['specs'] : [];
            $omitted = isset($parsed['omitted']) && is_array($parsed['omitted']) ? $parsed['omitted'] : [];
        } catch (\Throwable $e) {
            $apiError = $e->getMessage();
        }

        // Pricing context: reuse the version's landed cost basis + its markup.
        $baseEur      = (float) ($version['base_monthly_eur'] ?? 0.0);
        $finalMonthly = (float) ($version['final_monthly'] ?? 0.0);
        $multiplier   = $baseEur > 0.0 ? $finalMonthly / $baseEur : 0.0;
        $markupStrat  = (string) ($version['markup_strategy'] ?? 'cost_plus_pct');
        $markupValue  = (float) ($version['markup_value'] ?? 0.0);
        $roundingMode = 'exact_2_decimals';

        $ctx = new ConfigOptionPricingContext(
            WhmcsConfigOptionsAdapter::INR_CURRENCY_ID, // base currency id (1)
            $multiplier,
            $markupStrat,
            $markupValue,
            $roundingMode
        );

        // Dry-run adapter + an in-memory audit logger so repeated previews don't
        // spam mod_contabo_config_option_audit (real audit rows are written by
        // the apply path).
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $audit = new class($adapter->syncBatchId()) extends OptionAuditLog {
            protected function storeRow(array $row): int
            {
                static $n = 0;
                return ++$n;
            }
        };
        $syncer = new ConfigurableOptionsSyncer($adapter, $audit);

        $groupName = 'Contabo ' . (string) ($profile['plan_slug'] ?? 'options');
        $report = $syncer->observe($id, $groupName, $specs, $ctx);

        // Validate the default selection (one default value per dimension) through
        // SelectionValidator: hard compatibility violations + capability warnings
        // (destructive options). Both matrices are empty until populated (compat is
        // manual; capability is seeded on Apply), so this surfaces nothing until
        // there's data — the hook is in place for when there is.
        $defaultSelection = $this->defaultSelectionFromSpecs($specs);
        $validation = (new SelectionValidator())->validate((string) ($profile['plan_slug'] ?? ''), $defaultSelection);

        $this->render('config_preview.tpl', [
            'profile'       => $profile,
            'version'       => $version,
            'report'        => $report,
            'omitted'       => $omitted,
            'currency_iso'  => (string) ($version['currency_iso'] ?? $this->settings->currencyIso),
            'markup_strategy' => $markupStrat,
            'markup_value'  => $markupValue,
            'landed_mult'   => $multiplier,
            'api_error'     => $apiError,
            'mapped_products' => $this->mappedProductsForProfile($id),
            'validation'    => $validation,
            'flash'         => (string) ($req['flash'] ?? ''),
        ]);
    }

    /**
     * The default selection (one value per dimension) from DimensionParser specs:
     * the value flagged is_default, else the first value. Used to validate the
     * profile's out-of-the-box configuration.
     *
     * @param list<array<string,mixed>> $specs
     * @return list<array{dimension_key:string,value_key:string,qty:int}>
     */
    private function defaultSelectionFromSpecs(array $specs): array
    {
        $sel = [];
        foreach ($specs as $spec) {
            $dim = (string) ($spec['dimension_key'] ?? '');
            $values = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];
            if ($dim === '' || $values === []) {
                continue;
            }
            $chosen = null;
            foreach ($values as $v) {
                if (!empty($v['is_default'])) {
                    $chosen = $v;
                    break;
                }
            }
            if ($chosen === null) {
                $chosen = $values[0];
            }
            $sel[] = [
                'dimension_key' => $dim,
                'value_key'     => (string) ($chosen['value_key'] ?? ''),
                'qty'           => 1,
            ];
        }
        return $sel;
    }

    /**
     * The WHMCS products this profile is actively mapped to, with names. Used to
     * pick an apply target on the preview screen.
     *
     * @return list<array{id:int,name:string}>
     */
    private function mappedProductsForProfile(int $profileId): array
    {
        $out = [];
        $rows = Capsule::table('mod_contabo_mapping')
            ->where('profile_id', $profileId)
            ->where('active', 1)
            ->get(['product_id']);
        foreach ($rows as $r) {
            $pid = (int) (is_object($r) ? $r->product_id : $r['product_id']);
            $p = Capsule::table('tblproducts')->where('id', $pid)->first();
            $name = $p !== null ? (string) (is_object($p) ? $p->name : $p['name']) : ('Product #' . $pid);
            $out[] = ['id' => $pid, 'name' => $name];
        }
        return $out;
    }

    /**
     * A.6.3 — APPLY: write a profile's configurable options to a mapped WHMCS
     * product. POST-only, CSRF-protected, requires an explicit confirmation +
     * a product the profile is actually mapped to. Idempotent + ownership-scoped
     * via ConfigOptionLinkRepository. Base-currency only.
     *
     * @param array<string,mixed> $req
     */
    private function configApply(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $id        = (int) ($req['id'] ?? 0);
        $productId = (int) ($req['product_id'] ?? 0);
        $confirmed = !empty($req['confirm']);

        $pm = new ProfileManager($this->settings);
        $profile = $pm->find($id);
        if ($profile === null) {
            $this->redirect('profiles', ['flash' => 'Profile not found.']);
            return;
        }
        if (!$confirmed) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Apply cancelled — you must tick the confirmation box.']);
            return;
        }
        // The product must be one this profile is actively mapped to.
        $mapped = Capsule::table('mod_contabo_mapping')
            ->where('profile_id', $id)->where('product_id', $productId)->first();
        if ($productId <= 0 || $mapped === null) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Choose a WHMCS product this profile is mapped to (map one on the Mappings page first).']);
            return;
        }
        $version = $pm->latestVersion($id);
        if ($version === null) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'No version to apply — run a sync first.']);
            return;
        }

        $planSlug = (string) ($profile['plan_slug'] ?? '');
        try {
            $cfg = (new ApiClient($this->settings))->configurator($planSlug);
            $optionsMap = (isset($cfg['options']) && is_array($cfg['options'])) ? $cfg['options'] : [];
            $parsed = DimensionParser::parse($optionsMap);
            $specs  = isset($parsed['specs']) && is_array($parsed['specs']) ? $parsed['specs'] : [];
        } catch (\Throwable $e) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Cannot apply — API error loading options: ' . $e->getMessage()]);
            return;
        }
        if ($specs === []) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Nothing to apply — the plan has no configurable options.']);
            return;
        }

        $baseEur    = (float) ($version['base_monthly_eur'] ?? 0.0);
        $multiplier = $baseEur > 0.0 ? ((float) ($version['final_monthly'] ?? 0.0)) / $baseEur : 0.0;
        $ctx = new ConfigOptionPricingContext(
            WhmcsConfigOptionsAdapter::INR_CURRENCY_ID,
            $multiplier,
            (string) ($version['markup_strategy'] ?? 'cost_plus_pct'),
            (float) ($version['markup_value'] ?? 0.0),
            'exact_2_decimals'
        );

        $adapter = new WhmcsConfigOptionsAdapter(false); // real write
        $audit   = new OptionAuditLog($adapter->syncBatchId());
        $syncer  = new ConfigurableOptionsSyncer($adapter, $audit, new ConfigOptionLinkRepository());

        try {
            $r = $syncer->apply($id, $productId, 'contabo-' . $planSlug, 'Contabo ' . $planSlug, $specs, $ctx);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing config-apply error (profile #' . $id . '): ' . $e->getMessage());
            }
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Apply failed: ' . $e->getMessage()]);
            return;
        }

        // Seed the §4 capability defaults for the plan's dimensions/values so the
        // capability matrix is populated alongside the WHMCS config options. These
        // are manual_assumption defaults; a Phase C deploy-API check upgrades them.
        $capSeeded = 0;
        try {
            $capSeeded = (new CapabilityDefaultsProvider())
                ->seedForPlan($planSlug, $specs, new ConfigOptionCapabilityRepository());
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing capability seed warning (profile #' . $id . '): ' . $e->getMessage());
            }
        }

        $s = $r['summary'];
        $msg = sprintf(
            'Applied to product #%d — %d created, %d updated, %d unchanged, %d skipped (%d options, %d values); %d capability defaults seeded.',
            $productId,
            (int) $s['created'], (int) $s['updated'], (int) $s['noop'], (int) $s['skipped'],
            (int) $r['options'], (int) $r['values'], $capSeeded
        );
        $this->redirect('config-preview', ['id' => $id, 'flash' => $msg]);
    }

    private function mappings(array $req): void
    {
        $pm = new ProfileManager($this->settings);
        $profiles = $pm->listProfiles(false);
        $whmcsProducts = Capsule::table('tblproducts')
            ->orderBy('name')->limit(500)
            ->get(['id', 'name', 'gid'])->map(static fn ($r) => (array) $r)->all();
        $mappings = Capsule::table('mod_contabo_mapping')
            ->orderByDesc('updated_at')->get()->map(static fn ($r) => (array) $r)->all();

        $currencies = [];
        $defaultCurrencyId = 0;
        try {
            $currencies = Capsule::table('tblcurrencies')
                ->orderBy('code')
                ->get(['id', 'code', 'prefix', 'default'])
                ->map(static fn ($r) => (array) $r)->all();
            foreach ($currencies as $c) {
                if (!empty($c['default'])) {
                    $defaultCurrencyId = (int) $c['id'];
                    break;
                }
            }
            if ($defaultCurrencyId === 0 && !empty($currencies)) {
                $defaultCurrencyId = (int) $currencies[0]['id'];
            }
        } catch (\Throwable $e) {
            // Test / install paths without WHMCS currencies — fall through.
        }

        $this->render('mappings.tpl', [
            'profiles'            => $profiles,
            'whmcs_products'      => $whmcsProducts,
            'whmcs_currencies'    => $currencies,
            'default_currency_id' => $defaultCurrencyId,
            'mappings'            => $mappings,
            'flash'               => (string) ($req['flash'] ?? ''),
        ]);
    }

    /**
     * Persist a mapping row. Accepts the Phase A.5 bitmask payload
     * (`catalog_cycles_mask` / `renewal_cycles_mask` / `markup_overrides_json` +
     * the three boolean guards + `rounding_mode`) and falls back to the legacy
     * `apply_to_*` checkboxes if the new fields are absent.
     *
     * Mask values are clamped to CycleSet::MASK_MAX (six bits, 0..63) before
     * storage. The markup overrides JSON is decoded + re-encoded for shape
     * validation; an invalid blob is silently coerced to an empty object so a
     * malformed JS payload never poisons the row.
     *
     * Writes to the legacy `apply_to_*` columns are intentionally retained so
     * older code paths (SyncEngine pre-bitmask) keep observing the same
     * selections until they're migrated to read the masks directly.
     */
    private function mappingSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $profileId = (int) ($req['profile_id'] ?? 0);
        $productId = (int) ($req['product_id'] ?? 0);

        $catalogMask = $this->coerceMask($req['catalog_cycles_mask'] ?? null);
        $renewalMask = $this->coerceMask($req['renewal_cycles_mask'] ?? null);

        $markupOverrides = $this->coerceJsonObject($req['markup_overrides_json'] ?? null);
        $setupOverrides  = $this->coerceJsonObject($req['setup_fee_overrides_json'] ?? null);

        $respectDisabled = array_key_exists('respect_disabled_cycles', $req)
            ? !empty($req['respect_disabled_cycles'])
            : true;
        $overwriteFree = !empty($req['overwrite_free_cycles']);
        $syncSetupFees = !empty($req['sync_setup_fees']);

        $rounding = (string) ($req['rounding_mode'] ?? 'exact_2_decimals');
        $allowedRounding = ['exact_2_decimals', 'nearest_rupee', 'nearest_9', 'nearest_99', 'nearest_100'];
        if (!in_array($rounding, $allowedRounding, true)) {
            $rounding = 'exact_2_decimals';
        }

        try {
            // Single guarded write path — the repository whitelists schema-v3
            // columns, so a stray legacy `apply_to_*` key can never reach SQL.
            (new MappingRepository())->createOrUpdate([
                'profile_id'               => $profileId,
                'product_id'               => $productId,
                'product_group_id'         => isset($req['product_group_id']) ? (int) $req['product_group_id'] : null,
                'catalog_cycles_mask'      => $catalogMask,
                'renewal_cycles_mask'      => $renewalMask,
                'markup_overrides_json'    => $markupOverrides,
                'setup_fee_overrides_json' => $setupOverrides,
                'respect_disabled_cycles'  => $respectDisabled,
                'overwrite_free_cycles'    => $overwriteFree,
                'sync_setup_fees'          => $syncSetupFees,
                'rounding_mode'            => $rounding,
                'active'                   => true,
            ]);
        } catch (\Throwable $e) {
            $this->renderSaveError($e, 'mappings');
            return;
        }
        $this->redirect('mappings', ['flash' => 'Mapping saved.']);
    }

    /**
     * Coerce a posted mask value to an integer in [0, CycleSet::MASK_MAX].
     * Accepts decimal strings; rejects anything that doesn't parse.
     *
     * @param mixed $raw
     */
    private function coerceMask($raw): int
    {
        if ($raw === null || $raw === '') return 0;
        if (is_int($raw)) return max(0, min($raw, CycleSet::MASK_MAX));
        if (is_string($raw) && preg_match('/^\d+$/', $raw)) {
            return max(0, min((int) $raw, CycleSet::MASK_MAX));
        }
        return 0;
    }

    /**
     * Decode a JSON object payload posted from the form. Always returns an
     * associative array (possibly empty). Non-objects, arrays of non-strings,
     * or malformed JSON collapse to `[]` so the row is never written with
     * a structurally invalid blob.
     *
     * @param mixed $raw
     * @return array<string,mixed>
     */
    private function coerceJsonObject($raw): array
    {
        if (!is_string($raw) || $raw === '') return [];
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) return [];
        // Reject list-shaped JSON (e.g. `[1,2,3]`) — we only want objects keyed
        // by cycle name.
        $isList = $decoded === array_values($decoded);
        if ($isList && !empty($decoded)) return [];
        return $decoded;
    }

    /**
     * GET ajax-product-cycles?product_id=42&currency_id=1
     *
     * Returns the six recurring billing cycles with the current `tblpricing`
     * value, the classified price status (priced / free / disabled / absent),
     * and per-cycle eligibility flags for the cycles table in mappings.tpl.
     *
     * No CSRF — read-only.
     *
     * @param array<string, mixed> $req
     */
    private function ajaxProductCycles(array $req): void
    {
        try {
            $productId = (int) ($req['product_id'] ?? 0);
            if ($productId <= 0) {
                $this->jsonFail('product_id is required', 400);
                return;
            }
            $currencyId = (int) ($req['currency_id'] ?? 0);
            $currencyCode = '';
            if ($currencyId <= 0) {
                $row = null;
                try {
                    $row = Capsule::table('tblcurrencies')->where('default', 1)->first();
                } catch (\Throwable $e) {
                    $row = null;
                }
                if ($row) {
                    $row = (array) $row;
                    $currencyId = (int) ($row['id'] ?? 0);
                    $currencyCode = (string) ($row['code'] ?? '');
                }
            } else {
                try {
                    $row = Capsule::table('tblcurrencies')->where('id', $currencyId)->first();
                    if ($row) {
                        $row = (array) $row;
                        $currencyCode = (string) ($row['code'] ?? '');
                    }
                } catch (\Throwable $e) {
                    // Empty currency code is fine — caller can render '?'
                }
            }

            $pricingRow = null;
            try {
                $r = Capsule::table('tblpricing')
                    ->where('type', 'product')
                    ->where('relid', $productId)
                    ->where('currency', $currencyId)
                    ->first();
                if ($r) {
                    $pricingRow = (array) $r;
                }
            } catch (\Throwable $e) {
                $pricingRow = null;
            }

            $cycles = [];
            foreach (CycleSet::allCycles() as $cycleName) {
                $recCol   = CyclePricingMap::getRecurringColumn($cycleName);
                $setupCol = CyclePricingMap::getSetupFeeColumn($cycleName);
                $months   = CyclePricingMap::getCycleMonths($cycleName);

                $currentPrice = null;
                $currentSetup = null;
                if ($pricingRow !== null && $recCol !== null && array_key_exists($recCol, $pricingRow)) {
                    $val = $pricingRow[$recCol];
                    $currentPrice = ($val === null || $val === '') ? null : (float) $val;
                }
                if ($pricingRow !== null && $setupCol !== null && array_key_exists($setupCol, $pricingRow)) {
                    $val = $pricingRow[$setupCol];
                    $currentSetup = ($val === null || $val === '') ? null : (float) $val;
                }

                $status = CyclePricingMap::priceStatusFromValue($currentPrice);
                $canCatalogSync = in_array($status, ['priced', 'free'], true);
                $canRenewalSync = true; // all 6 recurring cycles are renewable

                $cycles[] = [
                    'cycle'              => $cycleName,
                    'months'             => $months,
                    'recurring_column'   => $recCol,
                    'setup_fee_column'   => $setupCol,
                    'current_price'      => $currentPrice === null ? null : round($currentPrice, 4),
                    'current_setup_fee'  => $currentSetup === null ? null : round($currentSetup, 4),
                    'status'             => $status,
                    'can_catalog_sync'   => $canCatalogSync,
                    'can_renewal_sync'   => $canRenewalSync,
                ];
            }

            $this->jsonOk([
                'product_id'    => $productId,
                'currency_id'   => $currencyId,
                'currency_code' => $currencyCode,
                'cycles'        => $cycles,
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    private function syncHistory(): void
    {
        $rows = Capsule::table('mod_contabo_sync_log')
            ->orderByDesc('id')->limit(200)
            ->get()->map(static fn ($r) => (array) $r)->all();
        $this->render('sync_history.tpl', ['logs' => $rows]);
    }

    private function syncRun(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        $engine = new SyncEngine($this->settings, new ApiClient($this->settings), new ProfileManager($this->settings));
        $summary = $engine->run('manual');
        $this->render('sync_run_result.tpl', ['summary' => $summary]);
    }

    private function refreshApi(): void
    {
        if (!$this->verifyToken()) { return; }
        $api = new ApiClient($this->settings);
        try {
            $r = $api->refresh();
            $this->redirect('dashboard', ['flash' => "Refresh queued: {$r['job_id']}"]);
        } catch (\Throwable $e) {
            $this->redirect('dashboard', ['flash' => 'Refresh failed: ' . $e->getMessage()]);
        }
    }

    private function settingsView(): void
    {
        $this->render('settings.tpl', ['settings' => $this->settings]);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Validates the WHMCS-managed CSRF token on mutating requests. Uses the
     * built-in `check_token()` helper which is loaded globally by WHMCS and
     * verifies $_POST['token'] (or appended ?token= for 'link' mode) against
     * the admin session. On mismatch WHMCS throws ProgrammerException; we
     * catch it and render an inline error rather than a stack trace.
     *
     * Returns true on success, false on failure (caller should `return;`).
     */
    private function verifyToken(): bool
    {
        if (!function_exists('check_token')) {
            // Defensive: only happens outside a real WHMCS request lifecycle.
            echo '<div class="errorbox">CSRF check unavailable: WHMCS helpers not loaded.</div>';
            return false;
        }
        try {
            check_token();
            return true;
        } catch (\Throwable $e) {
            echo '<div class="errorbox"><strong>Invalid security token.</strong> '
                . 'Please return to the previous page and try again. '
                . '<a href="' . htmlspecialchars($this->settings->moduleLink, ENT_QUOTES, 'UTF-8') . '">Back to dashboard</a></div>';
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing CSRF token validation failed: ' . $e->getMessage());
            }
            return false;
        }
    }

    /**
     * Minimal Smarty-style rendering. WHMCS exposes Smarty via global $smarty
     * but to keep this addon decoupled from internals we use a tiny PHP-include
     * template loop instead — every .tpl is plain PHP under a controlled scope.
     */
    private function render(string $tpl, array $data): void
    {
        $path = $this->templateDir . '/' . $tpl;
        if (!is_file($path)) {
            echo '<div class="errorbox">Template missing: ' . htmlspecialchars($tpl) . '</div>';
            return;
        }
        $data['module_link'] = $this->settings->moduleLink;
        $data['esc'] = static fn ($v): string => htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        // Drives the layout's asset cache-buster (app.js?v=…). Bound to the
        // addon version so every release invalidates the cached JS/CSS.
        if (!isset($data['cb_addon_version'])) {
            $data['cb_addon_version'] = self::VERSION;
        }
        extract($data, EXTR_SKIP);
        require $path;
    }

    /** @param array<string, mixed> $extra */
    private function redirect(string $action, array $extra = []): void
    {
        $qs = http_build_query(array_merge(['action' => $action], $extra));
        header('Location: ' . $this->settings->moduleLink . '&' . $qs);
        exit;
    }

    // ── A.5.1 — schema health guard + friendly error handling ────────────────

    /**
     * Run the schema auto-health check before a mutating admin action. On a
     * migration failure, render a friendly error (not a stack trace) and
     * return false so the caller aborts.
     */
    private function guardSchema(): bool
    {
        $r = SchemaHealth::assertOrMigrate();
        if (!empty($r['ok'])) {
            return true;
        }
        echo '<div class="errorbox"><strong>Contabo Pricing: schema update required.</strong> '
            . 'An automatic migration was attempted but failed: '
            . htmlspecialchars((string) ($r['error'] ?? 'unknown error'), ENT_QUOTES, 'UTF-8')
            . ' Visit <a href="' . htmlspecialchars($this->settings->moduleLink, ENT_QUOTES, 'UTF-8')
            . '&action=maintenance">Maintenance</a> to run migrations manually.</div>';
        return false;
    }

    /**
     * Translate a save-path exception into a friendly admin message instead of
     * leaking raw SQLSTATE. Full technical detail goes to the activity log.
     */
    private function renderSaveError(\Throwable $e, string $backAction): void
    {
        $msg = $e->getMessage();
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing save error (' . $backAction . '): ' . $msg);
        }
        $back = htmlspecialchars($this->settings->moduleLink . '&action=' . $backAction, ENT_QUOTES, 'UTF-8');

        if (strpos($msg, '42S22') !== false) {
            // Missing column → schema drift. Try to self-heal, then prompt retry.
            $heal = SchemaHealth::assertOrMigrate();
            $healed = !empty($heal['ok']);
            echo '<div class="errorbox"><strong>Database schema was out of date.</strong> '
                . ($healed
                    ? 'It has been repaired automatically — please retry your action.'
                    : 'Automatic repair failed; visit Maintenance to migrate.')
                . ' <a href="' . $back . '">Go back</a></div>';
            return;
        }
        if (strpos($msg, '23000') !== false) {
            echo '<div class="errorbox"><strong>That entry already exists.</strong> '
                . 'A record with the same unique key is already present. '
                . '<a href="' . $back . '">Go back</a></div>';
            return;
        }
        echo '<div class="errorbox"><strong>Could not save.</strong> '
            . 'The operation failed; the technical detail has been written to the activity log. '
            . '<a href="' . $back . '">Go back</a></div>';
    }

    // ── A.5.1 — maintenance page ─────────────────────────────────────────────

    private function maintenance(): void
    {
        $this->render('maintenance.tpl', [
            'cb_schema_health' => SchemaHealth::requiredColumnsPresent(),
            'cb_schema_target' => Installer::SCHEMA_VERSION,
            'cb_purge_phrase'  => SchemaHealth::PURGE_CONFIRMATION_PHRASE,
        ]);
    }

    private function maintenanceMigrate(): void
    {
        if (!$this->verifyToken()) { return; }
        $r = SchemaHealth::assertOrMigrate();
        $flash = !empty($r['ok'])
            ? 'Schema migrated to v' . (int) ($r['to'] ?? 0) . '.'
            : 'Migration failed: ' . (string) ($r['error'] ?? 'unknown');
        $this->redirect('maintenance', ['flash' => $flash]);
    }

    private function maintenancePurge(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (empty($req['purge_confirm_checkbox'])) {
            $this->redirect('maintenance', ['flash' => 'Purge cancelled — confirmation checkbox not ticked.']);
            return;
        }
        $phrase = (string) ($req['purge_confirmation_phrase'] ?? '');
        if (!SchemaHealth::isPurgeConfirmed($phrase)) {
            $this->redirect('maintenance', ['flash' => 'Purge cancelled — confirmation phrase did not match.']);
            return;
        }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing: PURGE requested by admin #' . $adminId . ' — backing up then truncating mod_contabo_* tables.');
        }

        try {
            // Back up every addon table to a timestamped *_purgebackup_ table,
            // then truncate. NEVER touches non mod_contabo_* tables.
            $stamp = date('YmdHis');
            $tables = $this->addonTables();
            foreach ($tables as $t) {
                $backup = $t . '_purgebackup_' . $stamp;
                Capsule::connection()->statement("CREATE TABLE `{$backup}` LIKE `{$t}`");
                Capsule::connection()->statement("INSERT INTO `{$backup}` SELECT * FROM `{$t}`");
                Capsule::table($t)->truncate();
            }
            // Re-seed schema_version so the addon still knows its version.
            Capsule::table('mod_contabo_settings')->updateOrInsert(
                ['key' => 'schema_version'],
                ['value' => (string) Installer::SCHEMA_VERSION, 'updated_at' => date('Y-m-d H:i:s')]
            );
            $this->redirect('maintenance', ['flash' => 'Module data purged. Backup tables created with suffix _purgebackup_' . $stamp . '.']);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: purge failed — ' . $e->getMessage());
            }
            $this->redirect('maintenance', ['flash' => 'Purge failed; see activity log. No tables were truncated past the failure point.']);
        }
    }

    /**
     * The set of addon-owned tables the purge may touch. STRICTLY mod_contabo_*.
     * @return list<string>
     */
    private function addonTables(): array
    {
        $rows = Capsule::connection()->select('SHOW TABLES LIKE "mod_contabo_%"');
        $out = [];
        foreach ($rows as $r) {
            foreach ((array) $r as $v) {
                $name = (string) $v;
                // Defence in depth: never include a backup table or anything
                // that doesn't start with the addon prefix.
                if (strpos($name, 'mod_contabo_') === 0 && strpos($name, '_purgebackup_') === false) {
                    $out[] = $name;
                }
            }
        }
        return $out;
    }

    // ── Renewal Pricing Policy Engine — Phase A pages ────────────────────────
    //
    // All five views are READ-ONLY in Phase A (tax-settings has a single write
    // path for the `tax_registration_mode` row, guarded by check_token()). The
    // engine is in `observe` phase so nothing here moves prices; we visualise
    // what would have happened.

    /**
     * Repricing dashboard — KPI strip + recent decisions table.
     *
     * The decision table reads the most recent 200 rows out of
     * `mod_contabo_price_decision`. Filter pills (Applied / Skipped / Awaiting
     * approval / Notice scheduled) and search are wired client-side via the
     * existing `data-cb-filter` infra in app.js.
     */
    private function repricingDashboard(): void
    {
        $services_tracked = 0;
        $decisions_today  = 0;
        $applied_today    = 0;
        $awaiting         = 0;
        $notices_scheduled = 0;
        $rows = [];

        try {
            $todayStart = date('Y-m-d 00:00:00');
            if (\WHMCS\Database\Capsule::schema()->hasTable('mod_contabo_service_policy')) {
                $services_tracked = (int) Capsule::table('mod_contabo_service_policy')->count();
            }
            if (Capsule::schema()->hasTable('mod_contabo_price_decision')) {
                $decisions_today = (int) Capsule::table('mod_contabo_price_decision')
                    ->where('decided_at', '>=', $todayStart)->count();
                $applied_today = (int) Capsule::table('mod_contabo_price_decision')
                    ->where('decided_at', '>=', $todayStart)
                    ->where('applied', true)->count();
                $awaiting = (int) Capsule::table('mod_contabo_price_decision')
                    ->where('requires_admin_approval', true)
                    ->where('applied', false)->count();
                $rows = Capsule::table('mod_contabo_price_decision')
                    ->orderByDesc('decided_at')
                    ->limit(200)
                    ->get()->map(static fn ($r) => (array) $r)->all();
            }
            if (Capsule::schema()->hasTable('mod_contabo_price_notice')) {
                $notices_scheduled = (int) Capsule::table('mod_contabo_price_notice')
                    ->whereIn('status', ['pending'])->count();
            }
        } catch (\Throwable $e) {
            // schema-not-ready: dashboard renders with zeros + an inline note.
        }

        $cycleStats = $this->computeCycleStats();

        $this->render('repricing.tpl', [
            'services_tracked'   => $services_tracked,
            'decisions_today'    => $decisions_today,
            'applied_today'      => $applied_today,
            'awaiting'           => $awaiting,
            'notices_scheduled'  => $notices_scheduled,
            'rows'               => $rows,
            'phase'              => $this->readSetting('repricing_phase', 'observe'),
            'cycle_breakdown'    => $cycleStats['breakdown'],
            'cycle_exposure'     => $cycleStats['exposure_count'],
            'cycle_exposure_svc' => $cycleStats['exposure_services'],
        ]);
    }

    /**
     * Compute the cycle KPI tiles for the repricing dashboard:
     *   - "Services per cycle"      → count of mapped services grouped by
     *                                  billingcycle (joined to mod_contabo_mapping).
     *   - "Cycle exposure"          → count of services where billingcycle is
     *                                  recurring but the mapping's
     *                                  renewal_cycles_mask doesn't include it
     *                                  (i.e. would skip with cycle_not_mapped).
     *
     * Returns ['breakdown' => [cycle => count], 'exposure_count' => int,
     *          'exposure_services' => list<int>].
     *
     * @return array{breakdown: array<string,int>, exposure_count: int, exposure_services: list<int>}
     */
    private function computeCycleStats(): array
    {
        $breakdown = [];
        foreach (CycleSet::allCycles() as $c) {
            $breakdown[$c] = 0;
        }
        $exposureCount = 0;
        $exposureServices = [];

        try {
            $hasHosting = Capsule::schema()->hasTable('tblhosting');
            $hasMapping = Capsule::schema()->hasTable('mod_contabo_mapping');
            if (!$hasHosting || !$hasMapping) {
                return ['breakdown' => $breakdown, 'exposure_count' => 0, 'exposure_services' => []];
            }

            // Load all active mappings keyed by product_id. We avoid a SQL JOIN
            // so the same code path works under the test FakeCapsule (which
            // has no join support).
            $mappingsByProduct = [];
            $mappings = Capsule::table('mod_contabo_mapping')
                ->where('active', true)
                ->get();
            foreach ($mappings as $m) {
                $row = (array) $m;
                $pid = (int) ($row['product_id'] ?? 0);
                if ($pid <= 0) continue;
                $mappingsByProduct[$pid] = $row;
            }
            if (empty($mappingsByProduct)) {
                return ['breakdown' => $breakdown, 'exposure_count' => 0, 'exposure_services' => []];
            }

            $svcs = Capsule::table('tblhosting')->get();
            foreach ($svcs as $s) {
                $svc = (array) $s;
                $pid = (int) ($svc['packageid'] ?? 0);
                if (!isset($mappingsByProduct[$pid])) continue;
                $cycle = (string) ($svc['billingcycle'] ?? '');
                if (!CyclePricingMap::isRecurringCycleSupported($cycle)) continue;
                $breakdown[$cycle] = ($breakdown[$cycle] ?? 0) + 1;

                $m = $mappingsByProduct[$pid];
                $renewalMask = isset($m['renewal_cycles_mask']) ? (int) $m['renewal_cycles_mask'] : 0;
                $set = CycleSet::fromMask($renewalMask);
                if (!$set->contains($cycle)) {
                    $exposureCount++;
                    $svcId = (int) ($svc['id'] ?? 0);
                    if ($svcId > 0 && count($exposureServices) < 50) {
                        $exposureServices[] = $svcId;
                    }
                }
            }
        } catch (\Throwable $e) {
            // Defensive: missing columns / schema-not-ready returns zero stats.
        }

        return [
            'breakdown'        => $breakdown,
            'exposure_count'   => $exposureCount,
            'exposure_services' => $exposureServices,
        ];
    }

    /**
     * Full audit log with date-range + policy + skip_reason + free-text filters.
     * Sorting is handled client-side by app.js (`data-cb-table="price-decisions"`).
     * Pagination is page-window of 500 rows max to keep DOM responsive.
     *
     * @param array<string, mixed> $req
     */
    private function priceDecisions(array $req): void
    {
        $from = (string) ($req['from'] ?? '');
        $to   = (string) ($req['to'] ?? '');
        $policy = (string) ($req['policy'] ?? '');
        $skipReason = (string) ($req['skip_reason'] ?? '');
        $cycle = (string) ($req['cycle'] ?? '');

        $rows = [];
        $policies = [];
        $skipReasons = [];
        try {
            if (Capsule::schema()->hasTable('mod_contabo_price_decision')) {
                $q = Capsule::table('mod_contabo_price_decision');
                if ($from !== '') { $q->where('decided_at', '>=', $from . ' 00:00:00'); }
                if ($to !== '')   { $q->where('decided_at', '<=', $to   . ' 23:59:59'); }
                if ($policy !== '')     { $q->where('policy_used', $policy); }
                if ($skipReason !== '') { $q->where('skip_reason', $skipReason); }
                if ($cycle !== '' && CyclePricingMap::isRecurringCycleSupported($cycle)) {
                    $q->where('billing_cycle', $cycle);
                }
                $rows = $q->orderByDesc('decided_at')->limit(500)
                    ->get()->map(static fn ($r) => (array) $r)->all();

                $policies = Capsule::table('mod_contabo_price_decision')
                    ->select('policy_used')->distinct()->orderBy('policy_used')
                    ->pluck('policy_used')->all();
                $skipReasons = Capsule::table('mod_contabo_price_decision')
                    ->select('skip_reason')->whereNotNull('skip_reason')
                    ->distinct()->orderBy('skip_reason')
                    ->pluck('skip_reason')->all();
            }
        } catch (\Throwable $e) {
            // schema-not-ready: empty table renders + the inline note.
        }

        $this->render('price_decisions.tpl', [
            'rows'                   => $rows,
            'available_policies'     => $policies,
            'available_skip_reasons' => $skipReasons,
            'filter_from'            => $from,
            'filter_to'              => $to,
            'filter_policy'          => $policy,
            'filter_skip_reason'     => $skipReason,
            'filter_cycle'           => $cycle,
        ]);
    }

    /**
     * CSV export of price decisions. POST-only with CSRF check; emits a
     * `text/csv` stream + standard header row + every column we display in
     * the audit log table. Filters honoured if present.
     *
     * @param array<string, mixed> $req
     */
    private function priceDecisionsCsv(array $req): void
    {
        if (!$this->verifyToken()) { return; }

        $from = (string) ($req['from'] ?? '');
        $to   = (string) ($req['to'] ?? '');
        $policy = (string) ($req['policy'] ?? '');
        $skipReason = (string) ($req['skip_reason'] ?? '');
        $cycle = (string) ($req['cycle'] ?? '');

        $cols = [
            'id', 'service_id', 'decided_at', 'effective_at',
            'billing_cycle', 'cycle_months', 'currency',
            'old_price', 'proposed_new_price',
            'vendor_cost_local_for_cycle', 'sell_price_gross_for_cycle',
            'margin_amount_for_cycle', 'margin_pct',
            'tax_mode_snapshot', 'policy_used', 'applied', 'applied_via',
            'skip_reason', 'requires_admin_approval',
        ];

        if (!headers_sent()) {
            $filename = 'contabo-price-decisions-' . date('Ymd-His') . '.csv';
            header('Content-Type: text/csv; charset=utf-8');
            header('Content-Disposition: attachment; filename="' . $filename . '"');
            header('X-Content-Type-Options: nosniff');
        }

        $out = fopen('php://output', 'w');
        fputcsv($out, $cols);

        try {
            if (Capsule::schema()->hasTable('mod_contabo_price_decision')) {
                $q = Capsule::table('mod_contabo_price_decision');
                if ($from !== '') { $q->where('decided_at', '>=', $from . ' 00:00:00'); }
                if ($to !== '')   { $q->where('decided_at', '<=', $to   . ' 23:59:59'); }
                if ($policy !== '')     { $q->where('policy_used', $policy); }
                if ($skipReason !== '') { $q->where('skip_reason', $skipReason); }
                if ($cycle !== '' && CyclePricingMap::isRecurringCycleSupported($cycle)) {
                    $q->where('billing_cycle', $cycle);
                }
                $q->orderBy('decided_at')->chunk(500, static function ($chunk) use ($cols, $out): void {
                    foreach ($chunk as $r) {
                        $row = (array) $r;
                        $line = [];
                        foreach ($cols as $c) {
                            $line[] = isset($row[$c]) ? (string) $row[$c] : '';
                        }
                        fputcsv($out, $line);
                    }
                });
            }
        } catch (\Throwable $e) {
            fputcsv($out, ['ERROR', $e->getMessage()]);
        }
        fclose($out);
        exit;
    }

    /**
     * "Skipped" report — services where `applied = false`, grouped by
     * skip_reason. Each group renders a sub-table with old → proposed_new
     * deltas. The "cost exposure" tile sums (proposed_new_price - old_price)
     * across all non-applied rows that the engine FLAGGED as actionable
     * (excluding phase/observe skips which by definition aren't exposure).
     */
    private function skippedReport(): void
    {
        $groups = [];
        $cost_exposure = 0.0;
        $exposureReasons = [
            'awaiting_admin_approval',
            'awaiting_force_approval_max_increase_exceeded',
            'within_notice_window',
            'notice_scheduled',
            'notice_failed',
        ];

        try {
            if (Capsule::schema()->hasTable('mod_contabo_price_decision')) {
                $rows = Capsule::table('mod_contabo_price_decision')
                    ->where('applied', false)
                    ->whereNotNull('skip_reason')
                    ->orderByDesc('decided_at')
                    ->limit(5000)
                    ->get()->map(static fn ($r) => (array) $r)->all();

                foreach ($rows as $r) {
                    $reason = (string) ($r['skip_reason'] ?? 'unknown');
                    if (!isset($groups[$reason])) {
                        $groups[$reason] = ['reason' => $reason, 'count' => 0, 'rows' => []];
                    }
                    $groups[$reason]['count']++;
                    if (count($groups[$reason]['rows']) < 100) {
                        $groups[$reason]['rows'][] = $r;
                    }
                    if (in_array($reason, $exposureReasons, true)) {
                        $cost_exposure += (float) ($r['proposed_new_price'] ?? 0)
                                        - (float) ($r['old_price'] ?? 0);
                    }
                }
                // Sort groups by count desc.
                uasort($groups, static fn ($a, $b) => $b['count'] <=> $a['count']);
            }
        } catch (\Throwable $e) {
            // schema-not-ready: empty groups render + inline note.
        }

        $this->render('skipped_report.tpl', [
            'groups'        => array_values($groups),
            'cost_exposure' => $cost_exposure,
            'currency'      => $this->settings->currencyIso,
        ]);
    }

    /**
     * Tax mode settings page. Read-only display of the 8 modes + worked
     * example + a (write-enabled) form to switch the active mode and toggle
     * the recovery flags. Form POSTs back to `tax-settings-save`.
     */
    private function taxSettings(): void
    {
        $current = [
            'tax_registration_mode'        => $this->readSetting('tax_registration_mode', \ContaboPricing\TaxModeEngine::defaultMode()),
            'vendor_tax_rate_pct'          => $this->readSetting('vendor_tax_rate_pct', '18.00'),
            'vendor_tax_recoverable'       => $this->readSetting('vendor_tax_recoverable', '0'),
            'charge_output_tax_to_client'  => $this->readSetting('charge_output_tax_to_client', '0'),
            'prices_include_output_tax'    => $this->readSetting('prices_include_output_tax', '0'),
            'output_tax_rate_pct'          => $this->readSetting('output_tax_rate_pct', '0.00'),
        ];

        $this->render('tax_settings.tpl', [
            'current'        => $current,
            'modes'          => \ContaboPricing\TaxModeEngine::modes(),
            'mode_summaries' => $this->buildModeSummaries(),
        ]);
    }

    /**
     * Persist a tax-settings form post. Wraps in check_token() per the hard
     * rule; writes the six keys via Capsule::updateOrInsert; emits a
     * `mod_contabo_pricing_action` row with action_type='phase_changed' so
     * the change is auditable.
     *
     * @param array<string, mixed> $req
     */
    private function taxSettingsSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }

        $mode = (string) ($req['tax_registration_mode'] ?? '');
        if (!\ContaboPricing\TaxModeEngine::isValid($mode)) {
            $this->redirect('tax-settings', ['flash' => 'Invalid tax mode rejected.']);
            return;
        }

        $values = [
            'tax_registration_mode'       => $mode,
            'vendor_tax_rate_pct'         => (string) (float) ($req['vendor_tax_rate_pct'] ?? '18.00'),
            'vendor_tax_recoverable'      => !empty($req['vendor_tax_recoverable']) ? '1' : '0',
            'charge_output_tax_to_client' => !empty($req['charge_output_tax_to_client']) ? '1' : '0',
            'prices_include_output_tax'   => !empty($req['prices_include_output_tax']) ? '1' : '0',
            'output_tax_rate_pct'         => (string) (float) ($req['output_tax_rate_pct'] ?? '0.00'),
        ];

        $now = date('Y-m-d H:i:s');
        try {
            foreach ($values as $key => $val) {
                Capsule::table('mod_contabo_settings')->updateOrInsert(
                    ['key' => $key],
                    ['value' => $val, 'updated_at' => $now],
                );
            }
            if (Capsule::schema()->hasTable('mod_contabo_pricing_action')) {
                $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
                Capsule::table('mod_contabo_pricing_action')->insert([
                    'action_type' => 'phase_changed',
                    'service_id'  => null,
                    'decision_id' => null,
                    'schedule_id' => null,
                    'admin_id'    => $adminId,
                    'reason'      => 'Tax settings updated: mode=' . $mode,
                    'created_at'  => $now,
                ]);
            }
            $this->redirect('tax-settings', ['flash' => 'Tax settings saved.']);
        } catch (\Throwable $e) {
            $this->redirect('tax-settings', ['flash' => 'Save failed: ' . $e->getMessage()]);
        }
    }

    /**
     * AJAX dry-run preview: given a service_id (and optional hypothetical
     * policy + options) return what RenewalEngine *would* propose without
     * persisting anything. Mirrors the read-only ajax-fx / ajax-quote pattern.
     *
     * Phase A: if RenewalEngine isn't deployed yet, we return a graceful
     * stub payload so the UI can render "preview unavailable" rather than
     * crash.
     *
     * @param array<string, mixed> $req
     */
    private function ajaxPolicyPreview(array $req): void
    {
        try {
            $serviceId = (int) ($req['service_id'] ?? 0);
            if ($serviceId <= 0) {
                $this->jsonFail('service_id is required', 400);
                return;
            }
            $hypotheticalPolicy = (string) ($req['policy'] ?? '');

            if (!class_exists('\\ContaboPricing\\RenewalEngine')) {
                $this->jsonOk([
                    'service_id' => $serviceId,
                    'available'  => false,
                    'reason'     => 'RenewalEngine not deployed yet (Phase A optional).',
                ]);
                return;
            }

            /** @var class-string $engineCls */
            $engineCls = '\\ContaboPricing\\RenewalEngine';
            $engine = new $engineCls();

            // Prefer an explicit dry-run method; fall back to decide() with a
            // dry-run flag if the engine exposes that signature.
            $result = null;
            if (method_exists($engine, 'previewDecision')) {
                $result = $engine->previewDecision($serviceId, $hypotheticalPolicy);
            } elseif (method_exists($engine, 'decide')) {
                $result = $engine->decide($serviceId, /* cron_run_id */ null, /* dry_run */ true, $hypotheticalPolicy);
            }

            if (!is_array($result)) {
                $result = ['raw' => $result];
            }
            $this->jsonOk([
                'service_id' => $serviceId,
                'available'  => true,
                'policy'     => $hypotheticalPolicy,
                'preview'    => $result,
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    /**
     * Renders the per-service "Contabo Pricing" tab content that's embedded
     * in the WHMCS admin service profile via AdminClientServicesTabFields.
     *
     * Public because it's called from hooks.php (not the dispatch table).
     * Returns rendered HTML as a string — WHMCS injects it into the tab pane.
     *
     * Phase A: read-only. Shows current policy badge, locked_price, last 5
     * decisions for the service, and the catalog-vs-applied price diff.
     */
    public function servicePricingTabContent(int $serviceId): string
    {
        if ($serviceId <= 0) {
            return '';
        }
        $policy = null;
        $decisions = [];
        $service = null;
        $latestApplied = null;
        try {
            $service = Capsule::table('tblhosting')->where('id', $serviceId)->first();
            if (!$service) {
                return '';
            }
            // Only render the tab for services actually mapped to a Contabo profile.
            $mapping = Capsule::table('mod_contabo_mapping')
                ->where('product_id', (int) $service->packageid)
                ->where('active', true)
                ->first();
            if (!$mapping) {
                return '';
            }
            if (Capsule::schema()->hasTable('mod_contabo_service_policy')) {
                $policy = Capsule::table('mod_contabo_service_policy')
                    ->where('service_id', $serviceId)->first();
                if ($policy) { $policy = (array) $policy; }
            }
            if (Capsule::schema()->hasTable('mod_contabo_price_decision')) {
                $decisions = Capsule::table('mod_contabo_price_decision')
                    ->where('service_id', $serviceId)
                    ->orderByDesc('decided_at')
                    ->limit(5)
                    ->get()->map(static fn ($r) => (array) $r)->all();
                $latestApplied = Capsule::table('mod_contabo_price_decision')
                    ->where('service_id', $serviceId)
                    ->where('applied', true)
                    ->orderByDesc('decided_at')
                    ->first();
                if ($latestApplied) { $latestApplied = (array) $latestApplied; }
            }
        } catch (\Throwable $e) {
            return '<div style="color:#b91c1c">Contabo Pricing tab render error: '
                . htmlspecialchars($e->getMessage(), ENT_QUOTES, 'UTF-8') . '</div>';
        }

        $path = $this->templateDir . '/service_pricing_tab.tpl';
        if (!is_file($path)) {
            return '';
        }
        $esc = static fn ($v): string => htmlspecialchars((string) $v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
        ob_start();
        $data = [
            'service'          => (array) $service,
            'policy_row'       => $policy,
            'decisions'        => $decisions,
            'latest_applied'   => $latestApplied,
            'module_link'      => $this->settings->moduleLink,
            'esc'              => $esc,
        ];
        extract($data, EXTR_SKIP);
        require $path;
        return (string) ob_get_clean();
    }

    /**
     * Read a string value out of `mod_contabo_settings`. Returns $default if
     * the table doesn't exist yet (install-but-not-migrated state) or the
     * row is absent.
     */
    private function readSetting(string $key, string $default): string
    {
        try {
            if (!Capsule::schema()->hasTable('mod_contabo_settings')) {
                return $default;
            }
            $v = Capsule::table('mod_contabo_settings')->where('key', $key)->value('value');
            return $v === null ? $default : (string) $v;
        } catch (\Throwable $e) {
            return $default;
        }
    }

    /**
     * Build a UI-friendly array of the eight tax modes with their behavior
     * flags, for the tax-settings.tpl table.
     *
     * @return list<array{mode:string,label:string,output_tax_charged:bool,vendor_tax_recoverable:bool,prices_include_output_tax:bool}>
     */
    private function buildModeSummaries(): array
    {
        $labels = [
            \ContaboPricing\TaxModeEngine::MODE_UNREGISTERED                  => 'Unregistered — no output tax (default)',
            \ContaboPricing\TaxModeEngine::MODE_REG_EXCLUSIVE_RECOVERABLE     => 'Registered · tax-exclusive · recoverable',
            \ContaboPricing\TaxModeEngine::MODE_REG_EXCLUSIVE_NON_RECOVERABLE => 'Registered · tax-exclusive · non-recoverable',
            \ContaboPricing\TaxModeEngine::MODE_REG_INCLUSIVE_RECOVERABLE     => 'Registered · tax-inclusive · recoverable',
            \ContaboPricing\TaxModeEngine::MODE_REG_INCLUSIVE_NON_RECOVERABLE => 'Registered · tax-inclusive · non-recoverable',
            \ContaboPricing\TaxModeEngine::MODE_NO_TAX_APPLICABLE             => 'No tax applicable in jurisdiction',
            \ContaboPricing\TaxModeEngine::MODE_TAX_EXEMPT_CUSTOMER           => 'Tax-exempt customer (per-service)',
            \ContaboPricing\TaxModeEngine::MODE_CUSTOM_MANUAL                 => 'Custom — admin-managed flags',
        ];
        $out = [];
        foreach (\ContaboPricing\TaxModeEngine::modes() as $mode) {
            $s = \ContaboPricing\TaxModeEngine::summary($mode);
            $out[] = [
                'mode'                      => $mode,
                'label'                     => $labels[$mode] ?? $mode,
                'output_tax_charged'        => (bool) $s['output_tax_charged'],
                'vendor_tax_recoverable'    => (bool) $s['vendor_tax_recoverable'],
                'prices_include_output_tax' => (bool) $s['prices_include_output_tax'],
            ];
        }
        return $out;
    }
}
