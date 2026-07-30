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
    public const VERSION = '1.0.0';

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
            case 'profiles-trash':   $this->profilesTrash($req); return;
            case 'profile-create':   $this->profileCreate($req); return;
            case 'profile-save':     $this->profileSave($req); return;
            case 'profile-toggle':   $this->profileToggle($req); return;
            case 'profile-delete':   $this->profileDelete($req); return;
            case 'profile-restore':  $this->profileRestore($req); return;
            case 'profile-purge':    $this->profilePurge($req); return;
            case 'profile-diff':     $this->profileDiff($req); return;
            case 'config-preview':   $this->configPreview($req); return;
            case 'config-apply':     $this->configApply($req); return;
            case 'config-diff':      $this->configDiff($req); return;
            case 'config-exposure':      $this->configExposure($req); return;
            case 'config-exposure-save': $this->configExposureSave($req); return;
            // ── A.6.3 — capability + compatibility editors ───────────────────
            case 'capability-editor':       $this->capabilityEditor($req); return;
            case 'capability-editor-save':  $this->capabilityEditorSave($req); return;
            case 'compatibility-editor':      $this->compatibilityEditor($req); return;
            case 'compatibility-editor-save': $this->compatibilityEditorSave($req); return;
            case 'mappings':         $this->mappings($req); return;
            case 'mapping-save':     $this->mappingSave($req); return;
            case 'mapping-publication-preview': $this->mappingPublicationPreview($req); return;
            case 'mapping-publication-approve': $this->mappingPublicationApprove($req); return;
            case 'catalog-import':    $this->catalogImport(); return;
            case 'operations':        $this->operations($req); return;
            case 'operation-detail':  $this->operationDetail($req); return;
            case 'operation-command': $this->operationCommand($req); return;
            case 'adoption-approve': $this->adoptionApprove($req); return;
            case 'capability-certify': $this->capabilityCertify($req); return;
            case 'provider-write-control': $this->providerWriteControl($req); return;
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
            case 'currency-report':      $this->currencyReport(); return;
            case 'currency-report-csv':  $this->currencyReportCsv(); return;
            // ── Phase C — approval queue ─────────────────────────────────────
            case 'approval-queue':       $this->approvalQueue($req); return;
            case 'approval-approve':     $this->approvalApprove($req); return;
            case 'approval-reject':      $this->approvalReject($req); return;
            case 'ajax-approval-count':  $this->ajaxApprovalCount(); return;
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
            // Trashed profiles are excluded by default (listProfiles filters them).
            'profiles' => $this->annotateDrift($pm->listProfiles(false), $plans),
            'available_plans' => $plans,
            'flash' => (string) ($req['flash'] ?? ''),
            // When a delete just happened, the page renders an inline Undo for it.
            'undo_id' => isset($req['undo_id']) ? (int) $req['undo_id'] : 0,
            'trash_count' => count($pm->listTrashed()),
        ]);
    }

    /**
     * Trash view — soft-deleted profiles with Restore (Undo) + guarded permanent
     * Purge. Reuses profiles.tpl in trash mode.
     *
     * @param array<string,mixed> $req
     */
    private function profilesTrash(array $req): void
    {
        $pm = new ProfileManager($this->settings);
        $trashed = $pm->listTrashed();

        // Annotate each trashed row with its purge eligibility so the template can
        // enable/disable the permanent-delete control + show why it's blocked.
        $purge = new ProfilePurgeService();
        foreach ($trashed as &$row) {
            $row['purge'] = $purge->assess((int) ($row['id'] ?? 0));
        }
        unset($row);

        $this->render('profiles.tpl', [
            'profiles'        => [],
            'available_plans' => [],
            'flash'           => (string) ($req['flash'] ?? ''),
            'trash_mode'      => true,
            'trashed'         => $trashed,
            'trash_count'     => count($trashed),
            'cb_purge_phrase' => SchemaHealth::PURGE_CONFIRMATION_PHRASE,
        ]);
    }

    /**
     * Soft-delete (Trash) a profile. Recoverable: the redirect carries undo_id so
     * the profiles page renders an inline "Deleted — Undo" action.
     *
     * @param array<string,mixed> $req
     */
    private function profileDelete(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $row = $pm->find($id);
        if ($row === null) {
            $this->redirect('profiles', ['flash' => 'Profile not found.']);
            return;
        }
        $pm->softDelete($id);
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing: profile #' . $id . ' (' . (string) ($row['slug'] ?? '') . ') moved to Trash.');
        }
        $this->redirect('profiles', [
            'flash'   => 'Profile "' . (string) ($row['name'] ?? $row['slug'] ?? ('#' . $id)) . '" moved to Trash.',
            'undo_id' => $id,
        ]);
    }

    /**
     * Restore a trashed profile (the Undo).
     *
     * @param array<string,mixed> $req
     */
    private function profileRestore(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $row = $pm->find($id);
        if ($row === null) {
            $this->redirect('profiles', ['flash' => 'Profile not found.']);
            return;
        }
        $pm->restore($id);
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing: profile #' . $id . ' restored from Trash.');
        }
        // Land back wherever made sense: trash if others remain, else profiles.
        $dest = count($pm->listTrashed()) > 0 ? 'profiles-trash' : 'profiles';
        $this->redirect($dest, ['flash' => 'Profile restored.']);
    }

    /**
     * Permanently purge a trashed profile + everything it owns. Guarded:
     * blocked while an active mapping or live service references it; requires the
     * typed confirmation phrase; cascades only this profile's rows; audited.
     *
     * @param array<string,mixed> $req
     */
    private function profilePurge(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        $pm = new ProfileManager($this->settings);
        $id = (int) ($req['id'] ?? 0);
        $row = $pm->find($id);
        if ($row === null) {
            $this->redirect('profiles-trash', ['flash' => 'Profile not found.']);
            return;
        }

        // Typed-phrase confirmation (shared validator with the global purge).
        $phrase = (string) ($req['purge_confirmation_phrase'] ?? '');
        if (!SchemaHealth::isPurgeConfirmed($phrase)) {
            $this->redirect('profiles-trash', ['flash' => 'Purge cancelled — confirmation phrase did not match.']);
            return;
        }

        $service = new ProfilePurgeService();
        $guard = $service->assess($id);
        if (!$guard['allowed']) {
            $this->redirect('profiles-trash', ['flash' => 'Purge blocked — ' . implode(' ', $guard['reasons'])]);
            return;
        }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        try {
            $counts = $service->purge($id);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: profile #' . $id . ' purge FAILED (admin #' . $adminId . ') — ' . $e->getMessage());
            }
            $this->redirect('profiles-trash', ['flash' => 'Purge failed; see activity log. Nothing was removed.']);
            return;
        }

        if (function_exists('logActivity')) {
            logActivity(sprintf(
                'Contabo Pricing: PROFILE PURGE #%d (%s) by admin #%d — removed %d mapping(s), %d version(s), %d config link(s) [%d group, %d option, %d value], and addon-created WHMCS objects: %d groups, %d options, %d sub-options, %d pricing rows, %d product links.',
                $id,
                (string) ($row['slug'] ?? ''),
                $adminId,
                (int) $counts['mappings'], (int) $counts['versions'],
                (int) $counts['group_links'] + (int) $counts['option_links'] + (int) $counts['value_links'],
                (int) $counts['group_links'], (int) $counts['option_links'], (int) $counts['value_links'],
                (int) $counts['groups'], (int) $counts['options'], (int) $counts['subs'],
                (int) $counts['sub_pricing'], (int) $counts['product_links']
            ));
        }

        $dest = count($pm->listTrashed()) > 0 ? 'profiles-trash' : 'profiles';
        $this->redirect($dest, ['flash' => 'Profile permanently purged.']);
    }

    /**
     * Flag each profile as drifted when its stored price no longer matches
     * upstream. Cheap + outage-tolerant: uses ONLY the already-loaded $plans (no
     * extra API calls) plus one batched version lookup. Drift =
     *   (a) the plan was removed/renamed upstream (orphaned), OR
     *   (b) the profile's latest version base monthly differs (> 1 cent) from
     *       the current effective monthly for its period.
     * This mirrors exactly how SyncEngine decides to append a new version
     * (ProfileVersionInput::differsFrom on base_monthly_eur). When $plans is
     * empty (API down) nothing is flagged — we never guess.
     *
     * @param list<array<string,mixed>> $profiles
     * @param list<array<string,mixed>> $plans
     * @return list<array<string,mixed>>
     */
    private function annotateDrift(array $profiles, array $plans): array
    {
        if (empty($profiles) || empty($plans)) {
            return $profiles; // can't compare → leave drift unset (template reads 0)
        }
        $bySlug = [];
        foreach ($plans as $p) {
            $slug = (string) ($p['product_slug'] ?? '');
            if ($slug !== '') { $bySlug[$slug] = $p; }
        }
        // Batch-load the latest version base price for every listed profile.
        $verIds = [];
        foreach ($profiles as $pr) {
            $vid = (int) ($pr['latest_version_id'] ?? 0);
            if ($vid > 0) { $verIds[] = $vid; }
        }
        $baseByVer = [];
        if (!empty($verIds)) {
            foreach (Capsule::table('mod_contabo_profile_version')
                         ->whereIn('id', $verIds)->get(['id', 'base_monthly_eur']) as $row) {
                $baseByVer[(int) $row->id] = (float) $row->base_monthly_eur;
            }
        }
        foreach ($profiles as &$pr) {
            $pr['drifted'] = 0;
            $slug = (string) ($pr['plan_slug'] ?? '');
            if ($slug === '') { continue; }
            if (!isset($bySlug[$slug])) { $pr['drifted'] = 1; continue; } // orphaned upstream
            $vid = (int) ($pr['latest_version_id'] ?? 0);
            if ($vid <= 0 || !isset($baseByVer[$vid])) { continue; }      // no snapshot to compare
            $current = $this->currentEffectiveMonthly($bySlug[$slug], (int) ($pr['period_months'] ?? 0));
            if ($current === null) { continue; }                          // period not comparable
            if (abs($current - $baseByVer[$vid]) > 0.01) { $pr['drifted'] = 1; }
        }
        unset($pr);
        return $profiles;
    }

    /**
     * Current effective monthly (EUR) for a plan at a given period, read from
     * the plan's `periods[]`. Returns null when the period isn't offered or the
     * payload lacks a per-period breakdown — caller treats null as "can't
     * compare" (no drift), never as a price.
     *
     * @param array<string,mixed> $plan
     */
    private function currentEffectiveMonthly(array $plan, int $periodMonths): ?float
    {
        if ($periodMonths <= 0) { return null; }
        $periods = isset($plan['periods']) && is_array($plan['periods']) ? $plan['periods'] : [];
        foreach ($periods as $per) {
            if (is_array($per) && (int) ($per['months'] ?? 0) === $periodMonths && isset($per['effective_monthly'])) {
                return (float) $per['effective_monthly'];
            }
        }
        return null;
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
        // v8: cycles the profile SOURCES (offered superset). Default 63 (all six)
        // when the form posts nothing. period_months is DERIVED from the longest
        // published cycle so the slug + identity fingerprint stay stable (the
        // Period dropdown is gone from the form).
        $publishedMask = $this->coercePublishedMask($req['published_cycles_mask'] ?? null);
        $periodMonths  = $this->longestPublishedMonths($publishedMask);

        $mode = $this->normalizeProfileMode($req['profile_mode'] ?? null);

        // Fixed mode is a pre-packaged SKU: every configurator dimension must be
        // pinned to a concrete value. Reject an incomplete fixed profile.
        if ($mode === ProfileIdentityResolver::MODE_FIXED) {
            $err = $this->fixedCompletenessError((string) ($req['plan_slug'] ?? ''), $optionsPayload);
            if ($err !== null) {
                $this->redirect('profiles', ['flash' => 'Cannot create fixed profile — ' . $err]);
                return;
            }
        }

        $create = [
            'slug'          => (string) ($req['slug'] ?? ''),
            'name'          => trim((string) ($req['name'] ?? '')),
            'plan_slug'     => (string) ($req['plan_slug'] ?? ''),
            'period_months' => $periodMonths,
            'published_cycles_mask' => $publishedMask,
            'region'        => $region,
            'os'            => $os,
            'tags'          => (string) ($req['tags'] ?? ''),
            'sync_strategy' => (string) ($req['sync_strategy'] ?? $this->settings->defaultSyncStrategy),
            // Mode + exposure gate. profile_mode feeds the identity fingerprint
            // (fixed vs configurable hash differently — see ProfileIdentityResolver);
            // expose_configurable_options is the master switch ConfigurableOptionsSyncer
            // honours on Apply. Both default to the backward-compatible values.
            'profile_mode'  => $mode,
            'expose_configurable_options' => $this->normalizeExposeFlag($req['expose_configurable_options'] ?? null),
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
            'profiles'        => $this->annotateDrift((new ProfileManager($this->settings))->listProfiles(false), $plans),
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

        // v8: published cycles → derived period_months. Only patch them when the
        // form actually posted the mask (so a partial save can't reset cycles).
        $publishedMask = null;
        $periodMonths  = null;
        if (isset($req['published_cycles_mask'])) {
            $publishedMask = $this->coercePublishedMask($req['published_cycles_mask']);
            $periodMonths  = $this->longestPublishedMonths($publishedMask);
        }

        // Fixed-mode completeness: validate when this save sets/keeps fixed mode
        // AND submits selections. (When the form doesn't post mode/options we
        // can't re-validate here; the create path is the primary gate.)
        $mode = isset($req['profile_mode']) ? $this->normalizeProfileMode($req['profile_mode']) : null;
        if ($mode === ProfileIdentityResolver::MODE_FIXED && $optionsPayload !== null) {
            $err = $this->fixedCompletenessError((string) ($req['plan_slug'] ?? ''), $optionsPayload);
            if ($err !== null) {
                $this->redirect('profiles', ['flash' => 'Cannot save fixed profile — ' . $err]);
                return;
            }
        }

        $patch = array_filter([
            'name'          => $req['name'] ?? null,
            'plan_slug'     => $req['plan_slug'] ?? null,
            'period_months' => $periodMonths,
            'published_cycles_mask' => $publishedMask,
            'region'        => $region,
            'os'            => $os,
            'tags'          => $req['tags'] ?? null,
            'sync_strategy' => $req['sync_strategy'] ?? null,
            'options'       => $optionsPayload,
            // Only patch mode/exposure when the form actually submitted them, so a
            // partial save never silently flips an existing profile's mode or gate.
            'profile_mode'  => $mode,
            'expose_configurable_options' => isset($req['expose_configurable_options'])
                ? $this->normalizeExposeFlag($req['expose_configurable_options']) : null,
        ], static fn ($v) => $v !== null);
        $pm->update($id, $patch);
        $this->redirect('profiles', ['flash' => "Updated profile #{$id}"]);
    }

    /**
     * Coerce a posted published_cycles_mask to [0, CycleSet::MASK_MAX]. Empty /
     * absent defaults to 63 (all six cycles offered).
     *
     * @param mixed $raw
     */
    private function coercePublishedMask($raw): int
    {
        if ($raw === null || $raw === '') {
            return CycleSet::MASK_MAX;
        }
        $mask = $this->coerceMask($raw);
        // A genuinely-empty selection is almost always a JS glitch; fall back to
        // all-offered rather than silently sourcing nothing.
        return $mask === 0 ? CycleSet::MASK_MAX : $mask;
    }

    /**
     * Longest (max-months) cycle enabled in a published mask, used to derive the
     * profile's primary period_months for slug + identity. Falls back to 1.
     */
    private function longestPublishedMonths(int $mask): int
    {
        $set = CycleSet::fromMask($mask);
        $max = 0;
        foreach ($set->enabledCycles() as $cycle) {
            $m = (int) CycleNormalizer::monthsForCycle($cycle);
            if ($m > $max) { $max = $m; }
        }
        return $max > 0 ? $max : 1;
    }

    /**
     * Validate that a FIXED (pre-packaged) profile pins every required
     * configurator dimension. Returns an error string, or null when complete.
     *
     * Best-effort on the API: if the configurator can't be fetched (API down),
     * we DON'T block the save — we log and allow — so an outage can't wedge admin
     * work. The create path is the primary gate; sync/provision re-validate.
     *
     * @param array<string,mixed>|null $selections control_key => {label,...}
     */
    private function fixedCompletenessError(string $planSlug, ?array $selections): ?string
    {
        if ($planSlug === '') {
            return 'pick a plan first.';
        }
        $selections = is_array($selections) ? $selections : [];

        try {
            $cfg = (new ApiClient($this->settings))->configurator($planSlug);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: fixed-completeness check skipped (configurator fetch failed) for '
                    . $planSlug . ' — ' . $e->getMessage());
            }
            return null; // don't block on API outage
        }

        $controls = $this->buildConfiguratorControls(is_array($cfg) ? $cfg : []);
        $missing = [];
        foreach ($controls as $control) {
            if (!empty($control['optional'])) {
                continue; // optional dimension (e.g. Apps/Panels/Data Protection)
            }
            $key = (string) ($control['key'] ?? '');
            $sel = $selections[$key] ?? null;
            $label = is_array($sel) ? (string) ($sel['label'] ?? '') : '';
            if ($label === '' || $label === 'None') {
                $missing[] = (string) ($control['label'] ?? $key);
            }
        }
        if ($missing !== []) {
            return 'select a value for every required option: ' . implode(', ', $missing) . '.';
        }
        return null;
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

    /**
     * Whitelist the posted profile_mode to one of the two known modes. Anything
     * unexpected (or absent) collapses to the backward-compatible fixed mode.
     *
     * @param mixed $raw
     */
    private function normalizeProfileMode($raw): string
    {
        return ((string) $raw) === ProfileIdentityResolver::MODE_CONFIGURABLE
            ? ProfileIdentityResolver::MODE_CONFIGURABLE
            : ProfileIdentityResolver::MODE_FIXED;
    }

    /**
     * Normalize the expose_configurable_options checkbox to 0/1. Absent (null)
     * defaults to 1 to match the column default + preserve pre-existing behavior;
     * the explicit hidden "0" the form posts when unchecked turns it off.
     *
     * @param mixed $raw
     */
    private function normalizeExposeFlag($raw): int
    {
        if ($raw === null) {
            return 1;
        }
        return in_array((string) $raw, ['1', 'yes', 'true', 'on'], true) ? 1 : 0;
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
     * 0.5.1 — READ-ONLY pre-apply DIFF. Shows what config-apply WOULD do to THIS
     * live product (create/update/noop/drift_skip per dimension) BEFORE any write,
     * so the billing-impacting apply path is always preceded by a diff. The Apply
     * form lives on the rendered diff screen. GET, zero writes.
     *
     * @param array<string,mixed> $req
     */
    private function configDiff(array $req): void
    {
        $id        = (int) ($req['id'] ?? 0);
        $productId = (int) ($req['product_id'] ?? 0);

        $pm = new ProfileManager($this->settings);
        $profile = $pm->find($id);
        if ($profile === null) {
            $this->redirect('profiles', ['flash' => 'Profile not found.']);
            return;
        }
        $mapped = Capsule::table('mod_contabo_mapping')
            ->where('profile_id', $id)->where('product_id', $productId)->first();
        if ($productId <= 0 || $mapped === null) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Choose a WHMCS product this profile is mapped to (map one on the Mappings page first).']);
            return;
        }

        $planSlug = (string) ($profile['plan_slug'] ?? '');
        try {
            $cfg = (new ApiClient($this->settings))->configurator($planSlug);
            $optionsMap = (isset($cfg['options']) && is_array($cfg['options'])) ? $cfg['options'] : [];
            $parsed = DimensionParser::parse($optionsMap);
            $specs  = isset($parsed['specs']) && is_array($parsed['specs']) ? $parsed['specs'] : [];
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing config-diff API error (profile #' . $id . '): ' . $e->getMessage());
            }
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Cannot build diff — API error loading options; see the activity log.']);
            return;
        }
        if ($specs === []) {
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Nothing to diff — the plan has no configurable options.']);
            return;
        }

        $diff  = null;
        $error = null;
        try {
            // Dry-run adapter: diff() only READS (fetchOption); zero writes.
            $adapter = new WhmcsConfigOptionsAdapter(true);
            $audit   = new OptionAuditLog($adapter->syncBatchId());
            $syncer  = new ConfigurableOptionsSyncer($adapter, $audit, new ConfigOptionLinkRepository());
            $diff = $syncer->diff($id, $productId, 'contabo-' . $planSlug, $specs);
        } catch (\Throwable $e) {
            $error = 'Could not build the diff; see the activity log.';
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing config-diff error (profile #' . $id . '): ' . $e->getMessage());
            }
        }

        $this->render('config_diff.tpl', [
            'profile'    => $profile,
            'profile_id' => $id,
            'product_id' => $productId,
            'plan_slug'  => $planSlug,
            'diff'       => $diff,
            'error'      => $error,
        ]);
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
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing config-apply API error (profile #' . $id . '): ' . $e->getMessage());
            }
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Cannot apply — error loading options from the API; see the activity log for detail.']);
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
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Apply failed; see the activity log for detail.']);
            return;
        }

        // Phase C: the profile-level expose_configurable_options gate is off —
        // no WHMCS config option group was created. Tell the admin plainly.
        if (!empty($r['skipped']) && ($r['skip_reason'] ?? '') === 'expose_gate_disabled') {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: config-apply skipped (profile #' . $id . ') — expose_configurable_options gate disabled');
            }
            $this->redirect('config-preview', ['id' => $id, 'flash' => 'Apply skipped — “Expose configurable options” is disabled for this profile. Enable it on the profile to create WHMCS config option groups.']);
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
        // Audit trail for the destructive write (consistent with maintenance-purge):
        // record the admin + product + outcome in the WHMCS activity log.
        if (function_exists('logActivity')) {
            $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
            logActivity('Contabo Pricing: config-apply by admin #' . $adminId . ' (profile #' . $id . ') — ' . $msg);
        }
        $this->redirect('config-preview', ['id' => $id, 'flash' => $msg]);
    }

    /**
     * Exposure editor — GET. Curate which configurable-option dimensions are
     * exposed to customers (the `expose_to_customer` / `hidden` flags on the
     * option-links). These flags only take effect on the live product when the
     * admin re-runs Apply (config-apply); this screen just records intent.
     *
     * @param array<string,mixed> $req
     */
    private function configExposure(array $req): void
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

        $links = (new ConfigOptionLinkRepository())->listOptionLinksForProfile($id);

        $this->render('config_exposure.tpl', [
            'profile'      => $profile,
            'option_links' => $links,
            'flash'        => (string) ($req['flash'] ?? ''),
        ]);
    }

    /**
     * Exposure editor — POST. For each posted option-link, persist the
     * `expose_to_customer` + `hidden` checkboxes via the link repository
     * (the upsert whitelists exposure flags). Nothing reaches the live product
     * until Apply is re-run; the flash says so.
     *
     * @param array<string,mixed> $req
     */
    private function configExposureSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $id = (int) ($req['id'] ?? 0);
        $pm = new ProfileManager($this->settings);
        $profile = $pm->find($id);
        if ($profile === null) {
            $this->redirect('profiles', ['flash' => 'Profile not found.']);
            return;
        }

        $repo  = new ConfigOptionLinkRepository();
        $links = $repo->listOptionLinksForProfile($id);

        $exposed = isset($req['expose_to_customer']) && is_array($req['expose_to_customer'])
            ? $req['expose_to_customer'] : [];
        $hidden = isset($req['hidden']) && is_array($req['hidden'])
            ? $req['hidden'] : [];

        $updated = 0;
        foreach ($links as $existing) {
            $dimensionKey = (string) ($existing['dimension_key'] ?? '');
            if ($dimensionKey === '') {
                continue;
            }
            $isExposed = !empty($exposed[$dimensionKey]);
            $isHidden  = !empty($hidden[$dimensionKey]);
            $repo->upsertOptionLink(
                $id,
                $dimensionKey,
                (int) ($existing['optiontype'] ?? 0),
                (isset($existing['whmcs_option_id']) && $existing['whmcs_option_id'] !== null)
                    ? (int) $existing['whmcs_option_id']
                    : null,
                ['expose_to_customer' => $isExposed, 'hidden' => $isHidden]
            );
            $updated++;
        }

        $this->redirect('config-exposure', [
            'id'    => $id,
            'flash' => sprintf(
                'Saved exposure for %d option%s. Re-run Apply on the preview screen to push these flags to the live product.',
                $updated,
                $updated === 1 ? '' : 's'
            ),
        ]);
    }

    // ── A.6.3 — capability + compatibility editors ─────────────────────────────

    /**
     * Resolve a profile + its plan's dimension/value specs from the live
     * configurator (same prep as configPreview). Shared by the capability +
     * compatibility editors so an admin can author rows for ANY of the plan's
     * options, not only ones a prior Apply happened to seed.
     *
     * @return array{profile: array<string,mixed>|null, plan_slug: string, specs: list<array<string,mixed>>, api_error: string}
     */
    private function profilePlanSpecs(int $id): array
    {
        $profile = (new ProfileManager($this->settings))->find($id);
        if ($profile === null) {
            return ['profile' => null, 'plan_slug' => '', 'specs' => [], 'api_error' => ''];
        }
        $planSlug = (string) ($profile['plan_slug'] ?? '');
        $specs    = [];
        $apiError = '';
        try {
            $cfg        = (new ApiClient($this->settings))->configurator($planSlug);
            $optionsMap = (isset($cfg['options']) && is_array($cfg['options'])) ? $cfg['options'] : [];
            $parsed     = DimensionParser::parse($optionsMap);
            $specs      = isset($parsed['specs']) && is_array($parsed['specs']) ? $parsed['specs'] : [];
        } catch (\Throwable $e) {
            $apiError = $e->getMessage();
        }
        return ['profile' => $profile, 'plan_slug' => $planSlug, 'specs' => $specs, 'api_error' => $apiError];
    }

    /**
     * Flatten DimensionParser specs into {dimension_key, value_key, label, row}
     * rows, overlaying each with a saved matrix row keyed by
     * "dimension_key\x1Fvalue_key" from $existingByKey ([] when none saved yet).
     *
     * @param list<array<string,mixed>>          $specs
     * @param array<string,array<string,mixed>>  $existingByKey
     * @return list<array{dimension_key:string,value_key:string,label:string,row:array<string,mixed>}>
     */
    private function mergeSpecRows(array $specs, array $existingByKey): array
    {
        $out = [];
        foreach ($specs as $spec) {
            if (!is_array($spec) || !isset($spec['dimension_key'])) { continue; }
            $dim    = (string) $spec['dimension_key'];
            $values = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];
            foreach ($values as $value) {
                if (!is_array($value) || !isset($value['value_key'])) { continue; }
                $val = (string) $value['value_key'];
                $k   = $dim . "\x1F" . $val;
                $out[] = [
                    'dimension_key' => $dim,
                    'value_key'     => $val,
                    'label'         => (string) ($value['label'] ?? $val),
                    'row'           => isset($existingByKey[$k]) ? $existingByKey[$k] : [],
                ];
            }
        }
        return $out;
    }

    /** Profile-not-found errorbox shared by the editors. */
    private function editorProfileMissing(): void
    {
        echo '<div class="errorbox">Profile not found. <a href="'
            . htmlspecialchars($this->settings->moduleLink, ENT_QUOTES, 'UTF-8')
            . '&action=profiles">Back to profiles</a>.</div>';
    }

    /**
     * Capability editor — GET. Shows the §4 capability matrix for the profile's
     * plan (one row per dimension/value), overlaying any saved
     * mod_contabo_option_capability rows. Editing here records intent; the
     * amendment-6 auto-apply gate reads capability_source.
     *
     * @param array<string,mixed> $req
     */
    private function capabilityEditor(array $req): void
    {
        $id  = (int) ($req['id'] ?? 0);
        $ctx = $this->profilePlanSpecs($id);
        if ($ctx['profile'] === null) { $this->editorProfileMissing(); return; }

        $existing = [];
        foreach ((new ConfigOptionCapabilityRepository())->listForPlan($ctx['plan_slug']) as $r) {
            $existing[((string) ($r['dimension_key'] ?? '')) . "\x1F" . ((string) ($r['value_key'] ?? ''))] = $r;
        }
        $this->render('capability_editor.tpl', [
            'profile'       => $ctx['profile'],
            'plan_slug'     => $ctx['plan_slug'],
            'rows'          => $this->mergeSpecRows($ctx['specs'], $existing),
            'api_error'     => $ctx['api_error'],
            'boolean_flags' => ConfigOptionCapabilityRepository::BOOLEAN_FLAGS,
            'valid_sources' => ConfigOptionCapabilityRepository::VALID_SOURCES,
            'flash'         => (string) ($req['flash'] ?? ''),
        ]);
    }

    /**
     * Capability editor — POST. Upserts one capability row per posted
     * row[]{dimension_key,value_key,<flags>,provisioning_action,capability_source}
     * through the repository chokepoint (which re-whitelists every column).
     *
     * @param array<string,mixed> $req
     */
    private function capabilityEditorSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }
        $id      = (int) ($req['id'] ?? 0);
        $profile = (new ProfileManager($this->settings))->find($id);
        if ($profile === null) { $this->redirect('profiles', ['flash' => 'Profile not found.']); return; }

        $planSlug = (string) ($profile['plan_slug'] ?? '');
        $rows     = isset($req['row']) && is_array($req['row']) ? $req['row'] : [];
        $repo     = new ConfigOptionCapabilityRepository();
        $saved    = 0;
        foreach ($rows as $row) {
            if (!is_array($row)) { continue; }
            $dim = (string) ($row['dimension_key'] ?? '');
            $val = (string) ($row['value_key'] ?? '');
            if ($dim === '' || $val === '') { continue; }
            $flags = [];
            foreach (ConfigOptionCapabilityRepository::BOOLEAN_FLAGS as $flag) {
                $flags[$flag] = !empty($row[$flag]) ? 1 : 0; // absent checkbox → explicit 0
            }
            if (isset($row['provisioning_action'])) {
                $pa = trim((string) $row['provisioning_action']);
                $flags['provisioning_action'] = $pa === '' ? null : $pa;
            }
            if (isset($row['capability_source']) && (string) $row['capability_source'] !== '') {
                $flags['capability_source'] = (string) $row['capability_source'];
            }
            $repo->upsertCapability($planSlug, $dim, $val, $flags);
            $saved++;
        }
        if (function_exists('logActivity')) {
            $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
            logActivity('Contabo Pricing: capability-editor save by admin #' . $adminId . ' (profile #' . $id . ', ' . $saved . ' rows)');
        }
        $this->redirect('capability-editor', [
            'id'    => $id,
            'flash' => sprintf('Saved %d capability row%s.', $saved, $saved === 1 ? '' : 's'),
        ]);
    }

    /**
     * Compatibility editor — GET. Shows the §5 compatibility matrix for the
     * profile's plan (one row per dimension/value), overlaying any saved
     * mod_contabo_option_compatibility rules. Feeds SelectionValidator.
     *
     * @param array<string,mixed> $req
     */
    private function compatibilityEditor(array $req): void
    {
        $id  = (int) ($req['id'] ?? 0);
        $ctx = $this->profilePlanSpecs($id);
        if ($ctx['profile'] === null) { $this->editorProfileMissing(); return; }

        $existing = [];
        foreach ((new ConfigOptionCompatibilityRepository())->listForPlan($ctx['plan_slug']) as $r) {
            $existing[((string) ($r['dimension_key'] ?? '')) . "\x1F" . ((string) ($r['value_key'] ?? ''))] = $r;
        }
        $this->render('compatibility_editor.tpl', [
            'profile'   => $ctx['profile'],
            'plan_slug' => $ctx['plan_slug'],
            'rows'      => $this->mergeSpecRows($ctx['specs'], $existing),
            'api_error' => $ctx['api_error'],
            'flash'     => (string) ($req['flash'] ?? ''),
        ]);
    }

    /**
     * Compatibility editor — POST. Upserts a rule per posted row, parsing the
     * incompatible_with / required_values textareas (one value-key per line or
     * comma-separated) and the optional min/max qty bounds. Untouched + never-saved
     * rows are skipped so the table isn't polluted with empty rules; an existing
     * rule the admin blanked out IS written (cleared).
     *
     * @param array<string,mixed> $req
     */
    private function compatibilityEditorSave(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }
        $id      = (int) ($req['id'] ?? 0);
        $profile = (new ProfileManager($this->settings))->find($id);
        if ($profile === null) { $this->redirect('profiles', ['flash' => 'Profile not found.']); return; }

        $planSlug = (string) ($profile['plan_slug'] ?? '');
        $repo     = new ConfigOptionCompatibilityRepository();

        $existingKeys = [];
        foreach ($repo->listForPlan($planSlug) as $r) {
            $existingKeys[((string) ($r['dimension_key'] ?? '')) . "\x1F" . ((string) ($r['value_key'] ?? ''))] = true;
        }

        $rows  = isset($req['row']) && is_array($req['row']) ? $req['row'] : [];
        $saved = 0;
        foreach ($rows as $row) {
            if (!is_array($row)) { continue; }
            $dim = (string) ($row['dimension_key'] ?? '');
            $val = (string) ($row['value_key'] ?? '');
            if ($dim === '' || $val === '') { continue; }

            $incompatible = $this->splitValueList((string) ($row['incompatible_with'] ?? ''));
            $required     = $this->splitValueList((string) ($row['required_values'] ?? ''));
            $minRaw       = trim((string) ($row['min_value'] ?? ''));
            $maxRaw       = trim((string) ($row['max_value'] ?? ''));

            $hasContent = $incompatible !== [] || $required !== [] || $minRaw !== '' || $maxRaw !== '';
            $k = $dim . "\x1F" . $val;
            if (!$hasContent && !isset($existingKeys[$k])) { continue; } // never-saved + blank → skip

            $repo->upsertRule($planSlug, $dim, $val, [
                'incompatible_with' => $incompatible,
                'required_values'   => $required,
                'min_value'         => $minRaw === '' ? null : (int) $minRaw,
                'max_value'         => $maxRaw === '' ? null : (int) $maxRaw,
            ]);
            $saved++;
        }
        if (function_exists('logActivity')) {
            $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
            logActivity('Contabo Pricing: compatibility-editor save by admin #' . $adminId . ' (profile #' . $id . ', ' . $saved . ' rules)');
        }
        $this->redirect('compatibility-editor', [
            'id'    => $id,
            'flash' => sprintf('Saved %d compatibility rule%s.', $saved, $saved === 1 ? '' : 's'),
        ]);
    }

    /**
     * Split a textarea of value-keys (one per line or comma-separated) into a
     * de-duplicated list. Only newline + comma are separators — value-keys may
     * themselves contain spaces / slashes / parens (e.g. "Panels:cPanel/WHM").
     *
     * @return list<string>
     */
    private function splitValueList(string $raw): array
    {
        $parts = preg_split('/[\r\n,]+/', trim($raw));
        $out   = [];
        foreach (($parts ?: []) as $p) {
            $p = trim((string) $p);
            if ($p !== '') { $out[] = $p; }
        }
        return array_values(array_unique($out));
    }

    private function mappings(array $req, ?array $publicationPreview = null): void
    {
        $pm = new ProfileManager($this->settings);
        $profiles = $pm->listProfiles(false);
        $whmcsProducts = Capsule::table('tblproducts')
            ->orderBy('name')->limit(500)
            ->get(['id', 'name', 'gid'])->map(static fn ($r) => (array) $r)->all();
        $mappings = Capsule::table('mod_contabo_mapping')
            ->orderByDesc('updated_at')->get()->map(static fn ($r) => (array) $r)->all();
        $catalogVersions = Capsule::table('mod_contabo_catalog_versions')
            ->whereNotIn('state', ['invalid', 'retired'])
            ->orderByDesc('source_observed_at')
            ->limit(100)
            ->get()
            ->map(static fn ($r) => (array) $r)
            ->all();
        $mappingPublications = Capsule::table('mod_contabo_mapping_publications')
            ->orderByDesc('created_at')
            ->limit(100)
            ->get()
            ->map(static fn ($r) => (array) $r)
            ->all();

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
            'catalog_versions'    => $catalogVersions,
            'mapping_publications' => $mappingPublications,
            'publication_preview' => $publicationPreview,
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
        $sourceOverrides = $this->coerceJsonObject($req['source_overrides_json'] ?? null);

        $respectDisabled = array_key_exists('respect_disabled_cycles', $req)
            ? !empty($req['respect_disabled_cycles'])
            : true;
        $overwriteFree = !empty($req['overwrite_free_cycles']);
        $syncSetupFees = !empty($req['sync_setup_fees']);

        $rounding = (string) ($req['rounding_mode'] ?? 'exact_2_decimals');
        if (!Rounding::isSelectableMode($rounding)) {
            $rounding = Rounding::MODE_EXACT_2_DECIMALS;
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
                'source_overrides_json'    => $sourceOverrides,
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
     * Create an immutable, no-write preview of the provider identifiers that
     * would become eligible for future paid-order snapshots.
     *
     * @param array<string,mixed> $req
     */
    private function mappingPublicationPreview(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        try {
            $preview = (new MappingPublicationService())->preview(
                (int) ($req['mapping_id'] ?? 0),
                [
                    'rust_catalog_version' => (string) ($req['rust_catalog_version'] ?? ''),
                    'provider_sku_id' => (string) ($req['provider_sku_id'] ?? ''),
                    'region_id' => (string) ($req['region_id'] ?? ''),
                    'image_id' => (string) ($req['image_id'] ?? ''),
                    'management_code' => (string) ($req['management_code'] ?? 'self_managed'),
                ]
            );
            $req['flash'] = 'Publication preview created. Review the exact identifiers and hash before approval.';
            $this->mappings($req, $preview);
        } catch (\Throwable $e) {
            $this->renderSaveError($e, 'mappings');
        }
    }

    /**
     * Advance the active mapping pointer only when the administrator submits
     * the exact preview hash and typed confirmation.
     *
     * @param array<string,mixed> $req
     */
    private function mappingPublicationApprove(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        try {
            $result = (new MappingPublicationService())->approve(
                (string) ($req['mapping_version'] ?? ''),
                (string) ($req['preview_hash'] ?? ''),
                $adminId,
                (string) ($req['confirmation'] ?? ''),
                (string) ($req['reason'] ?? '')
            );
            if (function_exists('logActivity')) {
                logActivity(
                    'Contabo Pricing: published mapping '
                    . (string) ($result['mapping_version'] ?? $req['mapping_version'] ?? '')
                    . ' by admin #' . $adminId
                );
            }
            $this->redirect('mappings', ['flash' => 'Mapping publication approved and activated.']);
        } catch (\Throwable $e) {
            $this->renderSaveError($e, 'mappings');
        }
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

            // v8: when a profile is in context, surface its per-cycle SOURCE
            // (cost) vector so the mapping cycle table can show source → customer
            // price side by side. EUR/mo by cycle months, with the same nearest-≤
            // fallback the engine uses (24/36 → 12-mo, etc.).
            $source = [];
            $profileId = (int) ($req['profile_id'] ?? 0);
            if ($profileId > 0) {
                $source = $this->profileSourceVector($profileId);
            }

            $this->jsonOk([
                'product_id'    => $productId,
                'currency_id'   => $currencyId,
                'currency_code' => $currencyCode,
                'cycles'        => $cycles,
                'profile_id'    => $profileId,
                'source_eur'    => (object) $source,
            ]);
        } catch (\Throwable $e) {
            $this->jsonFail($e->getMessage());
        }
    }

    /**
     * The profile's latest per-cycle SOURCE vector (months => EUR/mo). Reads the
     * latest version's period_prices_json; empty for legacy/unsynced profiles.
     *
     * @return array<int,float>
     */
    private function profileSourceVector(int $profileId): array
    {
        $version = (new ProfileManager($this->settings))->latestVersion($profileId);
        if ($version === null) {
            return [];
        }
        $raw = $version['period_prices_json'] ?? null;
        $vector = [];
        if (is_array($raw)) {
            $vector = $raw;
        } elseif (is_string($raw) && $raw !== '') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                $vector = $decoded;
            }
        }
        $out = [];
        foreach ($vector as $m => $eur) {
            $mi = (int) $m;
            if ($mi > 0) {
                $out[$mi] = (float) $eur;
            }
        }
        return $out;
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
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing refresh-api error: ' . $e->getMessage());
            }
            $this->redirect('dashboard', ['flash' => 'Refresh failed; see the activity log for detail.']);
        }
    }

    private function catalogImport(): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        try {
            $catalog = (new ApiClient($this->settings))->catalog();
            $result = (new CatalogImportService())->import($catalog, $adminId);
            $verb = $result['created'] ? 'Imported' : 'Verified existing';
            $this->redirect('mappings', [
                'flash' => sprintf(
                    '%s Rust catalog %s (%d items).',
                    $verb,
                    $result['catalog_version'],
                    $result['item_count']
                ),
            ]);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing catalog import failed: ' . $e->getMessage());
            }
            $this->redirect('mappings', [
                'flash' => 'Catalog import failed validation; see the activity log for detail.',
            ]);
        }
    }

    /** @param array<string,mixed> $req */
    private function operations(array $req): void
    {
        if (!$this->guardSchema()) { return; }
        $beforeId = max(0, (int) ($req['before_id'] ?? 0));
        try {
            $data = (new VpsOperationsWorkbench())->overview($beforeId, 50);
            $data['flash'] = (string) ($req['flash'] ?? '');
            $this->render('operations.tpl', $data);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing operations workbench failed: ' . $e->getMessage());
            }
            echo '<div class="errorbox">The VPS operations workbench is unavailable. '
                . 'Verify the native suite schema from Maintenance.</div>';
        }
    }

    /** @param array<string,mixed> $req */
    private function operationCommand(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        try {
            $uuid = (new VpsOperationsWorkbench())->queueCommand(
                (string) ($req['command_type'] ?? ''),
                isset($req['service_id']) ? (int) $req['service_id'] : null,
                isset($req['operation_uuid']) ? (string) $req['operation_uuid'] : null,
                ['reason' => trim((string) ($req['reason'] ?? 'operator_request'))],
                $adminId
            );
            $this->redirect('operations', [
                'flash' => 'Operator command queued for provisioning-worker validation: ' . $uuid,
            ]);
        } catch (\Throwable $e) {
            $this->operatorActionError($e);
        }
    }

    /** @param array<string,mixed> $req */
    private function operationDetail(array $req): void
    {
        if (!$this->guardSchema()) { return; }
        try {
            $data = (new VpsOperationsWorkbench())->operationDetail(
                (string) ($req['operation_uuid'] ?? '')
            );
            $this->render('operation_detail.tpl', $data);
        } catch (\Throwable $e) {
            $this->operatorActionError($e);
        }
    }

    /** @param array<string,mixed> $req */
    private function capabilityCertify(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        try {
            (new VpsOperationsWorkbench())->certifyCapability(
                (string) ($req['provider_account_id'] ?? ''),
                (string) ($req['capability'] ?? ''),
                (string) ($req['state'] ?? 'not_certified'),
                (string) ($req['certification_version'] ?? ''),
                [
                    'evidence_reference' => trim((string) ($req['evidence_reference'] ?? '')),
                    'notes' => trim((string) ($req['evidence_notes'] ?? '')),
                    'certification_checklist_version' => '1',
                ],
                $adminId,
                (string) ($req['confirmation'] ?? '')
            );
            $this->redirect('operations', [
                'flash' => 'Provider capability certification recorded. Its write switch remains independent.',
            ]);
        } catch (\Throwable $e) {
            $this->operatorActionError($e);
        }
    }

    /** @param array<string,mixed> $req */
    private function providerWriteControl(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        $scope = (string) ($req['scope'] ?? 'global');
        $enabled = !empty($req['enabled']);
        $commandType = $scope === 'capability'
            ? 'set_capability_write_state'
            : 'set_global_write_state';
        $payload = [
            'enabled' => $enabled,
            'confirmation' => (string) ($req['confirmation'] ?? ''),
        ];
        if ($scope === 'capability') {
            $payload['capability'] = (string) ($req['capability'] ?? '');
            $payload['provider_account_id'] = (string) ($req['provider_account_id'] ?? '');
        }

        try {
            $uuid = (new VpsOperationsWorkbench())->queueCommand(
                $commandType,
                null,
                null,
                $payload,
                $adminId
            );
            $this->redirect('operations', [
                'flash' => 'Write-control command queued for cron validation: ' . $uuid,
            ]);
        } catch (\Throwable $e) {
            $this->operatorActionError($e);
        }
    }

    /** @param array<string,mixed> $req */
    private function adoptionApprove(array $req): void
    {
        if (!$this->requirePost()) { return; }
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }
        $adminId = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        $serviceId = (int) ($req['service_id'] ?? 0);
        $payload = [
            'provider_resource_id' => trim((string) ($req['provider_resource_id'] ?? '')),
            'evidence_hash' => trim((string) ($req['evidence_hash'] ?? '')),
            'confirmation' => (string) ($req['confirmation'] ?? ''),
        ];
        try {
            $uuid = (new VpsOperationsWorkbench())->queueCommand(
                'approve_adoption',
                $serviceId,
                null,
                $payload,
                $adminId
            );
            $this->redirect('operations', [
                'flash' => 'Adoption approval queued for live re-verification: ' . $uuid,
            ]);
        } catch (\Throwable $e) {
            $this->operatorActionError($e);
        }
    }

    private function operatorActionError(\Throwable $e): void
    {
        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing operator action rejected: ' . $e->getMessage());
        }
        $safe = $e instanceof \InvalidArgumentException
            ? $e->getMessage()
            : 'The operator action could not be queued.';
        $this->redirect('operations', ['flash' => 'Request rejected: ' . $safe]);
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
     * Fail closed when a mutating action is requested with GET or another
     * non-POST method. WHMCS CSRF validation is necessary but does not replace
     * HTTP method enforcement.
     */
    private function requirePost(): bool
    {
        $method = strtoupper((string) ($_SERVER['REQUEST_METHOD'] ?? ''));
        if ($method === 'POST') {
            return true;
        }
        if (PHP_SAPI === 'cli' && $method === '') {
            // Unit tests invoke controller methods directly; production web
            // requests always carry REQUEST_METHOD.
            return true;
        }

        http_response_code(405);
        header('Allow: POST');
        echo '<div class="errorbox">This action requires a POST request.</div>';
        return false;
    }

    /**
     * Minimal Smarty-style rendering. WHMCS exposes Smarty via global $smarty
     * but to keep this addon decoupled from internals we use a tiny PHP-include
     * template loop instead — every .tpl is plain PHP under a controlled scope.
     */
    /**
     * 0.5.1 — READ-ONLY multi-currency exposure diagnostic (CurrencySupportReport).
     * Surfaces whether non-INR services exist and whether any are on mapped
     * products (which would make the INR-only revenue guard an ACTIVE risk).
     * Zero writes. Errors render a generic message + log full detail (no raw
     * exception text in the UI).
     */
    private function currencyReport(): void
    {
        $report = null;
        $error  = null;
        try {
            $report = (new CurrencySupportReport())->build();
        } catch (\Throwable $e) {
            $error = 'Could not build the currency report; see the activity log for detail.';
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: currency report error — ' . $e->getMessage());
            }
        }

        // Phase C: live FX rates panel (best-effort; the report still renders if
        // the pricing API is unreachable). $fx_rates === null signals "tried but
        // unavailable" to the template; a populated array drives the panel.
        $fxRates    = null;
        $fxFetched  = null;
        $fxStale    = true;
        try {
            $fx = (new ApiClient($this->settings))->fx();
            $rates = (isset($fx['rates']) && is_array($fx['rates'])) ? $fx['rates'] : [];
            if ($rates !== []) {
                $fxRates = [];
                foreach ($rates as $code => $rate) {
                    $fxRates[] = ['currency' => (string) $code, 'rate' => (float) $rate];
                }
                $ts = null;
                foreach (['asof', 'as_of', 'fetched_at', 'date', 'timestamp', 'updated_at'] as $k) {
                    if (isset($fx[$k]) && $fx[$k] !== '') { $ts = $fx[$k]; break; }
                }
                if ($ts !== null) {
                    $parsed = is_numeric($ts) ? (int) $ts : strtotime((string) $ts);
                    if ($parsed !== false && $parsed > 0) {
                        $fxFetched = date('Y-m-d H:i', $parsed);
                        $fxStale   = (time() - $parsed) > 21600; // > 6h
                    }
                }
            }
        } catch (\Throwable $e) {
            $fxRates = null; // unavailable
        }

        // Per-service INR equivalents for the at-risk (non-base mapped) list,
        // using the WHMCS-configured currency rates (the rate the service is
        // actually billed at), not the live market rate.
        $preview = [];
        if (is_array($report) && !empty($report['non_inr_mapped'])) {
            $rateByCode = [];
            foreach (($report['currencies'] ?? []) as $c) {
                $rateByCode[(string) ($c['code'] ?? '')] = (float) ($c['rate'] ?? 0);
            }
            foreach ($report['non_inr_mapped'] as $s) {
                $code   = (string) ($s['currency_code'] ?? '');
                $amount = (float) ($s['amount'] ?? 0);
                $rate   = $rateByCode[$code] ?? 0.0;
                $inr    = $rate > 0 ? ($amount / $rate) : 0.0;
                $preview[] = [
                    'service_id'     => (int) ($s['service_id'] ?? 0),
                    'currency_code'  => $code,
                    'amount'         => $amount,
                    'inr_equivalent' => round($inr, 2),
                    'rate_used'      => $rate,
                ];
            }
        }

        $this->render('currency_report.tpl', [
            'report'           => $report,
            'error'            => $error,
            'fx_rates'         => $fxRates,
            'fx_fetched_at'    => $fxFetched,
            'fx_stale'         => $fxStale,
            'currency_preview' => $preview,
            'csv_url'          => $this->settings->moduleLink . '&action=currency-report-csv',
        ]);
    }

    /**
     * Stream the at-risk (non-base, mapped-product) services as CSV so an admin
     * can work the remediation list offline. Read-only; no token needed (GET).
     */
    private function currencyReportCsv(): void
    {
        $rows = [];
        try {
            $report = (new CurrencySupportReport())->build();
            $rateByCode = [];
            foreach (($report['currencies'] ?? []) as $c) {
                $rateByCode[(string) ($c['code'] ?? '')] = (float) ($c['rate'] ?? 0);
            }
            foreach (($report['non_inr_mapped'] ?? []) as $s) {
                $code   = (string) ($s['currency_code'] ?? '');
                $amount = (float) ($s['amount'] ?? 0);
                $rate   = $rateByCode[$code] ?? 0.0;
                $rows[] = [
                    (int) ($s['service_id'] ?? 0),
                    (int) ($s['client_id'] ?? 0),
                    (int) ($s['packageid'] ?? 0),
                    $code,
                    number_format($amount, 2, '.', ''),
                    (string) ($s['billingcycle'] ?? ''),
                    $rate > 0 ? number_format($amount / $rate, 2, '.', '') : '',
                    $rate > 0 ? number_format($rate, 6, '.', '') : '',
                ];
            }
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: currency CSV export error — ' . $e->getMessage());
            }
        }

        header('Content-Type: text/csv; charset=utf-8');
        header('Content-Disposition: attachment; filename="contabo_currency_at_risk_' . date('Ymd_His') . '.csv"');
        $out = fopen('php://output', 'w');
        fputcsv($out, ['service_id', 'client_id', 'product_id', 'currency', 'amount', 'billingcycle', 'inr_equivalent', 'fx_rate_used']);
        foreach ($rows as $r) {
            fputcsv($out, $r);
        }
        fclose($out);
        exit;
    }

    // ── Phase C — approval queue ─────────────────────────────────────────────
    //
    // RenewalEngine parks two skip reasons for human sign-off:
    //   awaiting_admin_approval                        (soft-threshold breach)
    //   awaiting_force_approval_max_increase_exceeded  (hard-ceiling breach)
    // The queue lists pending decisions that have NOT yet been resolved. A
    // decision is "resolved" once a child decision row (parent_decision_id =
    // its id) exists — approve/reject both INSERT a child, honouring the
    // append-only ledger contract (the original row is never UPDATEd).

    /** Build the base query for unresolved approval-pending decisions. */
    private function approvalPendingQuery()
    {
        return Capsule::table('mod_contabo_price_decision as d')
            ->where('d.requires_admin_approval', 1)
            ->where('d.applied', 0)
            ->whereNotExists(static function ($q): void {
                $q->select(Capsule::raw('1'))
                  ->from('mod_contabo_price_decision as c')
                  ->whereColumn('c.parent_decision_id', 'd.id');
            });
    }

    private function approvalQueue(array $req): void
    {
        if (!$this->guardSchema()) { return; }

        $perPage = 50;
        $page    = max(1, (int) ($req['page'] ?? 1));

        $pending    = [];
        $softCount  = 0;
        $forceCount = 0;
        $totalPages = 1;

        try {
            $softCount  = (int) (clone $this->approvalPendingQuery())
                ->where('d.skip_reason', 'awaiting_admin_approval')->count();
            $forceCount = (int) (clone $this->approvalPendingQuery())
                ->where('d.skip_reason', 'awaiting_force_approval_max_increase_exceeded')->count();

            $total      = $softCount + $forceCount;
            $totalPages = max(1, (int) ceil($total / $perPage));
            $page       = min($page, $totalPages);

            $rows = $this->approvalPendingQuery()
                ->leftJoin('tblhosting as h', 'h.id', '=', 'd.service_id')
                ->orderBy('d.decided_at', 'asc')
                ->offset(($page - 1) * $perPage)
                ->limit($perPage)
                ->get([
                    'd.id', 'd.service_id', 'd.old_price', 'd.proposed_new_price',
                    'd.skip_reason', 'd.decided_at', 'h.userid as client_id', 'h.domain',
                ]);

            foreach ($rows as $r) {
                $r   = (array) $r;
                $old = (float) ($r['old_price'] ?? 0);
                $new = (float) ($r['proposed_new_price'] ?? 0);
                $pct = $old > 0 ? (($new - $old) / $old) * 100 : 0.0;
                $pending[] = [
                    'id'                  => (int) $r['id'],
                    'service_id'          => (int) $r['service_id'],
                    'client_id'           => (int) ($r['client_id'] ?? 0),
                    'domain'              => (string) ($r['domain'] ?? ''),
                    'current_price'       => $old,
                    'proposed_new_price'  => $new,
                    'proposed_change_pct' => round($pct, 1),
                    'skip_reason'         => (string) ($r['skip_reason'] ?? ''),
                    'decided_at'          => (string) ($r['decided_at'] ?? ''),
                ];
            }
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: approval queue error — ' . $e->getMessage());
            }
        }

        $this->render('approval_queue.tpl', [
            'pending'     => $pending,
            'soft_count'  => $softCount,
            'force_count' => $forceCount,
            'phase'       => $this->readSetting('repricing_phase', 'observe'),
            'page'        => $page,
            'total_pages' => $totalPages,
            'flash'       => (string) ($req['flash'] ?? ''),
        ]);
    }

    private function approvalApprove(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $decisionId = (int) ($req['decision_id'] ?? 0);
        $page       = max(1, (int) ($req['page'] ?? 1));
        $adminId    = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        if ($decisionId <= 0) {
            $this->redirect('approval-queue', ['page' => $page, 'flash' => 'Invalid decision id.']);
            return;
        }

        $phase   = $this->readSetting('repricing_phase', 'observe');
        $enabled = ($phase === 'opt_in' || $phase === 'enforce'); // observe suppresses the write
        $flash   = '';

        try {
            Capsule::connection()->transaction(function () use ($decisionId, $adminId, $enabled, &$flash): void {
                $row = Capsule::table('mod_contabo_price_decision')->where('id', $decisionId)->lockForUpdate()->first();
                if ($row === null) { $flash = 'Decision not found.'; return; }
                $row = (array) $row;

                // Stale / concurrent-resolution guard.
                if ((int) ($row['applied'] ?? 0) === 1 || (int) ($row['requires_admin_approval'] ?? 0) !== 1) {
                    $flash = 'Decision already resolved.'; return;
                }
                $childExists = Capsule::table('mod_contabo_price_decision')->where('parent_decision_id', $decisionId)->exists();
                if ($childExists) { $flash = 'Decision already resolved by another action.'; return; }

                $serviceId = (int) $row['service_id'];
                $newPrice  = (float) $row['proposed_new_price'];

                $writeResult = (new ServicePriceWriter($enabled))
                    ->updateRecurringAmount($serviceId, $newPrice, 'manual_admin_approval', $decisionId);
                $applied = !empty($writeResult['applied']);

                // Append-only: insert a child decision recording the outcome.
                (new DecisionLog())->insert([
                    'service_id'              => $serviceId,
                    'profile_id'              => $row['profile_id'] ?? null,
                    'profile_version_id'      => $row['profile_version_id'] ?? null,
                    'cron_run_id'             => (string) ($row['cron_run_id'] ?? ('manual-' . date('YmdHis'))),
                    'billing_cycle'           => (string) ($row['billing_cycle'] ?? 'monthly'),
                    'cycle_months'            => (int) ($row['cycle_months'] ?? 1),
                    'currency'                => (string) ($row['currency'] ?? 'INR'),
                    'old_price'               => (float) ($row['old_price'] ?? 0),
                    'proposed_new_price'      => $newPrice,
                    'policy_used'             => (string) ($row['policy_used'] ?? 'manual'),
                    'applied'                 => $applied ? 1 : 0,
                    'applied_via'             => 'manual_admin_approval',
                    'skip_reason'             => $applied ? null : (string) ($writeResult['via'] ?? 'writer_disabled'),
                    'requires_admin_approval' => 0,
                    'parent_decision_id'      => $decisionId,
                ]);

                // Action ledger: who approved (the writer logs the 'apply' row).
                Capsule::table('mod_contabo_pricing_action')->insert([
                    'action_type' => 'approve',
                    'service_id'  => $serviceId,
                    'decision_id' => $decisionId,
                    'admin_id'    => $adminId,
                    'reason'      => $applied ? 'approved + applied' : ('approved; write suppressed (' . (string) ($writeResult['via'] ?? '') . ')'),
                    'created_at'  => date('Y-m-d H:i:s'),
                ]);

                $flash = $applied
                    ? ('Approved decision #' . $decisionId . ' — new price applied.')
                    : ('Approved decision #' . $decisionId . ' — recorded, but the write was suppressed by phase “' . $this->readSetting('repricing_phase', 'observe') . '”.');
            });
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: approval-approve error (decision #' . $decisionId . ') — ' . $e->getMessage());
            }
            $flash = 'Approval failed; see the activity log for detail.';
        }

        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing: approval-approve by admin #' . $adminId . ' (decision #' . $decisionId . ') — ' . $flash);
        }
        $this->redirect('approval-queue', ['page' => $page, 'flash' => $flash]);
    }

    private function approvalReject(array $req): void
    {
        if (!$this->verifyToken()) { return; }
        if (!$this->guardSchema()) { return; }

        $decisionId = (int) ($req['decision_id'] ?? 0);
        $page       = max(1, (int) ($req['page'] ?? 1));
        $adminId    = isset($_SESSION['adminid']) ? (int) $_SESSION['adminid'] : 0;
        if ($decisionId <= 0) {
            $this->redirect('approval-queue', ['page' => $page, 'flash' => 'Invalid decision id.']);
            return;
        }

        $flash = '';
        try {
            Capsule::connection()->transaction(function () use ($decisionId, $adminId, &$flash): void {
                $row = Capsule::table('mod_contabo_price_decision')->where('id', $decisionId)->lockForUpdate()->first();
                if ($row === null) { $flash = 'Decision not found.'; return; }
                $row = (array) $row;
                if ((int) ($row['applied'] ?? 0) === 1 || (int) ($row['requires_admin_approval'] ?? 0) !== 1) {
                    $flash = 'Decision already resolved.'; return;
                }
                if (Capsule::table('mod_contabo_price_decision')->where('parent_decision_id', $decisionId)->exists()) {
                    $flash = 'Decision already resolved by another action.'; return;
                }

                $serviceId = (int) $row['service_id'];

                (new DecisionLog())->insert([
                    'service_id'              => $serviceId,
                    'profile_id'              => $row['profile_id'] ?? null,
                    'profile_version_id'      => $row['profile_version_id'] ?? null,
                    'cron_run_id'             => (string) ($row['cron_run_id'] ?? ('manual-' . date('YmdHis'))),
                    'billing_cycle'           => (string) ($row['billing_cycle'] ?? 'monthly'),
                    'cycle_months'            => (int) ($row['cycle_months'] ?? 1),
                    'currency'                => (string) ($row['currency'] ?? 'INR'),
                    'old_price'               => (float) ($row['old_price'] ?? 0),
                    'proposed_new_price'      => (float) ($row['proposed_new_price'] ?? 0),
                    'policy_used'             => (string) ($row['policy_used'] ?? 'manual'),
                    'applied'                 => 0,
                    'applied_via'             => null,
                    'skip_reason'             => 'admin_rejected',
                    'requires_admin_approval' => 0,
                    'parent_decision_id'      => $decisionId,
                ]);

                Capsule::table('mod_contabo_pricing_action')->insert([
                    'action_type' => 'reject',
                    'service_id'  => $serviceId,
                    'decision_id' => $decisionId,
                    'admin_id'    => $adminId,
                    'reason'      => 'admin_rejected',
                    'created_at'  => date('Y-m-d H:i:s'),
                ]);

                $flash = 'Rejected decision #' . $decisionId . ' — no price change applied.';
            });
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: approval-reject error (decision #' . $decisionId . ') — ' . $e->getMessage());
            }
            $flash = 'Rejection failed; see the activity log for detail.';
        }

        if (function_exists('logActivity')) {
            logActivity('Contabo Pricing: approval-reject by admin #' . $adminId . ' (decision #' . $decisionId . ') — ' . $flash);
        }
        $this->redirect('approval-queue', ['page' => $page, 'flash' => $flash]);
    }

    /** JSON badge count of unresolved approval-pending decisions. */
    private function ajaxApprovalCount(): void
    {
        try {
            $count = (int) $this->approvalPendingQuery()->count();
            $this->jsonOk(['count' => $count]);
        } catch (\Throwable $e) {
            $this->jsonOk(['count' => 0]);
        }
    }

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

        // DRY-RUN (preview only): report the exact blast radius — what the purge
        // WOULD remove — WITHOUT deleting or truncating anything. Read-only, so no
        // typed confirmation phrase is required.
        if (!empty($req['purge_dry_run'])) {
            $this->maintenancePurgePreview();
            return;
        }

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

        // A.6.5 (§19) — OPT-IN: also remove the WHMCS configurable options the addon
        // CREATED (groups/options/sub-options/pricing/product-links), scoped strictly
        // to the ids in the link tables. This MUST run BEFORE the truncate below,
        // because it reads mod_contabo_config_*_link to know what it owns — once those
        // are truncated the ownership records are gone. It never touches a config
        // object the addon didn't create, nor any client/service/invoice/order.
        $configPurge = '';
        if (!empty($req['purge_config_objects'])) {
            try {
                $c = (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();
                $configPurge = sprintf(
                    ' Addon-created WHMCS config objects removed: %d groups, %d options, %d sub-options, %d pricing rows, %d product links.',
                    (int) $c['groups'], (int) $c['options'], (int) $c['subs'], (int) $c['sub_pricing'], (int) $c['product_links']
                );
                if (function_exists('logActivity')) {
                    logActivity('Contabo Pricing: PURGE removed addon-created WHMCS config objects by admin #' . $adminId . ' —' . $configPurge);
                }
            } catch (\Throwable $e) {
                if (function_exists('logActivity')) {
                    logActivity('Contabo Pricing: config-object purge failed (mod_contabo_* NOT truncated) — ' . $e->getMessage());
                }
                $this->redirect('maintenance', ['flash' => 'Config-object purge failed; see activity log. No data was truncated.']);
                return;
            }
        }

        try {
            // Back up every addon table to a timestamped *_purgebackup_ table,
            // then truncate. NEVER touches non mod_contabo_* tables.
            $stamp = date('YmdHis');
            $tables = $this->addonTables();
            foreach ($tables as $t) {
                if (!preg_match('/^[a-zA-Z0-9_]+$/', $t)) {
                    throw new \RuntimeException('Refusing purge: unsafe table name: ' . $t);
                }
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
            $this->redirect('maintenance', ['flash' => 'Module data purged. Backup tables created with suffix _purgebackup_' . $stamp . '.' . $configPurge]);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: purge failed — ' . $e->getMessage());
            }
            $this->redirect('maintenance', ['flash' => 'Purge failed; see activity log. No tables were truncated past the failure point.']);
        }
    }

    /**
     * DRY-RUN preview of the purge: counts the exact blast radius — addon-created
     * WHMCS config objects (via ConfigPurgeService::previewRemoval) plus the
     * mod_contabo_* row counts a truncate would clear — and flashes it. Writes
     * nothing: no delete, no truncate, no backup.
     */
    private function maintenancePurgePreview(): void
    {
        try {
            $config = (new ConfigPurgeService())->previewRemoval();
            $tableCount = 0;
            $rowTotal   = 0;
            foreach ($this->addonTables() as $t) {
                $tableCount++;
                $rowTotal += (int) Capsule::table($t)->count();
            }
            $msg = sprintf(
                'DRY-RUN preview — nothing was deleted. A real purge would truncate %d mod_contabo_* tables (%d rows total) and, if the config-object option is ticked, remove addon-created WHMCS config objects: %d groups, %d options, %d sub-options, %d pricing rows, %d product links.',
                $tableCount, $rowTotal,
                (int) $config['groups'], (int) $config['options'], (int) $config['subs'], (int) $config['sub_pricing'], (int) $config['product_links']
            );
            $this->redirect('maintenance', ['flash' => $msg]);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: purge dry-run preview failed — ' . $e->getMessage());
            }
            $this->redirect('maintenance', ['flash' => 'Purge dry-run failed; see activity log. Nothing was changed.']);
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
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing tax-settings save error: ' . $e->getMessage());
            }
            $this->redirect('tax-settings', ['flash' => 'Save failed; see the activity log for detail.']);
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
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing service-pricing-tab render error (service #' . $serviceId . '): ' . $e->getMessage());
            }
            return '<link rel="stylesheet" href="/modules/addons/contabo_pricing/assets/app.css?v='
                . rawurlencode(self::VERSION)
                . '"><div class="cb-wrap"><div class="cb-error">'
                . 'Contabo Pricing tab unavailable — see the activity log for detail.'
                . '</div></div>';
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
