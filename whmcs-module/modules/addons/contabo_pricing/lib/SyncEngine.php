<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase A.5 — the catalog-only sync engine.
 *
 * Hard rules (carved into the code, enforced by tests):
 *
 *   - SyncEngine MUST NOT write to `tblhosting`. That is the renewal engine's
 *     job, and a single static-grep test + runtime fixture snapshot test
 *     enforce this. Catalog (`tblpricing`) only.
 *
 *   - Every `tblpricing` write considers the sentinel:
 *       -1.00 → DISABLED — never overwrite when `respect_disabled_cycles=1`
 *        0.00 → FREE     — never overwrite when `overwrite_free_cycles=0`
 *        >0   → priced   — may update when the cycle is in `catalog_cycles_mask`
 *        null → absent   — treated like priced (we have nothing to protect)
 *
 *   - Setup fee writes are gated by `sync_setup_fees=1`. Default off.
 *
 *   - Every (mapping × currency × cycle) decision emits exactly ONE
 *     `mod_contabo_catalog_audit` row, applied=1 or applied=0 + skipped_reason.
 *
 *   - One `sync_batch_id` (UUID) per SyncEngine::run() pass — every catalog
 *     audit row written in that pass shares the same id.
 *
 *   - Per-cycle markup overrides live in JSON on the mapping
 *     (`markup_overrides_json`). 'inherit' falls back to the profile_version's
 *     `markup_strategy` + `markup_value`.
 *
 * PHP 7.4 polyglot: typed properties only; no match, no readonly, no enums.
 */
class SyncEngine
{
    /** @var Settings */          private $settings;
    /** @var ApiClient */         private $api;
    /** @var ProfileManager */    private $profiles;
    /** @var CatalogAuditLog */   private $catalogAudit;

    /** @var float Hard threshold (50%) above which a change is "suspicious" */
    private const SUSPICIOUS_CHANGE_RATIO = 0.50;

    public function __construct(
        Settings $settings,
        ApiClient $api,
        ProfileManager $profiles,
        ?CatalogAuditLog $catalogAudit = null
    ) {
        $this->settings     = $settings;
        $this->api          = $api;
        $this->profiles     = $profiles;
        $this->catalogAudit = $catalogAudit ?? new CatalogAuditLog();
    }

    /**
     * @param string $trigger 'cron' | 'manual' | 'webhook'
     * @return array<string, mixed> summary
     */
    public function run(string $trigger = 'manual'): array
    {
        $startedAt = date('Y-m-d H:i:s');
        $logId = (int) Capsule::table('mod_contabo_sync_log')->insertGetId([
            'trigger'    => $trigger,
            'status'     => 'running',
            'started_at' => $startedAt,
        ]);

        $syncBatchId = self::uuidV4();

        $summary = [
            'trigger'           => $trigger,
            'sync_batch_id'     => $syncBatchId,
            'started_at'        => $startedAt,
            'profiles_checked'  => 0,
            'profiles_changed'  => 0,
            'products_updated'  => 0,
            'cycles_evaluated'  => 0,
            'cycles_applied'    => 0,
            'cycles_skipped'    => 0,
            'errors'            => [],
            'change_list'       => [],
        ];

        try {
            $meta = $this->api->meta();
            $sourceGeneratedAt = (string) ($meta['snapshot_meta']['generated_at'] ?? '');

            $lastSuccess = Capsule::table('mod_contabo_sync_log')
                ->where('status', 'succeeded')
                ->orderByDesc('started_at')
                ->first();
            if ($lastSuccess && $sourceGeneratedAt !== '') {
                $lastSummary = json_decode((string) ($lastSuccess->summary ?? '[]'), true);
                if (is_array($lastSummary) && ($lastSummary['snapshot_generated_at'] ?? null) === $sourceGeneratedAt) {
                    $summary['snapshot_generated_at'] = $sourceGeneratedAt;
                    $summary['status'] = 'no-change';
                    return $this->finalise($logId, 'no-change', $summary, null);
                }
            }
            $summary['snapshot_generated_at'] = $sourceGeneratedAt;

            // FX once per run — every profile uses the same rate snapshot.
            $fx = [];
            try { $fx = $this->api->fx(); } catch (\Throwable $e) { /* /fx still stub in API */ }
            $fxRate   = isset($fx['eurInr']) ? (float) $fx['eurInr'] : null;
            $fxSource = isset($fx['source']) ? (string) $fx['source'] : null;

            $byPlanSlug = [];
            foreach ($this->profiles->listProfiles(true) as $profile) {
                $summary['profiles_checked']++;
                try {
                    $planSlug = (string) $profile['plan_slug'];
                    if (!isset($byPlanSlug[$planSlug])) {
                        $byPlanSlug[$planSlug] = $this->api->plan($planSlug);
                    }
                    $plan = $byPlanSlug[$planSlug];
                    $period = $this->findPeriod($plan, (int) $profile['period_months']);
                    if ($period === null) {
                        $summary['errors'][] = "profile {$profile['slug']}: period {$profile['period_months']} mo not offered by Contabo for {$planSlug}";
                        continue;
                    }

                    $base       = (float) ($period['effective_monthly'] ?? 0);
                    $configured = $base; // Phase-3 vNext: apply selections{} via /quote
                    $setup      = (float) ($period['setup_fee'] ?? 0);

                    $version = ProfileVersionInput::computed(
                        $base,
                        $configured,
                        $setup,
                        [],
                        is_array($plan['specs_parsed'] ?? null) ? $plan['specs_parsed'] : [],
                        $fxRate,
                        $fxSource,
                        $this->settings->fxMarkupPct,
                        $this->settings->applyGst18,
                        $this->settings->currencyIso,
                        $sourceGeneratedAt
                    );

                    $latest = $this->profiles->latestVersion((int) $profile['id']);
                    $appended = false;
                    if ($version->differsFrom($latest)) {
                        $this->profiles->appendVersion((int) $profile['id'], $version);
                        $summary['profiles_changed']++;
                        $appended = true;
                        $summary['change_list'][] = [
                            'profile_slug'   => $profile['slug'],
                            'plan_slug'      => $planSlug,
                            'period_months'  => (int) $profile['period_months'],
                            'previous_final' => $latest['final_monthly'] ?? null,
                            'new_final'      => $version->finalMonthly,
                            'currency'       => $version->currencyIso,
                        ];
                    }

                    // Catalog walk runs every cycle on every pass — even when no
                    // new profile_version was appended — because mapping flags
                    // (catalog_cycles_mask, rounding_mode, …) can have changed
                    // independently of the upstream vendor price.
                    if ((string) ($profile['sync_strategy'] ?? '') === 'auto-apply') {
                        $catalogStats = $this->applyCatalogForProfile(
                            (int) $profile['id'],
                            (array) $profile,
                            $version,
                            $syncBatchId
                        );
                        $summary['products_updated'] += $catalogStats['products_touched'];
                        $summary['cycles_evaluated'] += $catalogStats['cycles_evaluated'];
                        $summary['cycles_applied']  += $catalogStats['cycles_applied'];
                        $summary['cycles_skipped']  += $catalogStats['cycles_skipped'];
                    }

                    unset($appended); // documentation marker; not needed downstream
                } catch (\Throwable $e) {
                    $summary['errors'][] = "profile {$profile['slug']}: " . $e->getMessage();
                }
            }

            $status = $summary['profiles_changed'] > 0 ? 'succeeded' : 'no-change';
            if (!empty($summary['errors']) && $summary['profiles_changed'] === 0) {
                $status = 'failed';
            }
            return $this->finalise($logId, $status, $summary, null);
        } catch (\Throwable $e) {
            return $this->finalise($logId, 'failed', $summary, $e->getMessage());
        }
    }

    /**
     * Walk every mapping × currency × cycle for a profile and apply the new
     * version's price to `tblpricing` (catalog only — NEVER `tblhosting`),
     * honouring the sentinel rules and emitting one audit row per decision.
     *
     * Public so tests can drive a single profile without round-tripping the
     * entire run() pipeline.
     *
     * @param array<string,mixed> $profile
     * @return array{products_touched:int,cycles_evaluated:int,cycles_applied:int,cycles_skipped:int}
     */
    public function applyCatalogForProfile(
        int $profileId,
        array $profile,
        ProfileVersionInput $version,
        string $syncBatchId
    ): array {
        $stats = [
            'products_touched' => 0,
            'cycles_evaluated' => 0,
            'cycles_applied'   => 0,
            'cycles_skipped'   => 0,
        ];

        $mappings = Capsule::table('mod_contabo_mapping')
            ->where('profile_id', $profileId)
            ->where('active', true)
            ->get();

        $activeCurrencies = $this->loadActiveCurrencies();
        if ($activeCurrencies === []) {
            return $stats;
        }

        foreach ($mappings as $rawMapping) {
            $mapping = (array) $rawMapping;
            $productId = (int) ($mapping['product_id'] ?? 0);
            if ($productId <= 0) {
                continue;
            }

            $catalogMaskInt = (int) ($mapping['catalog_cycles_mask'] ?? 0);
            $catalogSet = CycleSet::fromMask($catalogMaskInt);
            $respectDisabled = !empty($mapping['respect_disabled_cycles']);
            $overwriteFree   = !empty($mapping['overwrite_free_cycles']);
            $syncSetupFees   = !empty($mapping['sync_setup_fees']);
            $roundingMode    = (string) ($mapping['rounding_mode'] ?? 'exact_2_decimals');

            $touchedThisMapping = false;

            foreach ($activeCurrencies as $currency) {
                $currencyId = (int) $currency['id'];
                $currencyCode = (string) $currency['code'];

                $tblpricingRow = $this->loadTblpricingRow($productId, $currencyId);

                foreach (CycleSet::allCycles() as $cycle) {
                    $stats['cycles_evaluated']++;
                    $cycleMonths     = CycleNormalizer::monthsForCycle($cycle);
                    $recurringColumn = CyclePricingMap::getRecurringColumn($cycle);
                    $setupFeeColumn  = CyclePricingMap::getSetupFeeColumn($cycle);

                    $auditBase = [
                        'sync_batch_id'     => $syncBatchId,
                        'product_id'        => $productId,
                        'currency_id'       => $currencyId,
                        'currency_code'     => $currencyCode,
                        'cycle'             => $cycle,
                        'cycle_months'      => $cycleMonths,
                        'recurring_column'  => $recurringColumn,
                        'setup_fee_column'  => $setupFeeColumn,
                        'profile_id'        => $profileId,
                        'mapping_id'        => isset($mapping['id']) ? (int) $mapping['id'] : null,
                    ];

                    // 1) Catalog mask gate. Out-of-mask cycles audit-skipped.
                    if (!$catalogSet->contains($cycle)) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'        => 0,
                            'skipped_reason' => 'catalog_not_in_mask',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

                    // Recurring column always exists for in-mask cycles
                    // (the six canonical cycles all map). Defensive check.
                    if ($recurringColumn === null || $cycleMonths === null) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'        => 0,
                            'skipped_reason' => 'cycle_unsupported',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

                    $currentValue = null;
                    if ($tblpricingRow !== null && array_key_exists($recurringColumn, $tblpricingRow)) {
                        $raw = $tblpricingRow[$recurringColumn];
                        $currentValue = ($raw === null || $raw === '') ? null : (float) $raw;
                    }
                    $priceStatus = CyclePricingMap::priceStatusFromValue($currentValue);

                    // 2) Sentinel guards
                    if ($priceStatus === 'disabled' && $respectDisabled) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'              => 0,
                            'skipped_reason'       => 'catalog_skip_disabled_cycle',
                            'old_price'            => $currentValue,
                            'price_status_before'  => 'disabled',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }
                    if ($priceStatus === 'free' && !$overwriteFree) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'              => 0,
                            'skipped_reason'       => 'catalog_skip_free_cycle',
                            'old_price'            => $currentValue,
                            'price_status_before'  => 'free',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

                    // 3) Compute price for this cycle.
                    try {
                        list($preRound, $rounded, $markupStrategy, $markupValue) =
                            $this->computeCyclePrice($version, $mapping, $cycle, $cycleMonths, $roundingMode);
                    } catch (\Throwable $e) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'             => 0,
                            'skipped_reason'      => 'price_compute_error',
                            'old_price'           => $currentValue,
                            'price_status_before' => $priceStatus,
                            'cycle_pricing_notes' => $e->getMessage(),
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

                    // 4) Suspicious-change guard (only meaningful when there's
                    //    an existing positive price to compare against).
                    if ($priceStatus === 'priced'
                        && $currentValue !== null
                        && $currentValue > 0
                        && $this->isSuspicious((float) $currentValue, $rounded)
                    ) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'              => 0,
                            'skipped_reason'       => 'suspicious_change_blocked',
                            'old_price'            => $currentValue,
                            'pre_round_price'      => $preRound,
                            'rounded_price'        => $rounded,
                            'rounding_mode'        => $roundingMode,
                            'markup_strategy_used' => $markupStrategy,
                            'markup_value_used'    => $markupValue,
                            'price_status_before'  => $priceStatus,
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

                    // 5) Write the recurring column (catalog only).
                    $this->writeTblpricingCell($productId, $currencyId, $recurringColumn, $rounded);

                    // 6) Optional setup-fee write.
                    $oldSetupFee = null;
                    $newSetupFee = null;
                    if ($syncSetupFees && $setupFeeColumn !== null) {
                        if ($tblpricingRow !== null && array_key_exists($setupFeeColumn, $tblpricingRow)) {
                            $rawSetup = $tblpricingRow[$setupFeeColumn];
                            $oldSetupFee = ($rawSetup === null || $rawSetup === '') ? null : (float) $rawSetup;
                        }
                        $newSetupFee = $this->computeSetupFee($version, $mapping, $cycle, $cycleMonths, $roundingMode);
                        if ($newSetupFee !== null) {
                            $this->writeTblpricingCell($productId, $currencyId, $setupFeeColumn, $newSetupFee);
                        }
                    }

                    $this->catalogAudit->insert(array_merge($auditBase, [
                        'applied'              => 1,
                        'old_price'            => $currentValue,
                        'new_price'            => $rounded,
                        'old_setup_fee'        => $oldSetupFee,
                        'new_setup_fee'        => $newSetupFee,
                        'price_status_before'  => $priceStatus,
                        'markup_strategy_used' => $markupStrategy,
                        'markup_value_used'    => $markupValue,
                        'pre_round_price'      => $preRound,
                        'rounded_price'        => $rounded,
                        'rounding_mode'        => $roundingMode,
                    ]));
                    $stats['cycles_applied']++;
                    $touchedThisMapping = true;
                }
            }

            if ($touchedThisMapping) {
                $stats['products_touched']++;
            }
        }

        return $stats;
    }

    /**
     * Compute the per-cycle catalog sell price using MarginCalculator and the
     * resolved markup (per-cycle override or profile_version inherit), then
     * apply the mapping's rounding mode.
     *
     * Returns [preRoundPrice, roundedPrice, markupStrategyUsed, markupValueUsed].
     *
     * @param array<string,mixed> $mapping
     * @return array{0:float,1:float,2:string,3:float}
     */
    private function computeCyclePrice(
        ProfileVersionInput $version,
        array $mapping,
        string $cycle,
        int $cycleMonths,
        string $roundingMode
    ): array {
        $markup = $this->resolveMarkup($mapping, $cycle);

        $strategy = $markup['strategy'];
        $value    = $markup['value'];

        // 'inherit' means take the markup from the version (PHP 7.4: no null
        // coalescing assignment, write out the conditional).
        if ($strategy === 'inherit') {
            $strategy = $this->resolveVersionMarkupStrategy($version);
            $value    = $this->resolveVersionMarkupValue($version);
        }

        // For now `landedMonthly` in this rewrite phase is the same value the
        // existing pipeline produced: `version.finalMonthly` is already EUR ×
        // FX × (1 + gst) × (1 + fxMarkup). When Agent C lands the full
        // landed-cost pipeline this gets swapped for MarginCalculator
        // directly. Until then we use finalMonthly as the cost basis so the
        // engine continues to produce sensible numbers in production.
        $landedMonthly = (float) $version->finalMonthly;

        $preRoundCycle = MarginCalculator::sellPriceForCycle(
            $landedMonthly,
            $strategy,
            (float) $value,
            null, // 'fixed' not supported via per-cycle override here yet
            $cycleMonths
        );

        // sellPriceForCycle already rounds to 2dp; pre-round-of-final-write is
        // the same number prior to mapping-level rounding mode.
        $rounded = $this->applyRounding($preRoundCycle, $roundingMode);

        return [$preRoundCycle, $rounded, $strategy, (float) $value];
    }

    /**
     * Setup-fee per cycle, derived from the profile_version's setup fee.
     * For now we mirror MarginCalculator-style rounding (mapping's rounding
     * mode) and treat the setup-fee in the version as a single per-product
     * value (WHMCS stores per-cycle setup fees, but the upstream snapshot only
     * gives us one number).
     *
     * Returns null when the version carries no setup fee — caller skips the
     * column write entirely in that case.
     *
     * @param array<string,mixed> $mapping
     */
    private function computeSetupFee(
        ProfileVersionInput $version,
        array $mapping,
        string $cycle,
        int $cycleMonths,
        string $roundingMode
    ): ?float {
        unset($mapping, $cycle, $cycleMonths); // accepted for symmetry / future use

        $fee = (float) $version->finalSetup;
        if ($fee <= 0.0) {
            return null;
        }
        return $this->applyRounding($fee, $roundingMode);
    }

    /**
     * Resolve the per-cycle markup configuration from
     * `mapping.markup_overrides_json`. Falls back to {'inherit', 0} when no
     * override exists for the cycle.
     *
     * @param array<string,mixed> $mapping
     * @return array{strategy:string,value:float,source:string}
     */
    private function resolveMarkup(array $mapping, string $cycle): array
    {
        $json = (string) ($mapping['markup_overrides_json'] ?? '');
        $overrides = $json === '' ? [] : json_decode($json, true);
        if (!is_array($overrides)) {
            $overrides = [];
        }

        if (isset($overrides[$cycle]) && is_array($overrides[$cycle])) {
            return [
                'strategy' => (string) ($overrides[$cycle]['strategy'] ?? 'inherit'),
                'value'    => (float)  ($overrides[$cycle]['value']    ?? 0.0),
                'source'   => 'cycle_override',
            ];
        }

        return [
            'strategy' => 'inherit',
            'value'    => 0.0,
            'source'   => 'mapping_default',
        ];
    }

    /**
     * The profile_version may carry an explicit markup_strategy from Agent A's
     * schema work. ProfileVersionInput in the current codebase is a typed
     * value object that doesn't (yet) expose those fields directly; until it
     * does, fall back to 'cost_plus_pct' with markup value 0 so the catalog
     * computation produces an unmarked-up landed-cost-equivalent number
     * (preserves backward compatibility).
     */
    private function resolveVersionMarkupStrategy(ProfileVersionInput $version): string
    {
        if (property_exists($version, 'markupStrategy')
            && is_string($version->{'markupStrategy'} ?? null)
            && $version->{'markupStrategy'} !== ''
        ) {
            return (string) $version->{'markupStrategy'};
        }
        return 'cost_plus_pct';
    }

    private function resolveVersionMarkupValue(ProfileVersionInput $version): float
    {
        if (property_exists($version, 'markupValue')) {
            return (float) ($version->{'markupValue'} ?? 0.0);
        }
        return 0.0;
    }

    /**
     * Mapping-level rounding modes. Reads inputs as float, returns a float.
     */
    private function applyRounding(float $price, string $mode): float
    {
        switch ($mode) {
            case 'exact_2_decimals':
                return round($price, 2);

            case 'nearest_rupee':
                return round($price, 0);

            case 'nearest_9':
                // Hits the next "9-tail" at the tens place: 1234 → 1239,
                // 1234.567 → 1239, 1230 → 1239 (we don't go DOWN to 1229).
                $base = (int) floor($price / 10.0) * 10;
                $candidate = (float) ($base + 9);
                if ($candidate < $price) {
                    $candidate += 10.0;
                }
                return $candidate;

            case 'nearest_99':
                // Next "99-tail" at the hundreds place: 1234.567 → 1299.
                $base = (int) floor($price / 100.0) * 100;
                $candidate = (float) ($base + 99);
                if ($candidate < $price) {
                    $candidate += 100.0;
                }
                return $candidate;

            case 'nearest_100':
                return round($price / 100.0) * 100.0;

            case 'custom':
                // TODO(phase-a.6): take a printf-style template from
                // settings.setup_fee_overrides_json / a sibling settings JSON
                // and apply it here. For now degrades to exact_2_decimals.
                return round($price, 2);

            default:
                if (function_exists('logActivity')) {
                    logActivity(sprintf(
                        'Contabo Pricing: unknown rounding_mode "%s", falling back to exact_2_decimals.',
                        $mode
                    ));
                }
                return round($price, 2);
        }
    }

    /**
     * 50% absolute-jump guard. Tests it both ways (up and down) — large
     * decreases are equally suspicious from a catalog hygiene standpoint.
     *
     * Returns true when |new - old| / old > SUSPICIOUS_CHANGE_RATIO.
     */
    private function isSuspicious(float $oldPrice, float $newPrice): bool
    {
        if ($oldPrice <= 0.0) {
            // Nothing to compare against — caller should already have skipped
            // this branch via the sentinel guards.
            return false;
        }
        $delta = abs($newPrice - $oldPrice);
        return ($delta / $oldPrice) > self::SUSPICIOUS_CHANGE_RATIO;
    }

    /**
     * @return array<string,mixed>|null Row as assoc array, or null when absent.
     */
    private function loadTblpricingRow(int $productId, int $currencyId): ?array
    {
        try {
            $row = Capsule::table('tblpricing')
                ->where('type', 'product')
                ->where('currency', $currencyId)
                ->where('relid', $productId)
                ->first();
        } catch (\Throwable $e) {
            return null;
        }
        if ($row === null) {
            return null;
        }
        return (array) $row;
    }

    /**
     * Single-cell `tblpricing` UPDATE. Wrapped so static greps for the
     * `Capsule::table('tblpricing')` write surface land here.
     */
    private function writeTblpricingCell(int $productId, int $currencyId, string $column, float $value): void
    {
        Capsule::table('tblpricing')
            ->where('type', 'product')
            ->where('currency', $currencyId)
            ->where('relid', $productId)
            ->update([$column => $value]);
    }

    /**
     * Walk `tblcurrencies` and return [['id'=>1,'code'=>'INR'], …]. Active
     * currencies only — WHMCS has no `active` column so all rows are returned.
     *
     * @return list<array{id:int,code:string}>
     */
    private function loadActiveCurrencies(): array
    {
        try {
            $rows = Capsule::table('tblcurrencies')->get();
        } catch (\Throwable $e) {
            return [];
        }
        $out = [];
        foreach ($rows as $r) {
            $r = (array) $r;
            $id   = (int) ($r['id'] ?? 0);
            $code = (string) ($r['code'] ?? '');
            if ($id > 0 && $code !== '') {
                $out[] = ['id' => $id, 'code' => $code];
            }
        }
        return $out;
    }

    /**
     * @param array<string, mixed> $plan
     * @return array<string, mixed>|null
     */
    private function findPeriod(array $plan, int $months): ?array
    {
        foreach (($plan['periods'] ?? []) as $p) {
            if ((int) ($p['months'] ?? 0) === $months) {
                return $p;
            }
        }
        return null;
    }

    /**
     * @param array<string, mixed> $summary
     */
    private function finalise(int $logId, string $status, array $summary, ?string $err): array
    {
        $summary['status']        = $status;
        $summary['finished_at']   = date('Y-m-d H:i:s');
        $summary['error_message'] = $err;

        Capsule::table('mod_contabo_sync_log')->where('id', $logId)->update([
            'status'           => $status,
            'finished_at'      => $summary['finished_at'],
            'profiles_checked' => $summary['profiles_checked'],
            'profiles_changed' => $summary['profiles_changed'],
            'products_updated' => $summary['products_updated'],
            'error_message'    => $err,
            'summary'          => json_encode($summary, JSON_UNESCAPED_SLASHES),
        ]);
        return $summary;
    }

    /**
     * RFC 4122 v4 UUID. Avoids external dependency on a uuid lib; entropy via
     * random_bytes(). Used as `sync_batch_id` per SyncEngine::run() pass.
     */
    private static function uuidV4(): string
    {
        try {
            $data = random_bytes(16);
        } catch (\Throwable $e) {
            $data = md5(uniqid('', true) . microtime(true), true);
        }
        // Set version (4) and variant (RFC 4122) bits.
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
