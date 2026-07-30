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

    /** @var list<string> */
    private const RECURRING_COLUMNS = [
        'monthly',
        'quarterly',
        'semiannually',
        'annually',
        'biennially',
        'triennially',
    ];

    /** @var list<string> */
    private const SETUP_FEE_COLUMNS = [
        'msetupfee',
        'qsetupfee',
        'ssetupfee',
        'asetupfee',
        'bsetupfee',
        'tsetupfee',
    ];

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

                    // Profile = SOURCE: capture the FULL per-period EUR vector so
                    // every published cycle prices off its own scraped discount
                    // tier, not the single profile period. Absent cycles (24/36)
                    // are pre-expanded to the longest available period's rate.
                    $periodPricesEur = self::periodPriceVectorFromPlan($plan);

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
                        $sourceGeneratedAt,
                        $periodPricesEur
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

        // Profile = SOURCE authority for WHICH cycles are published. A cycle the
        // profile doesn't publish is never written, regardless of the mapping's
        // catalog mask. Absent (legacy / test drivers that pass a non-profile
        // array) defaults to "all published" so pre-v8 behaviour is preserved.
        $publishedSet = CycleSet::fromMask(
            isset($profile['published_cycles_mask'])
                ? (int) $profile['published_cycles_mask']
                : CycleSet::MASK_MAX
        );

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

                    // 0) Publish gate (profile-owned). A cycle the profile does
                    //    not publish is skipped before any per-product mask.
                    if (!$publishedSet->contains($cycle)) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'        => 0,
                            'skipped_reason' => 'cycle_not_published',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    }

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
                    } catch (PricingInvariantViolation $e) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'             => 0,
                            'skipped_reason'      => $e->reason(),
                            'old_price'           => $currentValue,
                            'price_status_before' => $priceStatus,
                            'cycle_pricing_notes' => $e->getMessage(),
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
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

                    // 5) Compute every value before opening the local write
                    // transaction. An invalid setup fee must not leave the
                    // recurring price updated on its own.
                    $oldSetupFee = null;
                    $newSetupFee = null;
                    if ($syncSetupFees && $setupFeeColumn !== null) {
                        if ($tblpricingRow !== null && array_key_exists($setupFeeColumn, $tblpricingRow)) {
                            $rawSetup = $tblpricingRow[$setupFeeColumn];
                            $oldSetupFee = ($rawSetup === null || $rawSetup === '') ? null : (float) $rawSetup;
                        }
                        try {
                            $newSetupFee = $this->computeSetupFee(
                                $version,
                                $mapping,
                                $cycle,
                                $cycleMonths,
                                $roundingMode
                            );
                        } catch (PricingInvariantViolation $e) {
                            $this->catalogAudit->insert(array_merge($auditBase, [
                                'applied'             => 0,
                                'skipped_reason'      => $e->reason(),
                                'old_price'           => $currentValue,
                                'price_status_before' => $priceStatus,
                                'cycle_pricing_notes' => $e->getMessage(),
                            ]));
                            $stats['cycles_skipped']++;
                            continue;
                        }
                    }

                    // 6) Apply the recurring price and optional setup fee as
                    // one local MySQL transaction.
                    try {
                        Capsule::connection()->transaction(function () use (
                            $productId,
                            $currencyId,
                            $recurringColumn,
                            $rounded,
                            $setupFeeColumn,
                            $newSetupFee
                        ): void {
                            $this->writeTblpricingCell(
                                $productId,
                                $currencyId,
                                $recurringColumn,
                                $rounded
                            );
                            if ($setupFeeColumn !== null && $newSetupFee !== null) {
                                $this->writeTblpricingCell(
                                    $productId,
                                    $currencyId,
                                    $setupFeeColumn,
                                    $newSetupFee
                                );
                            }
                        });
                    } catch (PricingInvariantViolation $e) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'             => 0,
                            'skipped_reason'      => $e->reason(),
                            'old_price'           => $currentValue,
                            'price_status_before' => $priceStatus,
                            'cycle_pricing_notes' => $e->getMessage(),
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
                    } catch (\Throwable $e) {
                        $this->catalogAudit->insert(array_merge($auditBase, [
                            'applied'             => 0,
                            'skipped_reason'      => 'price_write_error',
                            'old_price'           => $currentValue,
                            'price_status_before' => $priceStatus,
                            'cycle_pricing_notes' => 'Catalog pricing transaction failed',
                        ]));
                        $stats['cycles_skipped']++;
                        continue;
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

        // Per-cycle SOURCE (cost) basis in local currency. The PROFILE owns the
        // source: the version's per-period EUR vector, with the longest-available
        // period's rate standing in for absent cycles (24/36). A per-product
        // mapping `source_overrides_json` may pin its own basis (customer layer).
        // Legacy versions with no vector fall back to the single finalMonthly
        // basis so pre-v8 rows keep producing sensible numbers until the next
        // sync repopulates the vector.
        $sourceEur = $this->resolveCycleSourceEur($version, $mapping, $cycle, $cycleMonths);
        if ($sourceEur !== null) {
            PriceInvariant::requirePositiveFinite(
                $sourceEur,
                'missing_source_price',
                'Provider source price'
            );
            $landedMonthly = ProfileVersionInput::toLocalMonthly(
                $sourceEur,
                $version->fxRate,
                $version->fxMarkupPct,
                $version->gstPct / 100.0,
                $version->currencyIso
            );
        } else {
            $landedMonthly = (float) $version->finalMonthly;
        }
        PriceInvariant::requirePositiveFinite(
            $landedMonthly,
            'missing_source_price',
            'Landed monthly source price'
        );

        // For 'fixed' strategy, $value is the admin-set total sell price for
        // this cycle. Convert to monthly so sellPriceForCycle can use it.
        $fixedCycleTotal = null;
        if ($strategy === 'fixed') {
            $fixedCycleTotal = $cycleMonths > 0 ? ($value / $cycleMonths) : $value;
        }

        $preRoundCycle = MarginCalculator::sellPriceForCycle(
            $landedMonthly,
            $strategy,
            (float) $value,
            $fixedCycleTotal,
            $cycleMonths
        );
        PriceInvariant::requirePositiveFinite(
            $preRoundCycle,
            'price_invariant_violation',
            'Computed cycle price'
        );

        // sellPriceForCycle already rounds to 2dp; pre-round-of-final-write is
        // the same number prior to mapping-level rounding mode.
        $rounded = Rounding::apply($preRoundCycle, $roundingMode);
        PriceInvariant::requirePositiveFinite(
            $rounded,
            'price_invariant_violation',
            'Rounded cycle price'
        );

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
        if ($fee === 0.0) {
            return null;
        }
        PriceInvariant::requireNonNegativeFinite(
            $fee,
            'price_invariant_violation',
            'Setup fee'
        );
        $rounded = Rounding::apply($fee, $roundingMode);
        PriceInvariant::requireNonNegativeFinite(
            $rounded,
            'price_invariant_violation',
            'Rounded setup fee'
        );
        return $rounded;
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
     * Single-cell `tblpricing` UPDATE with a final price invariant. Recurring
     * computed prices must be positive; setup fees may legitimately be zero.
     */
    private function writeTblpricingCell(int $productId, int $currencyId, string $column, float $value): void
    {
        if (in_array($column, self::RECURRING_COLUMNS, true)) {
            PriceInvariant::requirePositiveFinite(
                $value,
                'price_invariant_violation',
                'Recurring catalog write'
            );
        } elseif (in_array($column, self::SETUP_FEE_COLUMNS, true)) {
            PriceInvariant::requireNonNegativeFinite(
                $value,
                'price_invariant_violation',
                'Setup-fee catalog write'
            );
        } else {
            throw new PricingInvariantViolation(
                'price_invariant_violation',
                'Unknown tblpricing column is not eligible for a write'
            );
        }

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
     * Build the per-period EUR SOURCE vector for a plan, keyed by the six
     * canonical cycle month-counts (1/3/6/12/24/36). Scraped periods carry their
     * real `effective_monthly`; cycles Contabo never exposes on the public site
     * take the rate of the LONGEST available scraped period whose months do not
     * EXCEED the target (owner rule):
     *
     *   - 24/36 mo → 12-mo rate (Contabo only exposes ≤12mo publicly; 24/36 are
     *     post-provision upgrade options, so we project from the deepest public
     *     tier).
     *   - a missing 3-mo (quarterly) → 1-mo rate (NOT a deeper longer-cycle
     *     discount the customer never qualified for).
     *
     * i.e. the source for target M months = effective_monthly of
     * max(scraped months ≤ M). 1-mo is always present, so a basis always exists.
     * Returns [] when the plan exposes no usable periods (caller leaves the
     * version vector empty → legacy single-basis fallback downstream).
     *
     * Public + static: a pure transformation (plan → vector) so it's unit-tested
     * directly without the run() plumbing.
     *
     * @param array<string,mixed> $plan
     * @return array<int,float>
     */
    public static function periodPriceVectorFromPlan(array $plan): array
    {
        $scraped = [];
        foreach (($plan['periods'] ?? []) as $p) {
            $m = (int) ($p['months'] ?? 0);
            if ($m > 0 && isset($p['effective_monthly'])) {
                $scraped[$m] = (float) $p['effective_monthly'];
            }
        }
        if ($scraped === []) {
            return [];
        }

        $vector = [];
        foreach (CycleSet::allCycles() as $cycle) {
            $months = CycleNormalizer::monthsForCycle($cycle);
            if ($months === null) {
                continue;
            }
            $vector[$months] = self::nearestSourceRate($scraped, $months);
        }
        return $vector;
    }

    /**
     * Source monthly EUR for a target cycle: the effective_monthly of the
     * longest scraped period whose months do NOT exceed the target. Falls back
     * to the shortest available period when nothing is ≤ target (defensive; 1-mo
     * is always present in real data).
     *
     * @param array<int,float> $scraped months => effective_monthly
     */
    private static function nearestSourceRate(array $scraped, int $targetMonths): float
    {
        $bestMonths = null;
        foreach ($scraped as $m => $rate) {
            if ($m <= $targetMonths && ($bestMonths === null || $m > $bestMonths)) {
                $bestMonths = $m;
            }
        }
        if ($bestMonths === null) {
            $bestMonths = min(array_keys($scraped));
        }
        return (float) $scraped[$bestMonths];
    }

    /**
     * Resolve the per-cycle SOURCE basis in EUR/month, in precedence order:
     *   1. mapping.source_overrides_json[cycle].monthly_eur — per-product pin.
     *   2. the profile version's per-period vector (exact cycle months, else the
     *      longest available period's rate).
     * Returns null only when the version carries no vector (legacy) — the caller
     * then falls back to version.finalMonthly.
     *
     * @param array<string,mixed> $mapping
     */
    private function resolveCycleSourceEur(
        ProfileVersionInput $version,
        array $mapping,
        string $cycle,
        int $cycleMonths
    ): ?float {
        $overrideJson = (string) ($mapping['source_overrides_json'] ?? '');
        if ($overrideJson !== '') {
            $overrides = json_decode($overrideJson, true);
            if (is_array($overrides) && isset($overrides[$cycle]) && is_array($overrides[$cycle])) {
                $entry = $overrides[$cycle];
                if (isset($entry['monthly_eur']) && is_numeric($entry['monthly_eur'])) {
                    return (float) $entry['monthly_eur'];
                }
            }
        }

        $vector = $version->periodPricesEur;
        if ($vector === []) {
            return null;
        }
        if (isset($vector[$cycleMonths])) {
            return (float) $vector[$cycleMonths];
        }
        // Vector is normally pre-expanded to all six cycles; this is a defensive
        // path for a sparse vector — same rule as the builder (longest ≤ target).
        return self::nearestSourceRate($vector, $cycleMonths);
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
