<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Orchestrates the renewal pricing decision for a single service.
 *
 *   decide($service, $now)
 *      → walks the algorithm from plan deliverable 2 (early skips first,
 *        then policy hierarchy, then applyWithGuards()) and returns a
 *        DECISION PAYLOAD ready to be inserted via DecisionLog::insert().
 *
 *   decideForScheduledChange($service, $forcedPrice, $now)
 *      → thin wrapper for ScheduledChangeProcessor that forces
 *        candidate = $forcedPrice (admin-set new_price) then runs the SAME
 *        applyWithGuards() path as decide(). The phrase "the same write path"
 *        is load-bearing: scheduled changes must NOT introduce a second
 *        unsafe writer surface — every byte of guard logic is shared.
 *
 * Phase A.5 contract (observe-only on the renewal side):
 *   - This class still NEVER calls ServicePriceWriter directly. The writer is
 *     wired with enabled=false at the addon level until Phase B.
 *   - What CHANGED in A.5: the engine now reads `renewal_cycles_mask` and
 *     `markup_overrides_json` from the mapping row, gates by cycle (new
 *     skip_reason `cycle_not_mapped`), resolves per-cycle markup, applies
 *     the configured Rounding mode and emits a metadata_json sidecar on every
 *     decision row that snapshots the per-cycle inputs/outputs used.
 *   - `skip_reason='cycle_unsupported'` keeps its A semantics: Free Account,
 *     One Time, anything CycleNormalizer cannot convert.
 *     `skip_reason='cycle_not_mapped'` is NEW: cycle is recurring (Quarterly,
 *     Annually, …) but absent from `renewal_cycles_mask`.
 *
 * Phase A (observe-only): at the end of decide(), if
 * settings.repricing_phase === 'observe' the decision is FORCED to
 * `applied=false, skip_reason='phase_observe_only'`. ServicePriceWriter is
 * NEVER called from this class. This contract is the safety net that lets
 * the engine ship without touching tblhosting.
 *
 * Dependencies are plumbed in by constructor; nothing is fetched from
 * globals. Tests can:
 *   - inject a $serviceFetcher to bypass `tblhosting` lookups,
 *   - inject `$now` to time-warp policy windows,
 *   - subclass PolicyResolver to bypass `mod_contabo_service_policy`.
 *
 * PHP 7.4 polyglot. Typed properties OK; no constructor promotion, no readonly,
 * no match, no enums.
 */
class RenewalEngine
{
    /** @var array<string, mixed> Resolved settings — see decide() for the keys this reads. */
    private $settings;

    /** @var PolicyResolver */
    private $policyResolver;

    /** @var callable|null Optional `(int serviceId): ?array<string,mixed>` for tests. */
    private $serviceFetcher;

    /** @var ServiceRevenueResolver|null Opt-in true-revenue resolver (amendment 5). */
    private $revenueResolver;

    /** @var ServiceConfigSnapshot|null Opt-in snapshot reader (preferred revenue source). */
    private $snapshotReader;

    /** @var string Per-engine instance cron-run UUID. Tests can override. */
    private $cronRunId;

    /**
     * @param array<string, mixed> $settings Resolved settings bag. Keys read:
     *     - repricing_phase                ('observe'|'opt_in'|'enforce')
     *     - tax_registration_mode          (one of TaxModeEngine::modes())
     *     - vendor_tax_rate_pct            (float)
     *     - vendor_tax_recoverable         (bool, often derived from mode)
     *     - prices_include_output_tax      (bool)
     *     - output_tax_rate_pct            (float)
     *     - payment_buffer_pct             (float)
     *     - fx_rate                        (float — caller resolves)
     *     - fx_unavailable                 (bool — caller signals here)
     *     - cron_run_id                    (string — UUID per cron pass, optional)
     * @param PolicyResolver|null   $policyResolver
     * @param callable|null         $serviceFetcher Optional `fn(int $id): ?array`
     */
    public function __construct(
        array $settings,
        ?PolicyResolver $policyResolver = null,
        ?callable $serviceFetcher = null,
        ?ServiceRevenueResolver $revenueResolver = null,
        ?ServiceConfigSnapshot $snapshotReader = null
    ) {
        $this->settings        = $settings;
        $this->policyResolver  = $policyResolver !== null ? $policyResolver : new PolicyResolver();
        $this->serviceFetcher  = $serviceFetcher;
        // Opt-in (amendment 5): when supplied, the engine records each service's
        // TRUE revenue (snapshot-preferred) + the drift from the stale
        // tblhosting.recurringamount in decision metadata. It does NOT yet drive
        // the margin/floor decision — that needs landedCostWithSelections (the
        // matching cost side), which §13 designates a Phase B step. Recording it
        // here surfaces the undercharge signal without skewing the base-only
        // margin math. Default (null) = unchanged behaviour.
        $this->revenueResolver = $revenueResolver;
        $this->snapshotReader  = $snapshotReader;
        $this->cronRunId      = isset($settings['cron_run_id']) && $settings['cron_run_id'] !== ''
            ? (string) $settings['cron_run_id']
            : self::uuidV4();
    }

    /**
     * Run the decision algorithm against one service row.
     *
     * @param array<string, mixed>   $service A row that looks like `tblhosting`
     *                                        joined with the addon's mapping /
     *                                        profile / version data. Required
     *                                        keys include id, status,
     *                                        billingcycle, recurringamount,
     *                                        subscriptionid, plus addon-side
     *                                        keys profile, profile_version,
     *                                        mapping (Phase A.5: carries
     *                                        renewal_cycles_mask /
     *                                        catalog_cycles_mask /
     *                                        markup_overrides_json /
     *                                        rounding_mode), flags
     *                                        (unpaid_invoice, pending_upgrade,
     *                                        promo_applied,
     *                                        on_demand_renewal_in_flight,
     *                                        explicitly_opted_in,
     *                                        plan_discontinued, no_mapping,
     *                                        missing_baseline_version,
     *                                        manual_edit_detected).
     * @param \DateTimeImmutable|null $now    Test time-warp; defaults to "now".
     *
     * @return array<string, mixed> Decision payload ready for DecisionLog::insert().
     */
    public function decide(array $service, ?\DateTimeImmutable $now = null): array
    {
        return $this->decideInternal($service, $now, /*forcedCandidate*/ null);
    }

    /**
     * Scheduled-change variant: forces the candidate sell price to
     * `$forcedNewPrice` (admin-supplied) and runs the SAME applyWithGuards()
     * path as decide(). There is intentionally NO second write path.
     *
     * Comment block / contract: ScheduledChangeProcessor MUST call this method
     * and not poke ServicePriceWriter directly. The phase gate, decrease guard,
     * max-increase ceiling, large-increase soft threshold and notice gate all
     * apply identically. The only difference is the candidate price source.
     *
     * @param array<string, mixed>   $service        Same shape as decide().
     * @param float                  $forcedNewPrice Admin-set target price
     *                                               (in cycle-currency units).
     * @param \DateTimeImmutable|null $now           Test time-warp.
     * @return array<string, mixed>                  Decision payload.
     */
    public function decideForScheduledChange(
        array $service,
        float $forcedNewPrice,
        ?\DateTimeImmutable $now = null
    ): array {
        return $this->decideInternal($service, $now, $forcedNewPrice);
    }

    /**
     * Shared decision pipeline used by both decide() and
     * decideForScheduledChange(). When $forcedCandidate is non-null the
     * computed candidate is REPLACED by the forced value AFTER the early
     * skips + cycle gate run but BEFORE the policy hierarchy / guards — so a
     * scheduled change still respects cycle masks, suspended status, etc.
     *
     * @param array<string, mixed> $service
     */
    private function decideInternal(
        array $service,
        ?\DateTimeImmutable $now,
        ?float $forcedCandidate
    ): array {
        if ($now === null) {
            $now = new \DateTimeImmutable('now');
        }

        // `service_amount` is the canonical normalized service-row key (derived
        // from the real tblhosting.amount column by the caller). `recurringamount`
        // is kept as a backward-compatible alias — it is a normalized row key
        // here, NOT a raw DB column. DB callers must alias `amount AS recurringamount`
        // (or set service_amount) when building this row.
        $oldPrice   = (float) ($service['service_amount'] ?? $service['recurringamount'] ?? 0.0);
        $cycle      = (string) ($service['billingcycle'] ?? '');
        $cycleMonths= CycleNormalizer::monthsForCycle($cycle);
        $mapping    = isset($service['mapping']) && is_array($service['mapping']) ? $service['mapping'] : [];

        // Build the decision metadata sidecar early so every code path can
        // append to it. Keys land in `metadata_json` on the decision row.
        $meta = [
            'cycle_recurring_column' => CyclePricingMap::getRecurringColumn($cycle),
            'cycle_setup_fee_column' => CyclePricingMap::getSetupFeeColumn($cycle),
            'catalog_cycle_enabled'  => null,
            'renewal_cycle_enabled'  => null,
            'markup_strategy_used'   => null,
            'markup_value_used'      => null,
            'markup_source'          => null,
            'pre_round_price'        => null,
            'rounded_price'          => null,
            'rounding_mode'          => null,
            // Amendment 5 — true revenue vs the stale stored charge. Recorded
            // for audit + the eventual Phase B margin use; does NOT drive this
            // decision (the cost side, landedCostWithSelections, is Phase B).
            'stale_recurringamount'  => $oldPrice,
            'resolved_revenue'       => null,
            'revenue_source'         => null,
            'revenue_drift'          => null,
            // Phase B (§13) — whole-config margin (base + selected options).
            // Recorded as an accurate undercharge signal; does NOT yet drive the
            // base candidate/floor decision (that's the final integration step).
            'margin_basis'                   => 'base_only',
            'whole_config_landed_for_cycle'  => null,
            'whole_config_margin_ratio'      => null,
            'whole_config_below_floor'       => null,
        ];

        // Phase B inputs (populated from the snapshot when present).
        $resolvedRevenueTotal   = null;
        $wholeConfigSelections  = [];

        if ($this->revenueResolver !== null) {
            $svcId = (int) ($service['id'] ?? 0);
            if ($svcId > 0) {
                $snap = $this->snapshotReader !== null ? $this->snapshotReader->latestForService($svcId) : null;
                $rev = $snap !== null
                    ? $this->revenueResolver->resolveFromSnapshot($snap)
                    : $this->revenueResolver->resolveForService($svcId);
                $meta['revenue_source']   = $snap !== null ? 'snapshot' : 'live';
                $meta['resolved_revenue'] = (float) ($rev['total'] ?? 0.0);
                $meta['revenue_drift']    = round($meta['resolved_revenue'] - $oldPrice, 4);
                $resolvedRevenueTotal     = (float) ($rev['total'] ?? 0.0);
                // The snapshot carries each selection's EUR delta — the cost
                // basis the whole-config landed cost needs. (Live resolves don't
                // carry EUR deltas, so whole-config margin requires a snapshot.)
                if ($snap !== null) {
                    $decoded = json_decode((string) ($snap['selected_options_json'] ?? '[]'), true);
                    if (is_array($decoded)) {
                        $wholeConfigSelections = $decoded;
                    }
                }
            }
        }

        // ── Edge-case early skips ──────────────────────────────────────────
        $earlySkip = $this->earlySkipReason($service);
        if ($earlySkip !== null) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, $earlySkip,
                /*requires_notice*/ false, /*requires_admin_approval*/ false,
                /* ...landed / tax */ null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        // cycle_unsupported = WHMCS Free Account / One Time / anything not in
        // CycleNormalizer. cycle_not_mapped (below) = recurring cycle absent
        // from renewal_cycles_mask. Two distinct skip reasons by design.
        if ($cycleMonths === null) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, null,
                'unknown', false, 'cycle_unsupported',
                false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        // ── Cycle gate by renewal_cycles_mask (Phase A.5) ──────────────────
        // The mask defines which recurring cycles the renewal engine evaluates
        // at all. A service on Quarterly with renewal_cycles_mask=Monthly+Annually
        // is intentionally untouched by renewal repricing. catalog_cycle_enabled
        // is captured for audit / cross-reference with Agent B's catalog rows.
        $renewalMask  = (int) ($mapping['renewal_cycles_mask'] ?? 0);
        $catalogMask  = (int) ($mapping['catalog_cycles_mask'] ?? 0);
        $renewalSet   = CycleSet::fromMask($renewalMask);
        $catalogSet   = CycleSet::fromMask($catalogMask);
        $meta['catalog_cycle_enabled'] = $catalogSet->contains($cycle);
        $meta['renewal_cycle_enabled'] = $renewalSet->contains($cycle);

        if (!$renewalSet->contains($cycle)) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'cycle_not_mapped',
                false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        // ── Profile & version availability ─────────────────────────────────
        $profile = isset($service['profile']) && is_array($service['profile']) ? $service['profile'] : null;
        $version = isset($service['profile_version']) && is_array($service['profile_version']) ? $service['profile_version'] : null;

        if (!empty($service['no_mapping']) || $profile === null) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'no_mapping', false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }
        if (!empty($profile['discontinued']) || !empty($service['plan_discontinued'])) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'plan_discontinued', false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }
        if (!empty($service['missing_baseline_version']) || $version === null) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'missing_baseline_version', false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        // ── FX / landed cost / candidate ───────────────────────────────────
        if (!empty($this->settings['fx_unavailable'])) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'fx_unavailable', false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        $fxRate              = (float) ($this->settings['fx_rate'] ?? 0.0);
        $fxBufferPct         = (float) ($profile['fx_buffer_pct'] ?? 2.00);
        $paymentBufferPct    = (float) ($this->settings['payment_buffer_pct'] ?? 2.00);
        $taxMode             = (string) ($this->settings['tax_registration_mode'] ?? TaxModeEngine::defaultMode());
        $modeSummary         = TaxModeEngine::isValid($taxMode)
            ? TaxModeEngine::summary($taxMode)
            : TaxModeEngine::summary(TaxModeEngine::defaultMode());
        $vendorTaxRatePct    = (float) ($this->settings['vendor_tax_rate_pct'] ?? 0.0);
        $vendorTaxRecoverable= array_key_exists('vendor_tax_recoverable', $this->settings)
            ? (bool) $this->settings['vendor_tax_recoverable']
            : (bool) $modeSummary['vendor_tax_recoverable'];
        $pricesIncludeOutput = array_key_exists('prices_include_output_tax', $this->settings)
            ? (bool) $this->settings['prices_include_output_tax']
            : (bool) $modeSummary['prices_include_output_tax'];
        $outputTaxRatePct    = (float) ($this->settings['output_tax_rate_pct'] ?? 0.0);

        if ($fxRate <= 0.0) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'fx_unavailable', false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        // Phase D — per-cycle SOURCE basis. The renewal cost basis must match the
        // catalog: price each cycle off ITS OWN source tier from the version's
        // per-period EUR vector (period_prices_json), so a quarterly renewal uses
        // the quarterly source, not a single base monthly. Falls back to
        // base_monthly_eur for legacy versions with no vector.
        $eurMonthly    = self::resolveCycleEurMonthly($version, $cycleMonths);

        // Pricing invariant: source price must be positive. Missing / zero source
        // means we cannot compute a valid candidate — fail closed (skip, do not
        // silently write a stale price).
        if ($eurMonthly <= 0.0) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'missing_source_price',
                false, false,
                null, null, null, null, null,
                null, null, null, null, null, null, null,
                $meta
            );
        }
        $landedMonthly = MarginCalculator::landedCostMonthly(
            $eurMonthly, $fxRate, $fxBufferPct, $paymentBufferPct,
            $vendorTaxRatePct, $vendorTaxRecoverable
        );
        $landedForCycle = $landedMonthly * $cycleMonths;

        // Phase B (§13) — whole-config margin. When a snapshot supplied the
        // selected options' EUR deltas, compute the landed cost + margin ratio of
        // the WHOLE configuration (base + options) against the resolved revenue,
        // and record whether it breaches the floor. This is the accurate signal
        // for config-driven undercharging. It is RECORDED only — the base
        // candidate/floor decision below is unchanged (driving repricing off the
        // whole config needs the candidate to back out config revenue, a separate
        // step), so there is no billing-math regression.
        if ($wholeConfigSelections !== [] && $resolvedRevenueTotal !== null && $resolvedRevenueTotal > 0.0) {
            $wholeLandedMonthly  = MarginCalculator::landedCostWithSelections(
                $eurMonthly, $wholeConfigSelections, $fxRate, $fxBufferPct,
                $paymentBufferPct, $vendorTaxRatePct, $vendorTaxRecoverable
            );
            $wholeLandedForCycle = $wholeLandedMonthly * $cycleMonths;
            $wholeNet            = MarginCalculator::netRevenueForCycle($resolvedRevenueTotal, $pricesIncludeOutput, $outputTaxRatePct);
            $wholeRatio          = MarginCalculator::currentMarginRatio($resolvedRevenueTotal, $wholeNet, $wholeLandedForCycle);
            $floorPct            = (float) ($profile['margin_floor_pct'] ?? 0.0);
            $meta['margin_basis']                  = 'whole_config';
            $meta['whole_config_landed_for_cycle'] = round($wholeLandedForCycle, 4);
            $meta['whole_config_margin_ratio']     = round($wholeRatio, 6);
            $meta['whole_config_below_floor']      = $wholeRatio < ($floorPct / 100.0);
        }

        // ── Per-cycle markup resolution (Phase A.5) ────────────────────────
        // Same JSON-decode pattern as Agent B's SyncEngine. Documented
        // contract: this logic MUST stay byte-identical between SyncEngine
        // and RenewalEngine. Future refactor: extract to MarkupResolver.
        $markupResolved = $this->resolveMarkup($mapping, $version, $cycle);
        $meta['markup_strategy_used'] = $markupResolved['strategy'];
        $meta['markup_value_used']    = $markupResolved['value'];
        $meta['markup_source']        = $markupResolved['source'];

        if ($markupResolved['as_total'] === true) {
            // 'fixed' override expressed as a cycle-total price (e.g. Annually
            // = 9200/yr). Bypass sellPriceForCycle's monthly×months multiplier
            // entirely — the value IS the total.
            $preRound = (float) $markupResolved['value'];
        } else {
            // Resolve the monthly fixed-sell price: prefer the override value
            // (per-cycle monthly), fall back to profile_version's stored
            // sell_price_local_monthly. Either way it's only consulted when
            // strategy === 'fixed'.
            $sellMonthly = null;
            if ($markupResolved['strategy'] === 'fixed') {
                if ($markupResolved['source'] === 'mapping_override') {
                    $sellMonthly = (float) $markupResolved['value'];
                } elseif (isset($version['sell_price_local_monthly'])) {
                    $sellMonthly = (float) $version['sell_price_local_monthly'];
                }
            }

            $preRound = MarginCalculator::sellPriceForCycle(
                $landedMonthly,
                $markupResolved['strategy'],
                (float) $markupResolved['value'],
                $sellMonthly,
                $cycleMonths
            );
        }

        $roundingMode = (string) ($mapping['rounding_mode'] ?? Rounding::MODE_EXACT_2_DECIMALS);
        $candidate    = Rounding::apply($preRound, $roundingMode);
        $meta['pre_round_price'] = round($preRound, 4);
        $meta['rounded_price']   = round($candidate, 4);
        $meta['rounding_mode']   = $roundingMode;

        // Scheduled-change override: forcedCandidate replaces the computed
        // value AFTER we've recorded what would have been computed (for
        // audit). The forced price still flows through applyWithGuards.
        if ($forcedCandidate !== null) {
            $meta['forced_candidate_source'] = 'scheduled_change';
            $meta['computed_candidate_pre_force'] = round($candidate, 4);
            $candidate = Rounding::apply($forcedCandidate, $roundingMode);
            $meta['rounded_price'] = round($candidate, 4);
        }

        // Pricing invariant: margin must produce a positive sell price.
        // Zero/negative means either landed cost was zero or markup was
        // misconfigured — fail closed rather than writing a nonsense price.
        if ($candidate <= 0.0) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $oldPrice, $cycle, $cycleMonths,
                'unknown', false, 'margin_zero_or_negative',
                false, false,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                null, null, null, null, null, null, null,
                $meta
            );
        }

        $netRevenue   = MarginCalculator::netRevenueForCycle($oldPrice, $pricesIncludeOutput, $outputTaxRatePct);
        $currentRatio = MarginCalculator::currentMarginRatio($oldPrice, $netRevenue, $landedForCycle);

        // ── Resolve policy ─────────────────────────────────────────────────
        $serviceId = (int) ($service['id'] ?? 0);
        $sp = $this->policyResolver->resolveForService($serviceId, $profile);

        $today = $now->format('Y-m-d H:i:s');

        // Manual override = automation BLOCKED. If expired, fall through.
        if ($sp['policy'] === 'manual') {
            $exp = $sp['manual_override_expires_at'];
            if ($exp === null || $today < $exp) {
                return $this->buildDecision(
                    $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                    'manual', false, 'manual_override_active',
                    false, false,
                    $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                    $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                    $pricesIncludeOutput, $outputTaxRatePct,
                    $netRevenue, $currentRatio,
                    $meta
                );
            }
            // Expired → fall through to the profile default.
            $sp['policy'] = (string) ($profile['default_policy'] ?? 'current_term');
            $sp['source'] = 'profile_default';
        }

        // Lifetime grandfather — always skip.
        if ($sp['policy'] === 'lifetime') {
            return $this->buildDecision(
                $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                'lifetime', false, 'lifetime_grandfather',
                false, false,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        // Frozen until — skip while in-window.
        if ($sp['policy'] === 'frozen_until') {
            $until = $sp['frozen_until'];
            if ($until !== null && $now->format('Y-m-d') < $until) {
                return $this->buildDecision(
                    $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                    'frozen_until', false, 'frozen_until',
                    false, false,
                    $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                    $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                    $pricesIncludeOutput, $outputTaxRatePct,
                    $netRevenue, $currentRatio,
                    $meta
                );
            }
            // Expired → fall through to default policy.
            $sp['policy'] = (string) ($profile['default_policy'] ?? 'current_term');
        }

        // Notice-window flag for current_term / margin_floor policies.
        $noticeDays      = (int) ($profile['notice_days_default'] ?? 30);
        $nextRenewalStr  = isset($service['nextduedate']) && $service['nextduedate'] !== '' ? (string) $service['nextduedate'] : null;
        $insideNoticeWin = false;
        if ($nextRenewalStr !== null) {
            try {
                $nextRenewal = new \DateTimeImmutable($nextRenewalStr);
                $windowStart = $nextRenewal->modify('-' . max(0, $noticeDays) . ' days');
                $insideNoticeWin = $now >= $windowStart;
            } catch (\Throwable $e) {
                // Bad date → treat as outside notice window.
                $insideNoticeWin = false;
            }
        }

        // margin_floor policy
        if ($sp['policy'] === 'margin_floor') {
            $floorPct = (float) $sp['margin_floor_pct'];

            if ($currentRatio !== null && $currentRatio >= ($floorPct / 100.0)) {
                return $this->buildDecision(
                    $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                    'margin_floor', false, 'margin_above_floor',
                    false, false,
                    $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                    $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                    $pricesIncludeOutput, $outputTaxRatePct,
                    $netRevenue, $currentRatio,
                    $meta
                );
            }

            $forcedMonthly = MarginCalculator::minimumSellMonthlyForFloor($landedMonthly, $floorPct);
            $forcedCycle   = Rounding::apply($forcedMonthly * $cycleMonths, $roundingMode);
            if ($sp['min_sell_price'] !== null) {
                $forcedCycle = max($forcedCycle, (float) $sp['min_sell_price']);
            }
            $meta['pre_round_price'] = round($forcedMonthly * $cycleMonths, 4);
            $meta['rounded_price']   = round($forcedCycle, 4);

            return $this->applyWithGuards(
                $service, $now, $oldPrice, $forcedCycle, $cycle, $cycleMonths,
                'margin_floor', /*requires_notice*/ true, $sp,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $profile, $meta
            );
        }

        // current_term — only act inside notice window.
        // Scheduled-change override bypasses the notice-window gate because
        // the admin already chose the effective date; ScheduledChangeProcessor
        // is responsible for honouring the schedule's effective_at clock.
        if ($sp['policy'] === 'current_term') {
            if ($forcedCandidate === null && !$insideNoticeWin) {
                return $this->buildDecision(
                    $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                    'current_term', false, 'within_current_term',
                    false, false,
                    $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                    $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                    $pricesIncludeOutput, $outputTaxRatePct,
                    $netRevenue, $currentRatio,
                    $meta
                );
            }
            return $this->applyWithGuards(
                $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                'current_term', /*requires_notice*/ true, $sp,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $profile, $meta
            );
        }

        // reprice_renewal — only act ON the renewal date (unless forced)
        if ($sp['policy'] === 'reprice_renewal') {
            if ($forcedCandidate === null
                && ($nextRenewalStr === null || $now->format('Y-m-d') < substr($nextRenewalStr, 0, 10))) {
                return $this->buildDecision(
                    $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                    'reprice_renewal', false, 'within_current_term',
                    false, false,
                    $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                    $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                    $pricesIncludeOutput, $outputTaxRatePct,
                    $netRevenue, $currentRatio,
                    $meta
                );
            }
            $changePct = $oldPrice > 0 ? abs($candidate - $oldPrice) / $oldPrice : 1.0;
            return $this->applyWithGuards(
                $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
                'reprice_renewal', /*requires_notice*/ $changePct > 0.10, $sp,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $profile, $meta
            );
        }

        // Fell off the end — flag for admin attention.
        return $this->buildDecision(
            $service, $now, $oldPrice, $candidate, $cycle, $cycleMonths,
            (string) $sp['policy'], false, 'missing_policy',
            false, true,
            $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
            $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
            $pricesIncludeOutput, $outputTaxRatePct,
            $netRevenue, $currentRatio,
            $meta
        );
    }

    /**
     * Resolve the markup strategy + value for the given cycle.
     *
     * Resolution order (first match wins):
     *   1. `mapping.markup_overrides_json[cycle]` — per-cycle override on the
     *      mapping row. JSON shape:
     *      {
     *        "Monthly":   {"strategy": "cost_plus_pct",    "value": 30.0},
     *        "Annually":  {"strategy": "fixed",            "value": 9200, "as_total": true}
     *      }
     *      The optional `as_total` flag means "this is the cycle-total price,
     *      not a monthly value × cycle months". Useful for fixed Annually
     *      prices like ₹9200/yr that shouldn't be 9200 × 12.
     *   2. `version.markup_strategy` + `version.markup_value` — profile-version
     *      default.
     *   3. Hard fallback: cost_plus_pct, value 0 (i.e. landed cost passed
     *      through with zero markup).
     *
     * DOCUMENTED CONTRACT: this method MUST stay byte-identical with Agent B's
     * equivalent in SyncEngine. If you change one you change the other in the
     * same commit. Future: extract to lib/MarkupResolver.php so the two
     * engines share an implementation.
     *
     * @param array<string,mixed> $mapping
     * @param array<string,mixed> $version
     * @return array{strategy:string, value:float, source:string, as_total:bool}
     */
    /**
     * Per-cycle SOURCE basis in EUR/month for a renewal, from the version's
     * period_prices_json vector. Exact cycle months win; else the longest period
     * whose months ≤ target (same rule as SyncEngine — 24/36 → 12-mo, a missing
     * 3-mo → 1-mo). Legacy versions with no vector fall back to base_monthly_eur.
     *
     * @param array<string,mixed> $version a profile_version DB row
     */
    private static function resolveCycleEurMonthly(array $version, int $cycleMonths): float
    {
        $vector = [];
        $raw = $version['period_prices_json'] ?? null;
        if (is_array($raw)) {
            $vector = $raw;
        } elseif (is_string($raw) && $raw !== '') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                $vector = $decoded;
            }
        }

        $normalized = [];
        foreach ($vector as $m => $rate) {
            $mi = (int) $m;
            if ($mi > 0) {
                $normalized[$mi] = (float) $rate;
            }
        }
        if ($normalized === []) {
            return (float) ($version['base_monthly_eur'] ?? 0.0);
        }
        if (isset($normalized[$cycleMonths])) {
            return $normalized[$cycleMonths];
        }
        $bestMonths = null;
        foreach ($normalized as $m => $rate) {
            if ($m <= $cycleMonths && ($bestMonths === null || $m > $bestMonths)) {
                $bestMonths = $m;
            }
        }
        if ($bestMonths === null) {
            $bestMonths = min(array_keys($normalized));
        }
        return $normalized[$bestMonths];
    }

    private function resolveMarkup(array $mapping, array $version, string $cycle): array
    {
        $overridesJson = isset($mapping['markup_overrides_json']) && $mapping['markup_overrides_json'] !== null
            ? (string) $mapping['markup_overrides_json']
            : '';

        if ($overridesJson !== '') {
            $decoded = json_decode($overridesJson, true);
            if (is_array($decoded) && isset($decoded[$cycle]) && is_array($decoded[$cycle])) {
                $entry    = $decoded[$cycle];
                $strategy = isset($entry['strategy']) ? (string) $entry['strategy'] : 'cost_plus_pct';
                $value    = isset($entry['value']) ? (float) $entry['value'] : 0.0;
                $asTotal  = !empty($entry['as_total']);
                if (in_array($strategy, ['cost_plus_pct', 'cost_plus_amount', 'fixed'], true)) {
                    return [
                        'strategy' => $strategy,
                        'value'    => $value,
                        'source'   => 'mapping_override',
                        'as_total' => $asTotal,
                    ];
                }
            }
        }

        $strategy = isset($version['markup_strategy']) ? (string) $version['markup_strategy'] : 'cost_plus_pct';
        $value    = isset($version['markup_value']) ? (float) $version['markup_value'] : 0.0;
        if (!in_array($strategy, ['cost_plus_pct', 'cost_plus_amount', 'fixed'], true)) {
            $strategy = 'cost_plus_pct';
            $value    = 0.0;
        }
        return [
            'strategy' => $strategy,
            'value'    => $value,
            'source'   => 'profile_version',
            'as_total' => false,
        ];
    }

    /**
     * Apply-path guards (phase gate, auto-decrease, max-increase ceiling,
     * large-increase approval). Phase A: this method NEVER actually writes
     * `tblhosting`. It returns a decision payload that DecisionLog will seal.
     *
     * Phase A.5 reminder: ScheduledChangeProcessor reaches this method via
     * decideForScheduledChange() → decideInternal(). One write path, one
     * guard surface. Do NOT duplicate this logic elsewhere.
     *
     * @param array<string, mixed> $service
     * @param array<string, mixed> $sp
     * @param array<string, mixed> $profile
     * @param array<string, mixed> $meta
     *
     * @return array<string, mixed>
     */
    private function applyWithGuards(
        array $service,
        \DateTimeImmutable $now,
        float $oldPrice,
        float $newPrice,
        string $cycle,
        int $cycleMonths,
        string $policy,
        bool $requiresNotice,
        array $sp,
        float $landedForCycle,
        float $landedMonthly,
        float $eurMonthly,
        float $fxRate,
        float $fxBufferPct,
        string $taxMode,
        float $vendorTaxRatePct,
        bool $vendorTaxRecoverable,
        bool $pricesIncludeOutput,
        float $outputTaxRatePct,
        float $netRevenue,
        ?float $currentRatio,
        array $profile,
        array $meta
    ): array {
        // Safety invariant: only positive prices reach the write path.
        // Zero/negative should have been caught as margin_zero_or_negative
        // or missing_source_price in decideInternal before we get here.
        $serviceId = (int) ($service['id'] ?? 0);
        if ($newPrice <= 0.0) {
            $msg = "RenewalEngine: refusing zero/negative price {$newPrice} for service {$serviceId} cycle {$cycle}";
            if (function_exists('logActivity')) {
                \logActivity($msg);
            }
            return $this->buildDecision(
                $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
                $policy, false, 'price_invariant_violation',
                $requiresNotice, false,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        $allowDecrease = (bool) $sp['allow_auto_decrease'];

        // Decrease-without-permission guard.
        if ($newPrice < $oldPrice && !$allowDecrease) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
                $policy, false, 'auto_decrease_disallowed',
                $requiresNotice, false,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        // Hard ceiling — must be force-approved.
        $maxIncrease = (float) ($profile['max_increase_pct'] ?? 25.00);
        if ($oldPrice > 0 && (($newPrice - $oldPrice) / $oldPrice) > ($maxIncrease / 100.0)) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
                $policy, false, 'awaiting_force_approval_max_increase_exceeded',
                $requiresNotice, true,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        // Soft threshold — needs an admin approval.
        $largeThreshold = (float) ($profile['large_increase_threshold_pct'] ?? 10.00);
        if ($oldPrice > 0 && (($newPrice - $oldPrice) / $oldPrice) > ($largeThreshold / 100.0)) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
                $policy, false, 'awaiting_admin_approval',
                $requiresNotice, true,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        // Notice window — Phase A defers wiring of actual sends, but we already
        // know "needs notice" → skip with that reason. Agent C wires real
        // Notifier later.
        if ($requiresNotice) {
            return $this->buildDecision(
                $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
                $policy, false, 'notice_scheduled',
                true, false,
                $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
                $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
                $pricesIncludeOutput, $outputTaxRatePct,
                $netRevenue, $currentRatio,
                $meta
            );
        }

        // All clear. Build a decision marked applied=true… BUT the phase gate
        // in buildDecision() will force it back to applied=false when
        // repricing_phase === 'observe'. That's the Phase A safety net.
        return $this->buildDecision(
            $service, $now, $oldPrice, $newPrice, $cycle, $cycleMonths,
            $policy, true, null,
            $requiresNotice, false,
            $landedForCycle, $landedMonthly, $eurMonthly, $fxRate, $fxBufferPct,
            $taxMode, $vendorTaxRatePct, $vendorTaxRecoverable,
            $pricesIncludeOutput, $outputTaxRatePct,
            $netRevenue, $currentRatio,
            $meta
        );
    }

    /**
     * Compose the immutable decision payload. Enforces the phase gate at the
     * tail: in 'observe' phase, every applied=true becomes applied=false with
     * skip_reason='phase_observe_only'. In 'opt_in', un-opted services are
     * forced to skip with 'phase_opt_in_required'.
     *
     * Phase A.5: the `metadata_json` sidecar collects per-cycle audit fields
     * (markup_strategy_used, markup_value_used, pre_round_price, rounded_price,
     * rounding_mode, cycle_recurring_column, cycle_setup_fee_column,
     * catalog_cycle_enabled, renewal_cycle_enabled) that don't have dedicated
     * columns yet. Stored as a JSON string on the decision row. Agent A's
     * migrateTo3() adds the column.
     *
     * @param array<string, mixed> $service
     * @param array<string, mixed> $meta
     * @return array<string, mixed>
     */
    private function buildDecision(
        array $service,
        \DateTimeImmutable $now,
        float $oldPrice,
        float $newPrice,
        string $cycle,
        ?int $cycleMonths,
        string $policy,
        bool $applied,
        ?string $skipReason,
        bool $requiresNotice,
        bool $requiresAdminApproval,
        ?float $landedForCycle      = null,
        ?float $landedMonthly       = null,
        ?float $eurMonthly          = null,
        ?float $fxRate              = null,
        ?float $fxBufferPct         = null,
        ?string $taxMode            = null,
        ?float $vendorTaxRatePct    = null,
        ?bool $vendorTaxRecoverable = null,
        ?bool $pricesIncludeOutput  = null,
        ?float $outputTaxRatePct    = null,
        ?float $netRevenue          = null,
        ?float $currentRatio        = null,
        array $meta                 = []
    ): array {
        $phase = (string) ($this->settings['repricing_phase'] ?? 'observe');

        // Phase A safety net: observe-only forces applied=false.
        if ($applied && $phase === 'observe') {
            $applied    = false;
            $skipReason = 'phase_observe_only';
        }
        // opt_in: un-opted services skip.
        if ($applied && $phase === 'opt_in' && empty($service['explicitly_opted_in'])) {
            $applied    = false;
            $skipReason = 'phase_opt_in_required';
        }

        $taxModeFinal = $taxMode !== null
            ? $taxMode
            : ((string) ($this->settings['tax_registration_mode'] ?? TaxModeEngine::defaultMode()));

        // vendor_tax_amount, output_tax_amount for the cycle (rough — caller
        // can recompute from the snapshotted rates).
        $vendorTaxAmount = null;
        if ($eurMonthly !== null && $fxRate !== null && $vendorTaxRatePct !== null && $cycleMonths !== null) {
            $vendorTaxAmount = round($eurMonthly * $fxRate * ($vendorTaxRatePct / 100.0) * $cycleMonths, 4);
        }
        $outputTaxAmount = null;
        if ($pricesIncludeOutput !== null && $outputTaxRatePct !== null && $outputTaxRatePct > 0 && $newPrice > 0) {
            if ($pricesIncludeOutput) {
                $rate = $outputTaxRatePct / 100.0;
                $outputTaxAmount = round($newPrice - ($newPrice / (1.0 + $rate)), 4);
            } else {
                $outputTaxAmount = round($newPrice * ($outputTaxRatePct / 100.0), 4);
            }
        }

        $marginAmount = null;
        $marginPct    = null;
        if ($netRevenue !== null && $landedForCycle !== null) {
            $marginAmount = round($netRevenue - $landedForCycle, 4);
            if ($netRevenue > 0) {
                $marginPct = round(($marginAmount / $netRevenue) * 100.0, 3);
            }
        }

        $profileId        = isset($service['profile']['id']) ? (int) $service['profile']['id'] : null;
        $profileVersionId = isset($service['profile_version']['id']) ? (int) $service['profile_version']['id'] : null;

        // Encode metadata sidecar (null when empty so we don't write `[]` on
        // every row). Decision-row tests can decode and assert per-key.
        $metadataJson = null;
        if (!empty($meta)) {
            $encoded = json_encode($meta, JSON_UNESCAPED_SLASHES);
            if ($encoded !== false) {
                $metadataJson = $encoded;
            }
        }

        return [
            'service_id'                       => (int) ($service['id'] ?? 0),
            'profile_id'                       => $profileId,
            'profile_version_id'               => $profileVersionId,
            'cron_run_id'                      => $this->cronRunId,
            'decided_at'                       => $now->format('Y-m-d H:i:s'),
            'effective_at'                     => isset($service['nextduedate']) && $service['nextduedate'] !== ''
                                                      ? (string) $service['nextduedate']
                                                      : null,
            'billing_cycle'                    => $cycle,
            'cycle_months'                     => $cycleMonths,
            'currency'                         => (string) ($this->settings['currency_iso'] ?? 'INR'),
            'old_price'                        => $oldPrice,
            'proposed_new_price'               => $newPrice,
            'vendor_cost_eur_monthly'          => $eurMonthly,
            'vendor_cost_local_monthly'        => $landedMonthly,
            'vendor_cost_local_for_cycle'      => $landedForCycle,
            'fx_rate'                          => $fxRate,
            'fx_buffer_pct'                    => $fxBufferPct,
            'tax_mode_snapshot'                => $taxModeFinal,
            'vendor_tax_rate_pct'              => $vendorTaxRatePct,
            'vendor_tax_amount'                => $vendorTaxAmount,
            'vendor_tax_recoverable'           => $vendorTaxRecoverable,
            'output_tax_rate_pct'              => $outputTaxRatePct,
            'output_tax_amount'                => $outputTaxAmount,
            'prices_include_output_tax'        => $pricesIncludeOutput,
            'sell_price_gross_for_cycle'       => $newPrice,
            'sell_price_net_revenue_for_cycle' => $netRevenue,
            'margin_amount_for_cycle'          => $marginAmount,
            'margin_pct'                       => $marginPct,
            'policy_used'                      => $policy,
            'applied'                          => $applied,
            'applied_via'                      => $applied ? 'cron' : null,
            'skip_reason'                      => $applied ? null : $skipReason,
            'requires_notice'                  => $requiresNotice,
            'requires_admin_approval'          => $requiresAdminApproval,
            'notice_id'                        => null,
            'parent_decision_id'               => null,
            'metadata_json'                    => $metadataJson,
        ];
    }

    /**
     * Returns the cron-run UUID this engine instance is sealing decisions
     * under. Useful for tests + admin "decisions from this pass" filters.
     */
    public function cronRunId(): string
    {
        return $this->cronRunId;
    }

    /**
     * @return string|null  skip_reason if early-exit applies, else null.
     */
    private function earlySkipReason(array $service): ?string
    {
        $status = (string) ($service['status'] ?? 'Active');
        if ($status === 'Cancelled')  return 'service_cancelled';
        if ($status === 'Terminated') return 'service_terminated';

        if (!empty($service['unpaid_renewal_invoice'])) return 'unpaid_renewal_invoice';
        if (!empty($service['pending_upgrade']))        return 'pending_upgrade';
        if (!empty($service['promo_applied']))          return 'promo_applied';

        $subId = (string) ($service['subscriptionid'] ?? '');
        if ($subId !== '') return 'subscription_id_present';

        if (!empty($service['on_demand_renewal_in_flight'])) return 'on_demand_renewal_in_flight';
        if (!empty($service['manual_edit_detected']))         return 'manual_edit_detected';

        $recurring = (float) ($service['service_amount'] ?? $service['recurringamount'] ?? 0.0); // canonical row key (not a raw column)
        if ($recurring <= 0.0) return 'recurring_amount_invalid';

        // Suspended is nuanced: blocking only if there's an unpaid invoice
        // (which would already have returned above). Pure-suspended-but-billable
        // is evaluated.
        if ($status === 'Suspended' && !empty($service['suspended_blocking'])) {
            return 'service_suspended_blocking';
        }

        return null;
    }

    /**
     * RFC 4122 v4. Same algorithm as Lock; copied to keep classes independent.
     */
    private static function uuidV4(): string
    {
        try {
            $bytes = random_bytes(16);
        } catch (\Throwable $e) {
            $bytes = '';
            for ($i = 0; $i < 16; $i++) {
                $bytes .= chr(mt_rand(0, 255));
            }
        }
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
