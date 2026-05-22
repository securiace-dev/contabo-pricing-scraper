<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Phase A.6.2 — configurable-options syncer (OBSERVE / preview).
 *
 * Given a profile's normalised dimension specs (the {@see DimensionParser}
 * output carried on a profile version's `specsSnapshot`), this produces the
 * full set of WHMCS configurable options that WOULD be created for the
 * profile's product — group → options → sub-options → per-cycle pricing —
 * WITHOUT writing to WHMCS. It is the read-only half of A.6; A.6.3 adds the
 * apply path. All writes are funnelled through {@see WhmcsConfigOptionsAdapter}
 * (constructed in dry-run for observe), the sole tblproductconfig + tblpricing
 * write chokepoint, and every step is recorded in {@see OptionAuditLog}.
 *
 * Pricing — each value's `monthly_eur_delta` is converted to the local landed
 * cost via {@see ConfigOptionPricingContext::landedMultiplier} and then to a
 * per-cycle sell price via {@see MarginCalculator::sellPriceForCycle}, so an
 * option delta is priced exactly like the base plan it sits on.
 *
 * Binding A.5.2 amendments enforced here:
 *   - #1 negative-delta clamp: a cheaper-than-default value (negative EUR
 *     delta) is clamped to 0 for v1, so we never emit negative tblpricing rows
 *     (WHMCS handles them, but the order form hides $0/negative labels and
 *     downgrades become credits — see PHASE_A6_PREFLIGHT_CONFIGOPTIONS.md).
 *   - #10 base-currency only: pricing for a non-base currency is skipped by the
 *     adapter (action 'skipped', never a stale row); the syncer surfaces it.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ConfigurableOptionsSyncer
{
    /** WHMCS billing cycle → months, in tblpricing column order. */
    private const CYCLE_MONTHS = [
        'monthly'      => 1,
        'quarterly'    => 3,
        'semiannually' => 6,
        'annually'     => 12,
        'biennially'   => 24,
        'triennially'  => 36,
    ];

    /** @var WhmcsConfigOptionsAdapter */
    private $adapter;

    /** @var OptionAuditLog */
    private $audit;

    public function __construct(WhmcsConfigOptionsAdapter $adapter, OptionAuditLog $audit)
    {
        $this->adapter = $adapter;
        $this->audit   = $audit;
    }

    /**
     * Build the preview for one profile.
     *
     * @param list<array<string,mixed>> $specs DimensionParser specs (each with
     *        dimension_key, optiontype, values[], optional default_value_key).
     * @param int|null $productId when set, the group is also previewed as
     *        linked to this WHMCS product.
     * @return array{
     *     profile_id:int, dry_run:bool, currency_id:int, group:array<string,mixed>,
     *     options:list<array<string,mixed>>, skipped:list<array<string,mixed>>,
     *     audit_count:int, totals:array{options:int,values:int,skipped:int}
     * }
     */
    public function observe(
        int $profileId,
        string $groupName,
        array $specs,
        ConfigOptionPricingContext $ctx,
        ?int $productId = null
    ): array {
        $auditCount = 0;
        $skipped    = [];

        $group = $this->adapter->upsertGroup(
            $groupName,
            'Contabo configurable options (profile #' . $profileId . ')'
        );
        $auditCount += $this->audit->observe(
            $profileId,
            null,
            'tblproductconfiggroups',
            $group['payload'],
            'group ' . $group['action']
        ) > 0 ? 1 : 0;

        $groupId = (int) ($group['id'] ?? 0); // null in dry-run; real id in apply mode
        if ($productId !== null) {
            $this->adapter->linkGroupToProduct($groupId, $productId);
        }

        $options = [];
        $valueCount = 0;

        foreach ($specs as $spec) {
            $dimKey     = (string) ($spec['dimension_key'] ?? '');
            $optionType = (int) ($spec['optiontype'] ?? OptionTypeMapper::TYPE_DROPDOWN);
            $values     = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];

            if ($dimKey === '' || $values === []) {
                $skipped[] = ['dimension_key' => $dimKey, 'reason' => 'empty_spec'];
                continue;
            }

            $isQuantity = $optionType === OptionTypeMapper::TYPE_QUANTITY;
            $option = $this->adapter->upsertOption(
                $groupId,
                $dimKey,
                $optionType,
                $isQuantity ? 0 : null,                 // qtyMin
                $isQuantity ? count($values) : null,    // qtyMax (provisional; refined in apply)
                0
            );
            $auditCount += $this->audit->observe(
                $profileId,
                $dimKey,
                'tblproductconfigoptions',
                $option['payload'],
                'option ' . $option['action']
            ) > 0 ? 1 : 0;

            $optionId   = (int) ($option['id'] ?? 0);
            $valuesPrev = [];

            foreach ($values as $i => $value) {
                $label      = (string) ($value['label'] ?? ($value['value_key'] ?? ('value ' . $i)));
                $sortOrder  = (int) ($value['sortorder'] ?? $i);
                $sub = $this->adapter->upsertSubOption($optionId, $label, $sortOrder, false);

                $cyclePrices = $this->priceCycles((float) ($value['monthly_eur_delta'] ?? 0.0), $ctx);
                $setupFees   = $this->priceSetup((float) ($value['setup_eur_delta'] ?? 0.0), $ctx);

                $subId  = (int) ($sub['id'] ?? 0);
                $pricing = $this->adapter->upsertConfigOptionPricing(
                    $subId,
                    $ctx->currencyId,
                    $cyclePrices,
                    $setupFees
                );

                if (($pricing['action'] ?? '') === 'skipped') {
                    $skipped[] = [
                        'dimension_key' => $dimKey,
                        'value'         => $label,
                        'reason'        => (string) ($pricing['skip_reason'] ?? 'pricing_skipped'),
                    ];
                }

                $auditCount += $this->audit->observe(
                    $profileId,
                    $dimKey,
                    'tblproductconfigoptionssub',
                    [
                        'label'        => $label,
                        'value_key'    => (string) ($value['value_key'] ?? ''),
                        'is_default'   => (bool) ($value['is_default'] ?? false),
                        'monthly_eur_delta' => (float) ($value['monthly_eur_delta'] ?? 0.0),
                        'cycle_prices' => $cyclePrices,
                        'setup_fees'   => $setupFees,
                    ],
                    'suboption ' . $sub['action']
                ) > 0 ? 1 : 0;

                $valuesPrev[] = [
                    'label'        => $label,
                    'value_key'    => (string) ($value['value_key'] ?? ''),
                    'is_default'   => (bool) ($value['is_default'] ?? false),
                    'cycle_prices' => $cyclePrices,
                    'setup_fees'   => $setupFees,
                    'sub'          => $sub,
                    'pricing'      => $pricing,
                ];
                $valueCount++;
            }

            $options[] = [
                'dimension_key' => $dimKey,
                'optiontype'    => $optionType,
                'is_quantity'   => $isQuantity,
                'option'        => $option,
                'values'        => $valuesPrev,
            ];
        }

        return [
            'profile_id'  => $profileId,
            'dry_run'     => $this->adapter->isDryRun(),
            'currency_id' => $ctx->currencyId,
            'group'       => $group,
            'options'     => $options,
            'skipped'     => $skipped,
            'audit_count' => $auditCount,
            'totals'      => [
                'options' => count($options),
                'values'  => $valueCount,
                'skipped' => count($skipped),
            ],
        ];
    }

    /**
     * Per-cycle sell price for one option value's monthly EUR delta.
     *
     * The EUR delta is clamped to >= 0 (amendment #1), converted to local
     * landed monthly via the context multiplier, then run through the same
     * MarginCalculator path as the base plan. A zero (or clamped) delta prices
     * to 0.00 in every cycle — exactly what a default / no-cost value should be.
     *
     * @return array<string,float> keyed by cycle name
     */
    private function priceCycles(float $monthlyEurDelta, ConfigOptionPricingContext $ctx): array
    {
        $eur = max(0.0, $monthlyEurDelta);
        $landedMonthly = $eur * $ctx->landedMultiplier;

        $prices = [];
        foreach (self::CYCLE_MONTHS as $cycle => $months) {
            // A zero (or negative-clamped) marginal cost is FREE — never apply
            // the markup to it. This matters for cost_plus_amount, where a flat
            // amount would otherwise be charged on a default/no-cost value; the
            // flat amount belongs to the base product, not to each option value.
            if ($landedMonthly <= 0.0) {
                $prices[$cycle] = 0.0;
                continue;
            }
            $sell = MarginCalculator::sellPriceForCycle(
                $landedMonthly,
                $ctx->markupStrategy,
                $ctx->markupValue,
                null,
                $months
            );
            $prices[$cycle] = Rounding::apply($sell, $ctx->roundingMode);
        }

        return $prices;
    }

    /**
     * One-time setup fee per cycle. Setup deltas are rare on Contabo options;
     * when present we pass the landed cost through (clamped >= 0, rounded) with
     * no recurring markup, and replicate it across the cycle columns WHMCS
     * stores. Returns an empty array when there is no setup delta, so the
     * adapter writes no setup columns.
     *
     * @return array<string,float>
     */
    private function priceSetup(float $setupEurDelta, ConfigOptionPricingContext $ctx): array
    {
        $eur = max(0.0, $setupEurDelta);
        if ($eur <= 0.0) {
            return [];
        }

        $setupLocal = Rounding::apply($eur * $ctx->landedMultiplier, $ctx->roundingMode);

        $fees = [];
        foreach (array_keys(self::CYCLE_MONTHS) as $cycle) {
            $fees[$cycle] = $setupLocal;
        }

        return $fees;
    }
}
