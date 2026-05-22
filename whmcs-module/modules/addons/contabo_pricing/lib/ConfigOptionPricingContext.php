<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Phase A.6.2 — immutable pricing inputs for {@see ConfigurableOptionsSyncer}.
 *
 * Each configurable-option value carries a Contabo `monthly_eur_delta` (the
 * marginal EUR cost of choosing it over the default). To price that delta in
 * the customer's currency we reuse the SAME cost basis the base plan used:
 * `landedMultiplier = version.finalMonthly / version.baseMonthlyEur` is the
 * EUR → local landed-cost-per-month factor (it already folds in FX, the FX
 * markup buffer and GST exactly as the base price did). The profit markup is
 * then applied per cycle by {@see MarginCalculator::sellPriceForCycle}, so an
 * option delta is priced identically to the base it sits on.
 *
 * PHP 7.4 polyglot: no readonly, no constructor promotion.
 */
final class ConfigOptionPricingContext
{
    /** @var int WHMCS currency id; must be the install base currency for v1. */
    public $currencyId;

    /** @var float EUR → local landed-cost-per-month multiplier. */
    public $landedMultiplier;

    /** @var string 'cost_plus_pct' | 'cost_plus_amount' | 'fixed'. */
    public $markupStrategy;

    /** @var float Percent or amount, per the strategy. */
    public $markupValue;

    /** @var string A Rounding::supportedModes() value. */
    public $roundingMode;

    public function __construct(
        int $currencyId,
        float $landedMultiplier,
        string $markupStrategy,
        float $markupValue,
        string $roundingMode
    ) {
        $this->currencyId       = $currencyId;
        $this->landedMultiplier = $landedMultiplier;
        $this->markupStrategy   = $markupStrategy;
        $this->markupValue      = $markupValue;
        $this->roundingMode     = $roundingMode;
    }

    /**
     * Derive the context from a profile version. The landed multiplier is
     * `finalMonthly / baseMonthlyEur`; when the base EUR is zero or missing the
     * multiplier is 0 (every delta then prices to 0 — safe, never negative).
     */
    public static function fromVersion(
        ProfileVersionInput $version,
        int $currencyId,
        string $markupStrategy,
        float $markupValue,
        string $roundingMode
    ): self {
        $baseEur = (float) $version->baseMonthlyEur;
        $multiplier = $baseEur > 0.0 ? ((float) $version->finalMonthly) / $baseEur : 0.0;

        return new self($currencyId, $multiplier, $markupStrategy, $markupValue, $roundingMode);
    }
}
