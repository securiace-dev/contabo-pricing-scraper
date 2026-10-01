<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** Outcome of PlanExtractor::extract(). `dom_probe` results carry no plans (never importable alone). */
final class ExtractionResult
{
    public const STRATEGY_SAPPER = 'sapper';
    public const STRATEGY_PROVIDER_JSON = 'provider_json';
    public const STRATEGY_DOM_PROBE = 'dom_probe';
    public const STRATEGY_NONE = 'none';

    /** @var list<array<string,mixed>> */
    public $plans;
    /** @var string one of the STRATEGY_* constants */
    public $strategy;
    /** @var bool whether the HTML contained a `__SAPPER__=` assignment */
    public $sapperPresent;
    /** @var list<string> */
    public $warnings;
    /** @var array{title:?string, monthly_eur:?float}|null */
    public $probe;

    /**
     * @param list<array<string,mixed>> $plans
     * @param list<string> $warnings
     * @param array{title:?string, monthly_eur:?float}|null $probe
     */
    public function __construct(array $plans, string $strategy, bool $sapperPresent, array $warnings, ?array $probe = null)
    {
        $this->plans = $plans;
        $this->strategy = $strategy;
        $this->sapperPresent = $sapperPresent;
        $this->warnings = $warnings;
        $this->probe = $probe;
    }

    /** True only for strategies whose plans may be imported. */
    public function importable(): bool
    {
        return $this->plans !== []
            && ($this->strategy === self::STRATEGY_SAPPER || $this->strategy === self::STRATEGY_PROVIDER_JSON);
    }
}
