<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** Outcome of one provider fetch of one plan page. Immutable by convention. */
final class FetchResult
{
    /** @var string provider id (SourceInterface::id()) */
    public $provider;
    /** @var string|null upstream that actually served it (e.g. treg's _treg.served_by) */
    public $servedBy;
    /** @var string|null raw page HTML */
    public $html;
    /** @var array<string,mixed>|null provider-extracted JSON (S2 input) */
    public $json;
    /** @var int cost in micro-USD */
    public $costMicro;
    /** @var int */
    public $latencyMs;
    /** @var int */
    public $httpStatus;
    /** @var string|null e.g. 'provider-json' when only json is meaningful */
    public $strategyHint;
    /** @var string|null URL after provider-reported redirects (AlterLab final_url) */
    public $finalUrl;

    /** @param array<string,mixed>|null $json */
    public function __construct(
        string $provider,
        ?string $servedBy,
        ?string $html,
        ?array $json,
        int $costMicro,
        int $latencyMs,
        int $httpStatus,
        ?string $strategyHint = null,
        ?string $finalUrl = null
    ) {
        $this->provider = $provider;
        $this->servedBy = $servedBy;
        $this->html = $html;
        $this->json = $json;
        $this->costMicro = $costMicro;
        $this->latencyMs = $latencyMs;
        $this->httpStatus = $httpStatus;
        $this->strategyHint = $strategyHint;
        $this->finalUrl = $finalUrl;
    }
}
