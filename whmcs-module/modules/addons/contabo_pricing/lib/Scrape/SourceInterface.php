<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

interface SourceInterface
{
    public function id(): string;

    /**
     * @param array<string,mixed> $opts per-call overrides (e.g. 'format')
     * @throws SourceException
     */
    public function fetchFamilyPage(string $url, array $opts = []): FetchResult;

    /** @return array{ok:bool, latency_ms:int, message:string, cost_micro:int} */
    public function testConnection(): array;

    /** Prior success probability in [0,1] used for auto ranking before real stats exist. */
    public function priorSuccessRate(): float;

    /** Expected cost of one page fetch, micro-USD. */
    public function priceMicroPerPage(): int;

    /** True when the source must never run unattended (excluded from scheduled runs). */
    public function manualOnly(): bool;
}
