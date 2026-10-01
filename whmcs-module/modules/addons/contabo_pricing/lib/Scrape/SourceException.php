<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** A provider call failed (transport, non-2xx, malformed or empty payload). */
final class SourceException extends \RuntimeException
{
    /** @var int HTTP status when known, 0 otherwise */
    private $httpStatus;
    /** @var string|null provider-side run id of a paid, asynchronous run (so it can be recovered, never relaunched) */
    private $providerRunId = null;
    /** @var int spend already incurred by the failed call, micro-USD */
    private $costMicro = 0;

    public function __construct(string $message, int $httpStatus = 0, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
        $this->httpStatus = $httpStatus;
    }

    public function httpStatus(): int
    {
        return $this->httpStatus;
    }

    /** Fluent: attach the provider run id and the cost the run has already accrued. */
    public function withProviderRun(string $runId, int $costMicro = 0): self
    {
        $this->providerRunId = $runId;
        $this->costMicro = max(0, $costMicro);
        return $this;
    }

    public function providerRunId(): ?string
    {
        return $this->providerRunId;
    }

    public function costMicro(): int
    {
        return $this->costMicro;
    }
}
