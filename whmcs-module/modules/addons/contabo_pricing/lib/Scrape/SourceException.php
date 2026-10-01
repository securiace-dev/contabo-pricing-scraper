<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** A provider call failed (transport, non-2xx, malformed or empty payload). */
final class SourceException extends \RuntimeException
{
    /** @var int HTTP status when known, 0 otherwise */
    private $httpStatus;

    public function __construct(string $message, int $httpStatus = 0, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
        $this->httpStatus = $httpStatus;
    }

    public function httpStatus(): int
    {
        return $this->httpStatus;
    }
}
