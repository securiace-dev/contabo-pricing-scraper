<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * A safe, classified pricing failure that must prevent a monetary write.
 */
final class PricingInvariantViolation extends \RuntimeException
{
    /** @var string */
    private $reason;

    public function __construct(string $reason, string $message)
    {
        parent::__construct($message);
        $this->reason = $reason;
    }

    public function reason(): string
    {
        return $this->reason;
    }
}
