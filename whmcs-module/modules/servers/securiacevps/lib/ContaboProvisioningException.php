<?php
declare(strict_types=1);

namespace SecuriAceVps;

/**
 * Raised for any provisioning failure (auth, API, mapping, misconfiguration).
 * The main module file catches it and returns its message to WHMCS as the
 * human-readable error string.
 */
class ContaboProvisioningException extends \RuntimeException
{
    /** @var string */
    private $safeCode;
    /** @var string */
    private $retryClassification;
    /** @var bool */
    private $ambiguousOutcome;

    public function __construct(
        string $message,
        string $safeCode = 'provisioning_error',
        string $retryClassification = 'terminal',
        bool $ambiguousOutcome = false
    ) {
        parent::__construct($message);
        $this->safeCode = $safeCode;
        $this->retryClassification = $retryClassification;
        $this->ambiguousOutcome = $ambiguousOutcome;
    }

    public function safeCode(): string
    {
        return $this->safeCode;
    }

    public function retryClassification(): string
    {
        return $this->retryClassification;
    }

    public function hasAmbiguousOutcome(): bool
    {
        return $this->ambiguousOutcome;
    }
}
