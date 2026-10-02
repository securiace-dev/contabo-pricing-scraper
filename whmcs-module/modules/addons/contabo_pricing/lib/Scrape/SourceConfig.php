<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/** Immutable provider config handed to an adapter (apiKey is plaintext, in memory only). */
final class SourceConfig
{
    /** @var string */ public $id;
    /** @var string */ public $baseUrl;
    /** @var string */ public $apiKey;
    /** @var array<string,mixed> */ public $options;

    /** @param array<string,mixed> $options */
    public function __construct(string $id, string $baseUrl, string $apiKey, array $options = [])
    {
        $this->id = $id;
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->apiKey = $apiKey;
        $this->options = $options;
    }
}
