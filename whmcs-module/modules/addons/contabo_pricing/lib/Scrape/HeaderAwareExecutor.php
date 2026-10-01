<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * HTTP transport that also returns response headers (needed for provider
 * cost headers such as X-Treg-Cost-Micro). Separate from RequestExecutor so
 * the existing 4-tuple contract of RequestExecutor stays untouched.
 */
interface HeaderAwareExecutor
{
    /**
     * @param array<int,string> $headers pre-formatted "Name: value" lines
     * @return array{status:int, body:string, errno:int, error:string, headers:array<string,string>}
     *         headers: lower-cased name => value (last occurrence wins)
     */
    public function executeWithHeaders(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array;
}
