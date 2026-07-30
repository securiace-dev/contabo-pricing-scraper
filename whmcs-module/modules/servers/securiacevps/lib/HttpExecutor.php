<?php
declare(strict_types=1);

namespace SecuriAceVps;

/**
 * Abstraction over the HTTP transport used by ContaboApiClient / ContaboAuth.
 * Mirrors the addon's RequestExecutor seam so tests can script responses
 * without touching curl or the network.
 *
 * Implementations MUST NOT throw on non-2xx status codes — those are reported
 * via the returned tuple so the caller can map them (retry, refresh, throw)
 * with the context it has.
 */
interface HttpExecutor
{
    /**
     * Execute a single HTTP request.
     *
     * @param string             $method  GET|POST|PUT|PATCH|DELETE
     * @param string             $url     Fully-qualified URL.
     * @param array<int,string>  $headers Pre-formatted header lines ("Name: value").
     * @param string|null        $body    Already-encoded body, or null.
     * @param int                $timeoutSec
     * @return array{0:int, 1:string, 2:int, 3:string} {status, body, curlErrno, curlError}
     */
    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array;
}
