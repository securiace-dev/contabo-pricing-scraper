<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Abstraction over the actual HTTP transport used by ApiClient. Allows tests to
 * stub out curl-driven I/O while production code keeps the same calling shape.
 *
 * Implementations MUST NOT throw on non-2xx status codes — those are reported
 * via the returned tuple so ApiClient can map them into RuntimeException(s)
 * with the path/method context it has.
 */
interface RequestExecutor
{
    /**
     * Execute a single HTTP request.
     *
     * @param 'GET'|'POST'|string $method
     * @param string              $url       Fully-qualified URL.
     * @param array<int, string>  $headers   Pre-formatted header lines ("Name: value").
     * @param string|null         $body      Already JSON-encoded body, or null.
     * @param int                 $timeoutSec
     * @return array{0: int, 1: string, 2: int, 3: string} {status, body, curlErrno, curlError}
     */
    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array;
}
