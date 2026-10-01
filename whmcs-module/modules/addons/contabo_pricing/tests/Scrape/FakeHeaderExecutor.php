<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\HeaderAwareExecutor;

/** Queue-backed HeaderAwareExecutor that records every call. */
final class FakeHeaderExecutor implements HeaderAwareExecutor
{
    /** @var list<array{status:int, body:string, errno:int, error:string, headers:array<string,string>}> */
    public $queue = [];

    /** @var list<array{method:string, url:string, headers:array<int,string>, body:?string, timeout:int}> */
    public $calls = [];

    /** @param array<string,string> $headers */
    public function push(int $status, string $body, array $headers = [], int $errno = 0, string $error = ''): self
    {
        $this->queue[] = ['status' => $status, 'body' => $body, 'errno' => $errno, 'error' => $error, 'headers' => $headers];
        return $this;
    }

    public function executeWithHeaders(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $this->calls[] = ['method' => $method, 'url' => $url, 'headers' => $headers, 'body' => $body, 'timeout' => $timeoutSec];
        $r = array_shift($this->queue);
        if ($r === null) {
            throw new \LogicException('FakeHeaderExecutor queue exhausted');
        }
        return $r;
    }
}
