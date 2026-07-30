<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use SecuriAceVps\HttpExecutor;

/**
 * Scripted HttpExecutor: queue responses (optionally keyed by "METHOD url-substring"
 * matchers), record every call for assertions. Unmatched requests fall back to
 * the default response.
 */
final class FakeHttpExecutor implements HttpExecutor
{
    /** @var list<array{method:string,url:string,headers:array<int,string>,body:?string,timeout:int}> */
    public array $calls = [];

    /** @var list<array{match:?string, response:array{0:int,1:string,2:int,3:string}}> */
    private array $queue = [];

    /** @var list<array{match:string, response:array{0:int,1:string,2:int,3:string}}> persistent responders */
    private array $stubs = [];

    /** @var array{0:int,1:string,2:int,3:string} */
    private array $default = [200, '{}', 0, ''];

    /**
     * Queue a response. $match is "METHOD substring" (e.g. "POST /v1/compute/instances")
     * or null to match the next request regardless.
     *
     * @param array<string,mixed>|string $body decoded array (json-encoded for you) or raw string
     */
    public function queue(?string $match, int $status, $body = [], int $errno = 0, string $error = ''): void
    {
        $raw = is_string($body) ? $body : (string) json_encode($body);
        $this->queue[] = ['match' => $match, 'response' => [$status, $raw, $errno, $error]];
    }

    /**
     * Persistent responder (not consumed): every request matching "METHOD substring"
     * gets this response unless a queued one-shot matched first.
     *
     * @param array<string,mixed>|string $body
     */
    public function stub(string $match, int $status, $body = [], int $errno = 0, string $error = ''): void
    {
        $raw = is_string($body) ? $body : (string) json_encode($body);
        $this->stubs[] = ['match' => $match, 'response' => [$status, $raw, $errno, $error]];
    }

    /** Convenience: OAuth token endpoint always succeeds. */
    public function stubToken(): void
    {
        $this->stub('POST auth.contabo.com', 200, ['access_token' => 'test-token', 'expires_in' => 300]);
    }

    /** @param array<string,mixed>|string $body */
    public function setDefault(int $status, $body = [], int $errno = 0, string $error = ''): void
    {
        $raw = is_string($body) ? $body : (string) json_encode($body);
        $this->default = [$status, $raw, $errno, $error];
    }

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $this->calls[] = [
            'method'  => $method,
            'url'     => $url,
            'headers' => $headers,
            'body'    => $body,
            'timeout' => $timeoutSec,
        ];

        foreach ($this->queue as $idx => $entry) {
            $match = $entry['match'];
            if ($match === null || $this->matches($match, $method, $url)) {
                unset($this->queue[$idx]);
                $this->queue = array_values($this->queue);
                return $entry['response'];
            }
        }
        foreach ($this->stubs as $entry) {
            if ($this->matches($entry['match'], $method, $url)) {
                return $entry['response'];
            }
        }
        return $this->default;
    }

    /** Calls whose "METHOD url" contains the needle. @return list<array<string,mixed>> */
    public function callsMatching(string $needle): array
    {
        $out = [];
        foreach ($this->calls as $call) {
            if (strpos($call['method'] . ' ' . $call['url'], $needle) !== false) {
                $out[] = $call;
            }
        }
        return $out;
    }

    private function matches(string $match, string $method, string $url): bool
    {
        $space = strpos($match, ' ');
        if ($space === false) {
            return strpos($url, $match) !== false;
        }
        $wantMethod = substr($match, 0, $space);
        $wantUrl    = substr($match, $space + 1);
        return strcasecmp($wantMethod, $method) === 0 && strpos($url, $wantUrl) !== false;
    }
}
