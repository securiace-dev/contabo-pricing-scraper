<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Shared plumbing for HTTP-backed provider adapters: transport call with
 * latency timing, error mapping that never leaks the API key, JSON decoding,
 * and an injectable sleep for polling.
 */
abstract class AbstractSource implements SourceInterface
{
    /** @var SourceConfig */
    protected $config;
    /** @var HeaderAwareExecutor */
    protected $executor;
    /** @var int */
    protected $timeoutSec;
    /** @var callable(int):void */
    protected $sleep;

    /** @param callable(int):void|null $sleep receives whole seconds */
    public function __construct(
        SourceConfig $config,
        HeaderAwareExecutor $executor,
        int $timeoutSec = 60,
        ?callable $sleep = null
    ) {
        $this->config = $config;
        $this->executor = $executor;
        $this->timeoutSec = $timeoutSec;
        $this->sleep = $sleep ?? static function (int $seconds): void {
            sleep($seconds);
        };
    }

    public function id(): string
    {
        return $this->config->id;
    }

    public function manualOnly(): bool
    {
        return false;
    }

    public function testConnection(): array
    {
        $t0 = microtime(true);
        try {
            $r = $this->fetchFamilyPage('https://example.com/');
            $ok = ($r->html !== null && $r->html !== '') || $r->json !== null;
            return [
                'ok' => $ok,
                'latency_ms' => $r->latencyMs,
                'message' => $ok ? 'OK (HTTP ' . $r->httpStatus . ')' : 'Empty response',
                'cost_micro' => $r->costMicro,
            ];
        } catch (SourceException $e) {
            return [
                'ok' => false,
                'latency_ms' => (int) round((microtime(true) - $t0) * 1000),
                'message' => $e->getMessage(),
                'cost_micro' => 0,
            ];
        }
    }

    /** Base URL from config, else the adapter default. Only https (or loopback) is accepted. */
    protected function base(string $default): string
    {
        $base = $this->config->baseUrl !== '' ? $this->config->baseUrl : $default;
        $base = rtrim($base, '/');
        if (preg_match('#^(https://|http://(127\.0\.0\.1|localhost)(:|/|$))#i', $base) !== 1) {
            throw new SourceException('Provider base URL must use https://');
        }
        return $base;
    }

    /**
     * One HTTP call. Throws SourceException on transport failure or non-2xx.
     *
     * @param array<int,string> $headers
     * @return array{status:int, body:string, headers:array<string,string>, latency_ms:int}
     */
    protected function call(string $method, string $url, array $headers, ?string $body): array
    {
        $t0 = microtime(true);
        $r = $this->executor->executeWithHeaders($method, $url, $headers, $body, $this->timeoutSec);
        $latency = (int) round((microtime(true) - $t0) * 1000);

        if ($r['errno'] !== 0) {
            throw new SourceException($this->id() . ' transport error: ' . $this->scrub($r['error']));
        }
        if ($r['status'] < 200 || $r['status'] >= 300) {
            throw new SourceException(
                $this->id() . ' HTTP ' . $r['status'] . ': ' . $this->scrub(substr($r['body'], 0, 200)),
                $r['status']
            );
        }
        return [
            'status' => $r['status'],
            'body' => $r['body'],
            'headers' => $r['headers'],
            'latency_ms' => $latency,
        ];
    }

    /** @return array<string,mixed> */
    protected function decodeJson(string $body): array
    {
        $decoded = json_decode($body, true);
        if (!is_array($decoded)) {
            throw new SourceException($this->id() . ' returned a non-JSON response');
        }
        return $decoded;
    }

    /** @param array<string,mixed> $payload */
    protected function encodeJson(array $payload): string
    {
        $s = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if (!is_string($s)) {
            throw new SourceException($this->id() . ' could not encode the request body');
        }
        return $s;
    }

    /** Removes the API key from any text that may end up in logs or the UI. */
    protected function scrub(string $text): string
    {
        if ($this->config->apiKey !== '') {
            $text = str_replace($this->config->apiKey, '***', $text);
        }
        return $text;
    }

    /**
     * First non-empty string found at any of the given dotted paths.
     *
     * @param array<string,mixed> $data
     * @param list<string> $paths
     */
    protected function firstString(array $data, array $paths): ?string
    {
        foreach ($paths as $path) {
            $cur = $data;
            foreach (explode('.', $path) as $seg) {
                if (!is_array($cur) || !array_key_exists($seg, $cur)) {
                    $cur = null;
                    break;
                }
                $cur = $cur[$seg];
            }
            if (is_string($cur) && $cur !== '') {
                return $cur;
            }
        }
        return null;
    }
}
