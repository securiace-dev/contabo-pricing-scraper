<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * HTTP client for the contabo-pricing API server. Uses curl directly to avoid
 * a Guzzle dependency clash with WHMCS's own bundled Guzzle.
 */
class ApiClient
{
    /** @var Settings */         private $settings;
    /** @var int */              private $timeoutSec;
    /** @var RequestExecutor */  private $executor;

    public function __construct(
        Settings $settings,
        int $timeoutSec = 8,
        ?RequestExecutor $executor = null
    ) {
        $this->settings   = $settings;
        $this->timeoutSec = $timeoutSec;
        $this->executor   = $executor ?? new CurlRequestExecutor();
    }

    /** @return array<string, mixed> */
    public function meta(): array
    {
        return $this->get('/meta');
    }

    /** @return list<array<string, mixed>> */
    public function plans(?string $family = null): array
    {
        $path = '/plans';
        if ($family !== null && $family !== '') {
            $path .= '?family=' . rawurlencode($family);
        }
        $res = $this->get($path);
        if (!is_array($res)) return [];
        return array_values($res);
    }

    /** @return array<string, mixed> */
    public function plan(string $slug): array
    {
        return $this->get('/plans/' . rawurlencode($slug));
    }

    /** @return array<string, mixed> */
    public function configurator(string $slug): array
    {
        return $this->get('/plans/' . rawurlencode($slug) . '/configurator');
    }

    /** @return array<string, mixed> */
    public function catalog(): array
    {
        return $this->get('/catalog');
    }

    /** @return array<string, mixed> */
    public function fx(): array
    {
        return $this->get('/fx');
    }

    /**
     * Calculate a configured price server-side. Mirrors the report calculator
     * exactly so previews stay consistent.
     *
     * @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public function quote(array $body): array
    {
        return $this->post('/quote', $body);
    }

    /**
     * Trigger an async scrape on the API server. Bearer token required.
     *
     * @return array{job_id: string, status: string}
     */
    public function refresh(): array
    {
        $res = $this->post('/refresh', [], true);
        return [
            'job_id' => (string) ($res['job_id'] ?? ''),
            'status' => (string) ($res['status'] ?? 'unknown'),
        ];
    }

    /** @return array<string, mixed> */
    public function job(string $id): array
    {
        return $this->get('/jobs/' . rawurlencode($id));
    }

    // ── internals ────────────────────────────────────────────────────────────

    /** @return array<string, mixed> */
    private function get(string $path): array
    {
        return $this->request('GET', $path, null);
    }

    /**
     * @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private function post(string $path, array $body, bool $requireAuth = false): array
    {
        if ($requireAuth && $this->settings->apiToken === '') {
            throw new \RuntimeException('API token not configured in addon Settings.');
        }
        return $this->request('POST', $path, $body);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function request(string $method, string $path, ?array $body): array
    {
        $scheme = strtolower((string)(parse_url($this->settings->apiBaseUrl, PHP_URL_SCHEME) ?? ''));
        if ($scheme !== 'http' && $scheme !== 'https') {
            throw new \RuntimeException('API base URL must use http or https scheme.');
        }
        $url = $this->settings->apiBaseUrl . $path;

        $headers = ['Accept: application/json'];
        if ($body !== null) {
            $headers[] = 'Content-Type: application/json';
        }
        if ($this->settings->apiToken !== '') {
            $headers[] = 'Authorization: Bearer ' . $this->settings->apiToken;
        }

        $bodyStr = $body !== null ? (string) json_encode($body, JSON_UNESCAPED_SLASHES) : null;
        [$code, $resp, $errno, $err] = $this->executor->execute($method, $url, $headers, $bodyStr, $this->timeoutSec);

        if ($errno !== 0) {
            throw new \RuntimeException("API {$method} {$path} failed: {$err} (errno {$errno})");
        }
        if ($code >= 500) {
            throw new \RuntimeException("API {$method} {$path} returned HTTP {$code}: " . substr($resp, 0, 240));
        }
        if ($code === 401 || $code === 403) {
            throw new \RuntimeException("API {$method} {$path} unauthorized — check bearer token");
        }
        if ($code === 404) {
            throw new \RuntimeException("API {$method} {$path} not found");
        }
        if ($code >= 400) {
            throw new \RuntimeException("API {$method} {$path} client error HTTP {$code}: " . substr($resp, 0, 240));
        }

        $decoded = json_decode($resp, true);
        if (!is_array($decoded)) {
            throw new \RuntimeException("API {$method} {$path} returned non-JSON body");
        }
        return $decoded;
    }
}
