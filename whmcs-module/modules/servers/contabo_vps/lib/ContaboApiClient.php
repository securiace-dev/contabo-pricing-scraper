<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Thin curl wrapper around the Contabo REST API. Mirrors the addon's
 * CurlRequestExecutor pattern (no Guzzle, to avoid a dependency clash with
 * whatever WHMCS bundles). SSL verification is always on.
 *
 * Resilience:
 *   - 401 → force-refresh the token once, then retry.
 *   - 429 → linear backoff, up to MAX_RETRIES.
 *   - 5xx → exponential backoff, up to MAX_RETRIES.
 * Every request carries an x-request-id (UUID v4) for Contabo-side tracing.
 */
class ContaboApiClient
{
    private const BASE_URL    = 'https://api.contabo.com';
    private const MAX_RETRIES = 3;

    /** @var ContaboAuth */ private $auth;

    public function __construct(ContaboAuth $auth)
    {
        $this->auth = $auth;
    }

    /** @return array<string,mixed> */
    public function get(string $path): array
    {
        return $this->request('GET', $path, null);
    }

    /** @param array<string,mixed> $body @return array<string,mixed> */
    public function post(string $path, array $body): array
    {
        return $this->request('POST', $path, $body);
    }

    /** @param array<string,mixed> $body @return array<string,mixed> */
    public function patch(string $path, array $body): array
    {
        return $this->request('PATCH', $path, $body);
    }

    /**
     * @param array<string,mixed>|null $body
     * @return array<string,mixed>
     */
    private function request(string $method, string $path, ?array $body, int $attempt = 0): array
    {
        $url     = self::BASE_URL . $path;
        $token   = $this->auth->getToken();
        $headers = [
            'Authorization: Bearer ' . $token,
            'x-request-id: ' . $this->generateRequestId(),
            'Content-Type: application/json',
            'Accept: application/json',
        ];

        $ch   = curl_init($url);
        $opts = [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_TIMEOUT        => 30,
            CURLOPT_HTTPHEADER     => $headers,
        ];

        if ($method === 'POST') {
            $opts[CURLOPT_POST]       = true;
            $opts[CURLOPT_POSTFIELDS] = $body !== null ? (string) json_encode($body) : '{}';
        } elseif ($method === 'PATCH') {
            $opts[CURLOPT_CUSTOMREQUEST] = 'PATCH';
            $opts[CURLOPT_POSTFIELDS]    = $body !== null ? (string) json_encode($body) : '{}';
        }

        curl_setopt_array($ch, $opts);
        $raw  = curl_exec($ch);
        $code = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err  = curl_error($ch);
        curl_close($ch);

        if ($raw === false || $err !== '') {
            throw new ContaboProvisioningException('API curl error: ' . $err);
        }

        if ($code === 401 && $attempt === 0) {
            $this->auth->forceRefresh();
            return $this->request($method, $path, $body, 1);
        }
        if ($code === 429 && $attempt < self::MAX_RETRIES) {
            sleep(1 + $attempt);
            return $this->request($method, $path, $body, $attempt + 1);
        }
        if ($code >= 500 && $attempt < self::MAX_RETRIES) {
            sleep((int) (2 ** $attempt));
            return $this->request($method, $path, $body, $attempt + 1);
        }

        $data = json_decode((string) $raw, true);
        if ($code >= 400) {
            $msg = is_array($data) ? (string) ($data['message'] ?? json_encode($data)) : (string) $raw;
            throw new ContaboProvisioningException('API error (HTTP ' . $code . '): ' . substr($msg, 0, 300));
        }

        return is_array($data) ? $data : [];
    }

    /** UUID v4 without an extension dependency. */
    private function generateRequestId(): string
    {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
