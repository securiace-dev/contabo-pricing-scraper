<?php
declare(strict_types=1);

namespace SecuriAceVps;

/**
 * Thin client for the Contabo REST API over the HttpExecutor seam (no Guzzle,
 * to avoid a dependency clash with whatever WHMCS bundles). SSL verification
 * is always on (enforced by CurlHttpExecutor).
 *
 * Resilience:
 *   - 401 → force-refresh the token once (does NOT consume the retry budget),
 *     then replay the request.
 *   - 429 → linear backoff, up to MAX_RETRIES.
 *   - 5xx → exponential backoff, up to MAX_RETRIES.
 *   - Total backoff sleep is capped at MAX_TOTAL_SLEEP_SEC so a flapping API
 *     can never stall a WHMCS admin/client page load for long.
 * Every request carries an x-request-id (UUID v4) for Contabo-side tracing.
 */
class ContaboApiClient
{
    private const BASE_URL            = 'https://api.contabo.com';
    private const MAX_RETRIES         = 3;
    private const MAX_TOTAL_SLEEP_SEC = 6;
    private const DEFAULT_TIMEOUT_SEC = 30;

    /** @var ContaboAuth */ private $auth;
    /** @var HttpExecutor */ private $executor;
    /** @var callable */ private $sleeper;
    /** @var int */ private $timeoutSec = self::DEFAULT_TIMEOUT_SEC;

    public function __construct(ContaboAuth $auth, ?HttpExecutor $executor = null, ?callable $sleeper = null)
    {
        $this->auth     = $auth;
        $this->executor = $executor !== null ? $executor : new CurlHttpExecutor();
        $this->sleeper  = $sleeper !== null ? $sleeper : static function (int $seconds): void {
            sleep($seconds);
        };
    }

    /**
     * Per-call timeout override. View paths (admin tab / client area render)
     * use a short timeout so a slow API degrades the page instead of hanging
     * it; provisioning keeps the default.
     */
    public function setTimeout(int $seconds): void
    {
        $this->timeoutSec = max(1, $seconds);
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

    /**
     * Submit a mutation using a stable request identity. Contabo documents the
     * x-request-id as a tracing identity, not a universal idempotency
     * guarantee, so callers must still reconcile ambiguous outcomes.
     *
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     */
    public function postWithIdentity(string $path, array $body, string $requestIdentity): array
    {
        return $this->request('POST', $path, $body, $requestIdentity);
    }

    /**
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     */
    public function putWithIdentity(string $path, array $body, string $requestIdentity): array
    {
        return $this->request('PUT', $path, $body, $requestIdentity);
    }

    /** @param array<string,mixed> $body @return array<string,mixed> */
    public function put(string $path, array $body): array
    {
        return $this->request('PUT', $path, $body);
    }

    /** @param array<string,mixed> $body @return array<string,mixed> */
    public function patch(string $path, array $body): array
    {
        return $this->request('PATCH', $path, $body);
    }

    /** @return array<string,mixed> */
    public function delete(string $path): array
    {
        return $this->request('DELETE', $path, null);
    }

    /**
     * @param array<string,mixed>|null $body
     * @return array<string,mixed>
     */
    private function request(string $method, string $path, ?array $body, ?string $requestIdentity = null): array
    {
        $url     = self::BASE_URL . $path;
        $encoded = null;
        if ($body !== null) {
            // An empty PHP array must serialise as a JSON OBJECT ({}), not [] —
            // Contabo's endpoints validate the body as an object.
            $encoded = $body === [] ? '{}' : (string) json_encode($body);
        } elseif ($method === 'POST' || $method === 'PUT' || $method === 'PATCH') {
            $encoded = '{}';
        }

        $attempt   = 0;
        $refreshed = false;
        $slept     = 0;

        while (true) {
            $requestId = $requestIdentity !== null && $requestIdentity !== ''
                ? substr($requestIdentity, 0, 64)
                : $this->generateRequestId();
            $headers = [
                'Authorization: Bearer ' . $this->auth->getToken(),
                'x-request-id: ' . $requestId,
                'Content-Type: application/json',
                'Accept: application/json',
            ];

            list($code, $raw, $errno, $err) = $this->executor->execute($method, $url, $headers, $encoded, $this->timeoutSec);

            if ($errno !== 0) {
                throw new ContaboProvisioningException(
                    'API transport error: ' . ($err !== '' ? $err : ('curl errno ' . $errno)),
                    'provider_transport_error',
                    'transient',
                    in_array($method, ['POST', 'PUT', 'PATCH', 'DELETE'], true)
                );
            }

            if ($code === 401 && !$refreshed) {
                // Stale/expired token — refresh once and replay. A second 401
                // means the credentials themselves are bad; fall through to the
                // error mapping below on the replay.
                $this->auth->forceRefresh();
                $refreshed = true;
                continue;
            }

            $retryable = ($code === 429 || $code >= 500);
            if ($retryable && $attempt < self::MAX_RETRIES) {
                $delay = $code === 429 ? (1 + $attempt) : (2 ** $attempt);
                $delay = (int) min($delay, max(0, self::MAX_TOTAL_SLEEP_SEC - $slept));
                if ($delay > 0) {
                    call_user_func($this->sleeper, $delay);
                    $slept += $delay;
                }
                $attempt++;
                continue;
            }

            $data = json_decode($raw, true);
            if ($code >= 400) {
                $msg = is_array($data) ? (string) ($data['message'] ?? json_encode($data)) : $raw;
                $retry = ($code === 429 || $code >= 500) ? 'transient' : 'terminal';
                throw new ContaboProvisioningException(
                    'API error (HTTP ' . $code . '): ' . substr($msg, 0, 300),
                    'provider_http_' . $code,
                    $retry,
                    $code >= 500 && in_array($method, ['POST', 'PUT', 'PATCH', 'DELETE'], true)
                );
            }

            return is_array($data) ? $data : [];
        }
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
