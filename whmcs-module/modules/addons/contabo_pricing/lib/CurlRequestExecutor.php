<?php
declare(strict_types=1);

namespace ContaboPricing;

use ContaboPricing\Scrape\HeaderAwareExecutor;

/**
 * Production RequestExecutor — issues calls through ext-curl. Kept thin: it
 * does not interpret status codes or decode JSON; callers own that mapping.
 *
 * HTTPS redirects are followed by default (Frankfurter and other providers
 * have moved hosts with a 301). Callers that attach secrets (Proposal Studio
 * AI Bearer tokens) MUST construct with $followRedirects = false so a 302
 * cannot forward the key. Redirects are HTTPS-only so a 301 cannot downgrade
 * the channel.
 */
final class CurlRequestExecutor implements RequestExecutor, HeaderAwareExecutor
{
    public const MAX_REDIRECTS = 5;

    /** @var bool */
    private $followRedirects;

    public function __construct(bool $followRedirects = true)
    {
        $this->followRedirects = $followRedirects;
    }

    public function followsRedirects(): bool
    {
        return $this->followRedirects;
    }

    /**
     * Curl options that implement the redirect + TLS policy. Split out so
     * tests can assert FOLLOWLOCATION without opening a socket.
     *
     * @return array<int, mixed>
     */
    public function transportOptions(int $timeoutSec): array
    {
        $opts = [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $timeoutSec,
            CURLOPT_CONNECTTIMEOUT => $timeoutSec,
            CURLOPT_USERAGENT      => 'whmcs-contabo-pricing/0.1',
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_FOLLOWLOCATION => $this->followRedirects,
        ];
        if ($this->followRedirects) {
            $opts[CURLOPT_MAXREDIRS] = self::MAX_REDIRECTS;
            if (defined('CURLPROTO_HTTPS')) {
                $opts[CURLOPT_REDIR_PROTOCOLS] = CURLPROTO_HTTPS;
            }
        }
        return $opts;
    }

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            return [0, '', -1, 'curl init failed'];
        }

        curl_setopt_array($ch, $this->transportOptions($timeoutSec) + [
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_HTTPHEADER    => $headers,
        ]);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $resp  = curl_exec($ch);
        $errno = curl_errno($ch);
        $err   = (string) curl_error($ch);
        $code  = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        $bodyStr = $resp === false ? '' : (string) $resp;
        return [$code, $bodyStr, $errno, $err];
    }

    /**
     * Same transport as execute() but also captures response headers
     * (lower-cased names; last occurrence wins; reset on each new status line
     * so only the final response's headers survive).
     *
     * @return array{status:int, body:string, errno:int, error:string, headers:array<string,string>}
     */
    public function executeWithHeaders(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            return ['status' => 0, 'body' => '', 'errno' => -1, 'error' => 'curl init failed', 'headers' => []];
        }

        /** @var array<string,string> $respHeaders */
        $respHeaders = [];
        curl_setopt_array($ch, $this->transportOptions($timeoutSec) + [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_HEADERFUNCTION => static function ($_ch, string $line) use (&$respHeaders): int {
                $len = strlen($line);
                $trim = trim($line);
                if (stripos($trim, 'HTTP/') === 0) {
                    $respHeaders = [];
                } elseif (($pos = strpos($trim, ':')) !== false) {
                    $respHeaders[strtolower(trim(substr($trim, 0, $pos)))] = trim(substr($trim, $pos + 1));
                }
                return $len;
            },
        ]);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $resp  = curl_exec($ch);
        $errno = curl_errno($ch);
        $err   = (string) curl_error($ch);
        $code  = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        return [
            'status'  => $code,
            'body'    => $resp === false ? '' : (string) $resp,
            'errno'   => $errno,
            'error'   => $err,
            'headers' => $respHeaders,
        ];
    }
}
