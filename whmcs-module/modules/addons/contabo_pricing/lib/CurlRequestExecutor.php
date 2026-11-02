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
 *
 * The response body is accumulated in a write callback with RETURNTRANSFER
 * off, so libcurl does not keep a second full buffer. Bodies over 4 MiB
 * come back empty with a non-zero errno.
 */
final class CurlRequestExecutor implements RequestExecutor, HeaderAwareExecutor
{
    public const MAX_REDIRECTS = 5;

    private const USER_AGENT = 'whmcs-contabo-pricing/0.1';

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
     * tests can assert FOLLOWLOCATION without opening a socket. The body
     * capture (RETURNTRANSFER off plus a write callback) is layered on top in
     * applyTransport().
     *
     * @return array<int, mixed>
     */
    public function transportOptions(int $timeoutSec): array
    {
        $opts = [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $timeoutSec,
            CURLOPT_CONNECTTIMEOUT => $timeoutSec,
            CURLOPT_USERAGENT      => self::USER_AGENT,
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

        $cap = new ResponseBodyCap();
        $this->applyTransport($ch, $method, $headers, $timeoutSec, $cap, null);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $done = $this->finish($ch, $cap);
        return [$done['code'], $done['body'], $done['errno'], $done['error']];
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
        $cap = new ResponseBodyCap();
        $this->applyTransport($ch, $method, $headers, $timeoutSec, $cap, static function ($_ch, string $line) use (&$respHeaders): int {
            $len = strlen($line);
            $trim = trim($line);
            if (stripos($trim, 'HTTP/') === 0) {
                $respHeaders = [];
            } elseif (($pos = strpos($trim, ':')) !== false) {
                $respHeaders[strtolower(trim(substr($trim, 0, $pos)))] = trim(substr($trim, $pos + 1));
            }
            return $len;
        });
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $done = $this->finish($ch, $cap);
        return [
            'status'  => $done['code'],
            'body'    => $done['body'],
            'errno'   => $done['errno'],
            'error'   => $done['error'],
            'headers' => $respHeaders,
        ];
    }

    /**
     * @param resource|\CurlHandle $ch
     * @param callable|null $onHeader
     */
    private function applyTransport($ch, string $method, array $headers, int $timeoutSec, ResponseBodyCap $cap, $onHeader): void
    {
        $opts = [
            CURLOPT_CUSTOMREQUEST  => $method,
            // Off on purpose: a write callback plus RETURNTRANSFER keeps a second buffer.
            CURLOPT_RETURNTRANSFER => false,
            CURLOPT_HTTPHEADER     => $headers,
            // Received bytes only. Content-Length is not read and is not trusted.
            CURLOPT_WRITEFUNCTION  => static function ($_ch, string $data) use ($cap): int {
                return $cap->write($data);
            },
        ] + $this->transportOptions($timeoutSec);
        if ($onHeader !== null) {
            $opts[CURLOPT_HEADERFUNCTION] = $onHeader;
        }
        curl_setopt_array($ch, $opts);
    }

    /**
     * curl_exec's return is success or failure only. The body lives in $cap.
     *
     * @param resource|\CurlHandle $ch
     * @return array{code:int, body:string, errno:int, error:string}
     */
    private function finish($ch, ResponseBodyCap $cap): array
    {
        $ok = curl_exec($ch);
        $errno = (int) curl_errno($ch);
        $err = (string) curl_error($ch);
        $code = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if ($cap->overflowed()) {
            return [
                'code'  => $code,
                'body'  => '',
                'errno' => $errno !== 0 ? $errno : ResponseBodyCap::ERRNO,
                'error' => ResponseBodyCap::ERROR,
            ];
        }
        if ($ok !== true) {
            return [
                'code'  => $code,
                'body'  => '',
                'errno' => $errno !== 0 ? $errno : -1,
                'error' => $err !== '' ? $err : 'curl exec failed',
            ];
        }
        return [
            'code'  => $code,
            'body'  => $cap->body(),
            'errno' => $errno,
            'error' => $err,
        ];
    }
}
