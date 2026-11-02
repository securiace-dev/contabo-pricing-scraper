<?php
declare(strict_types=1);

namespace SecuriAceVps;

require_once __DIR__ . '/ResponseBodyCap.php';

/**
 * Production HttpExecutor — issues calls through ext-curl. Kept thin: it does
 * not interpret status codes or decode JSON; ContaboApiClient owns that
 * mapping. SSL verification is always on.
 *
 * The response body is accumulated in a write callback with RETURNTRANSFER
 * off. Bodies over 4 MiB come back empty with a non-zero errno.
 */
final class CurlHttpExecutor implements HttpExecutor
{
    private const USER_AGENT = 'whmcs-contabo-vps/1.0';

    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            return [0, '', -1, 'curl init failed'];
        }

        $cap = new ResponseBodyCap();
        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST  => $method,
            // Off on purpose: a write callback plus RETURNTRANSFER keeps a second buffer.
            CURLOPT_RETURNTRANSFER => false,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_TIMEOUT        => $timeoutSec,
            CURLOPT_CONNECTTIMEOUT => $timeoutSec,
            CURLOPT_USERAGENT      => self::USER_AGENT,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            // Received bytes only. Content-Length is not read and is not trusted.
            CURLOPT_WRITEFUNCTION  => static function ($_ch, string $data) use ($cap): int {
                return $cap->write($data);
            },
        ]);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $ok = curl_exec($ch);
        $errno = (int) curl_errno($ch);
        $err = (string) curl_error($ch);
        $code = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if ($cap->overflowed()) {
            return [
                $code,
                '',
                $errno !== 0 ? $errno : ResponseBodyCap::ERRNO,
                ResponseBodyCap::ERROR,
            ];
        }
        if ($ok !== true) {
            return [$code, '', $errno !== 0 ? $errno : -1, $err !== '' ? $err : 'curl exec failed'];
        }
        return [$code, $cap->body(), $errno, $err];
    }
}
