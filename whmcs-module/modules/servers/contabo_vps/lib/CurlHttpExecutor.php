<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Production HttpExecutor — issues calls through ext-curl. Kept thin: it does
 * not interpret status codes or decode JSON; ContaboApiClient owns that
 * mapping. SSL verification is always on.
 */
final class CurlHttpExecutor implements HttpExecutor
{
    public function execute(string $method, string $url, array $headers, ?string $body, int $timeoutSec): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            return [0, '', -1, 'curl init failed'];
        }

        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_TIMEOUT        => $timeoutSec,
            CURLOPT_CONNECTTIMEOUT => $timeoutSec,
            CURLOPT_USERAGENT      => 'whmcs-contabo-vps/1.0',
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
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
}
