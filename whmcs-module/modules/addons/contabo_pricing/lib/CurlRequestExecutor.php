<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Production RequestExecutor — issues calls through ext-curl. Kept thin: it
 * does not interpret status codes or decode JSON; ApiClient owns that mapping.
 */
final class CurlRequestExecutor implements RequestExecutor
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
            CURLOPT_USERAGENT      => 'whmcs-contabo-pricing/0.1',
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
