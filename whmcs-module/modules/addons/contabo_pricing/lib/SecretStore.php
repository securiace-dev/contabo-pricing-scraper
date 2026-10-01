<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Seal / open helper for secrets stored in addon tables (provider API keys).
 *
 * Wraps WHMCS's global encrypt()/decrypt() helpers and marks sealed values
 * with the same "ENC:" prefix Settings uses for the bearer token. Plaintext
 * is never logged.
 *
 * PHP 7.4 compatible.
 */
final class SecretStore
{
    private const PREFIX = 'ENC:';

    private function __construct()
    {
    }

    public static function seal(string $plain): string
    {
        return self::PREFIX . encrypt($plain);
    }

    /**
     * Returns the plaintext. A value without the ENC: prefix is treated as
     * legacy plaintext and returned as-is. Returns '' when decryption fails.
     */
    public static function open(string $stored): string
    {
        if ($stored === '') {
            return '';
        }
        if (!self::isSealed($stored)) {
            return $stored;
        }
        try {
            return (string) decrypt(substr($stored, strlen(self::PREFIX)));
        } catch (\Throwable $e) {
            return '';
        }
    }

    public static function isSealed(string $stored): bool
    {
        return strpos($stored, self::PREFIX) === 0;
    }

    /** Display mask: four bullets plus the last four characters (none when the secret is <= 4 chars). */
    public static function mask(string $plain): string
    {
        if ($plain === '') {
            return '';
        }
        $tail = strlen($plain) > 4 ? substr($plain, -4) : '';
        return "\u{2022}\u{2022}\u{2022}\u{2022}" . $tail;
    }
}
