<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Seal / open helper for secrets stored in addon tables (provider API keys).
 *
 * Wraps WHMCS's global encrypt()/decrypt() helpers and marks sealed values
 * with the "ENC:" prefix. Plaintext
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
     * legacy plaintext and returned as-is.
     *
     * A sealed value that cannot be decrypted (WHMCS encryption key changed, or
     * the value is corrupt) is NEVER returned as '': that would look exactly
     * like "no key configured". It is logged to the activity log (never the
     * value) and raised as SecretUnreadableException. Re-seal by re-entering
     * the key; see docs/SECRET_RESEAL.md.
     *
     * @throws SecretUnreadableException
     */
    public static function open(string $stored): string
    {
        if ($stored === '') {
            return '';
        }
        if (!self::isSealed($stored)) {
            return $stored;
        }
        $plain = '';
        $cause = 'decrypt() returned an empty value';
        try {
            $plain = (string) decrypt(substr($stored, strlen(self::PREFIX)));
        } catch (\Throwable $e) {
            $cause = 'decrypt() raised ' . get_class($e);
        }
        if ($plain === '') {
            $message = 'Contabo Pricing: a sealed secret cannot be decrypted (' . $cause . '). '
                . 'The WHMCS encryption key most likely changed since it was saved. '
                . 'Re-enter the API key in the add-on to re-seal it; see docs/SECRET_RESEAL.md.';
            if (function_exists('logActivity')) {
                logActivity($message);
            }
            throw new SecretUnreadableException($message);
        }
        return $plain;
    }

    /**
     * Like open() but reports unreadable as null instead of throwing, for
     * display paths. The failure is still logged by open().
     */
    public static function tryOpen(string $stored): ?string
    {
        try {
            return self::open($stored);
        } catch (SecretUnreadableException $e) {
            return null;
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
