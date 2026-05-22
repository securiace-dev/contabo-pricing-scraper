<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Atomic, single-holder cron lock for the renewal engine.
 *
 * Two paths in priority order:
 *   1. MySQL `SELECT GET_LOCK(name, 0)` — preferred. Auto-releases on
 *      connection close, so a crashing PHP process won't deadlock the cron.
 *   2. Compare-and-swap on `mod_contabo_repricing_lock`. Used when GET_LOCK
 *      is unavailable (some managed MySQL setups restrict named locks).
 *
 * The fallback table is created by Installer; this class assumes it exists.
 * A holder UUID is generated per acquire and MUST be passed back to release —
 * a stale holder can't accidentally drop someone else's lock.
 *
 * PHP 7.4 polyglot: no constructor promotion, no readonly.
 */
class Lock
{
    /**
     * Try to acquire a named lock.
     *
     * @param string $name        Lock name (e.g. 'contabo_repricing').
     * @param int    $ttlSeconds  Lifetime hint for the fallback table path.
     *                            (GET_LOCK path ignores this — auto-frees on
     *                            connection close.) Must be > 0.
     * @return string|null Holder token (UUID) on success, null on contention.
     */
    public function acquire(string $name, int $ttlSeconds): ?string
    {
        if ($ttlSeconds <= 0) {
            $ttlSeconds = 300;
        }

        $token = self::uuidV4();

        // Path 1: GET_LOCK (preferred).
        $got = $this->tryGetLock($name);
        if ($got === true) {
            return $token;
        }
        if ($got === false) {
            // GET_LOCK works on this server but someone else holds it.
            return null;
        }

        // Path 2: GET_LOCK unavailable — fall back to compare-and-swap table.
        return $this->tryFallbackTable($name, $ttlSeconds, $token) ? $token : null;
    }

    /**
     * Release a previously-acquired lock. Safe to call with a stale token —
     * the row is only deleted when the holder matches.
     *
     * @param string $name  Lock name.
     * @param string $token Holder token returned by acquire().
     */
    public function release(string $name, string $token): void
    {
        if ($token === '') {
            return;
        }

        // Path 1: RELEASE_LOCK. No-op if we never acquired via GET_LOCK.
        $this->tryReleaseLock($name);

        // Path 2: delete fallback row IFF holder matches.
        try {
            Capsule::table('mod_contabo_repricing_lock')
                ->where('name', $name)
                ->where('holder', $token)
                ->delete();
        } catch (\Throwable $e) {
            // Best-effort release — surface via logActivity in production,
            // suppress here so the engine can continue.
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: Lock::release fallback table failed: ' . $e->getMessage());
            }
        }
    }

    /**
     * @return bool|null true=acquired, false=contended, null=GET_LOCK unsupported.
     */
    protected function tryGetLock(string $name): ?bool
    {
        try {
            $row = Capsule::connection()
                ->select('SELECT GET_LOCK(?, 0) AS got', [$name]);
            if (!is_array($row) || $row === []) {
                return null;
            }
            $first = $row[0];
            $got = is_array($first) ? ($first['got'] ?? null) : ($first->got ?? null);
            if ($got === null) {
                // MySQL returns NULL when GET_LOCK errors out (e.g. unsupported).
                return null;
            }
            return ((int) $got) === 1;
        } catch (\Throwable $e) {
            return null;
        }
    }

    protected function tryReleaseLock(string $name): void
    {
        try {
            Capsule::connection()->select('SELECT RELEASE_LOCK(?) AS released', [$name]);
        } catch (\Throwable $e) {
            // Best effort.
        }
    }

    protected function tryFallbackTable(string $name, int $ttlSeconds, string $token): bool
    {
        $now            = date('Y-m-d H:i:s');
        $expiresAtTs    = time() + $ttlSeconds;
        $lockedUntilStr = date('Y-m-d H:i:s', $expiresAtTs);

        try {
            // Phase 1: optimistic insert. Wins iff no row exists for $name.
            $inserted = false;
            try {
                Capsule::table('mod_contabo_repricing_lock')->insert([
                    'name'         => $name,
                    'locked_until' => $lockedUntilStr,
                    'holder'       => $token,
                ]);
                $inserted = true;
            } catch (\Throwable $e) {
                // Likely PK collision — fall through to phase 2.
            }
            if ($inserted) {
                return true;
            }

            // Phase 2: compare-and-swap — only steal if the existing row expired.
            $affected = Capsule::table('mod_contabo_repricing_lock')
                ->where('name', $name)
                ->where('locked_until', '<', $now)
                ->update([
                    'locked_until' => $lockedUntilStr,
                    'holder'       => $token,
                ]);

            return ((int) $affected) > 0;
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing: Lock::acquire fallback table failed: ' . $e->getMessage());
            }
            return false;
        }
    }

    /**
     * Strong-ish UUIDv4 generator. random_bytes() is available on PHP 7+.
     */
    private static function uuidV4(): string
    {
        try {
            $bytes = random_bytes(16);
        } catch (\Throwable $e) {
            // Should never happen on a sane host; mt_rand fallback.
            $bytes = '';
            for ($i = 0; $i < 16; $i++) {
                $bytes .= chr(mt_rand(0, 255));
            }
        }
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }
}
