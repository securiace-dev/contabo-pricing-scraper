<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Size gate for admin-posted JSON before json_decode or a save write.
 *
 * The largest options payload in the PHPUnit suite is the fixed-profile
 * selection in ProfileIdentityTest (220 bytes). 4× that is 880 bytes.
 * 64 KiB is above that floor and below the 1 MiB hard cap, and it still
 * fits a full configurator selection (one chosen value per control).
 *
 * Already-decoded arrays are measured by a node walk. They are not passed
 * through json_encode, which would allocate a second copy of a hostile value.
 * PHP 7.4 compatible.
 */
final class JsonPayloadCap
{
    public const MAX_BYTES = 65536;

    /**
     * @param mixed $raw
     */
    public static function exceeds($raw): bool
    {
        if (is_string($raw)) {
            return strlen($raw) > self::MAX_BYTES;
        }
        if (!is_array($raw)) {
            return false;
        }
        return self::arrayExceeds($raw);
    }

    /**
     * @param array<mixed> $root
     */
    private static function arrayExceeds(array $root): bool
    {
        // A JSON array of single-digit numbers is about 2 bytes per element.
        // Abort the walk once that many nodes could not encode under the ceiling.
        $maxNodes = intdiv(self::MAX_BYTES, 2);
        $nodes = 0;
        $bytes = 2;
        $stack = [$root];
        while ($stack !== []) {
            $current = array_pop($stack);
            foreach ($current as $key => $child) {
                $nodes++;
                if ($nodes > $maxNodes) {
                    return true;
                }
                if (is_string($key)) {
                    $bytes += strlen($key) + 3;
                } else {
                    $bytes += 12;
                }
                $bytes += 1;
                if (is_string($child)) {
                    $bytes += strlen($child) + 2;
                } elseif (is_int($child) || is_float($child)) {
                    $bytes += 32;
                } elseif (is_bool($child)) {
                    $bytes += 5;
                } elseif ($child === null) {
                    $bytes += 4;
                } elseif (is_array($child)) {
                    $bytes += 2;
                    $stack[] = $child;
                } else {
                    return true;
                }
                if ($bytes > self::MAX_BYTES) {
                    return true;
                }
            }
        }
        return false;
    }
}
