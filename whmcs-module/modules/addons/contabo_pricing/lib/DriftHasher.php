<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Stable canonical hash of the addon-controlled fields of a WHMCS config object.
 *
 * Used by the apply path (amendment #14 drift detection): when the addon creates
 * a WHMCS object (configurable option group / option / value) it records a
 * baseline hash of *only the columns it controls*. Before a later sync re-applies
 * changes, it re-hashes the live row's controlled columns and compares against the
 * stored baseline. If they differ, an admin has manually edited the object since
 * the addon last wrote it, and the apply path refuses to clobber the change.
 *
 * Design requirements (all enforced by DriftHasherTest):
 *
 *   - Deterministic & pure: no DB, no IO, no clock, no randomness. Same input →
 *     same output, on every run and across PHP versions.
 *   - Order-independent: the same logical field map produces the same hash
 *     regardless of the order its keys happen to be iterated in (rows coming back
 *     from Capsule / different code paths must not produce a different baseline).
 *   - No serialize() / no json_encode() of values: serialize() embeds PHP-version
 *     dependent type tags and json_encode()'s float formatting drifts between
 *     builds. We build the canonical string by hand so the bytes are stable.
 *   - Collision-resistant across the "tricky" scalars: null, '' (empty string),
 *     '0', 0, false, true must all hash to DISTINCT canonical values. We tag
 *     null with a NUL-prefixed sentinel ("\0null") that no plain string value
 *     can legally produce, and we cast bool to '1'/'0' and numbers to a canonical
 *     numeric string so an int 0 and the literal string '0' both canonicalise to
 *     '0' (they're indistinguishable as WHMCS column data and SHOULD collide),
 *     while null and '' stay distinct from both.
 *
 * Canonical form:
 *
 *   ksort the map by key (string compare), then for each entry emit
 *
 *       <key> "\x1e=" <canonical-value>
 *
 *   joined by "\x1f" (ASCII Unit Separator). The "\x1e" (Record Separator) before
 *   the '=' and the "\x1f" record separator are control bytes that don't appear in
 *   WHMCS column names and won't be confused with literal '=' / separator chars
 *   inside a value, so two different maps can't canonicalise to the same string by
 *   shuffling where one key ends and the next begins. The whole string is then
 *   sha1()'d.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args. All-static — there is no instance state.
 */
final class DriftHasher
{
    /**
     * Sentinel canonical value for PHP null. Begins with a NUL byte so no
     * ordinary string field value (which WHMCS never stores with a leading NUL)
     * can collide with it, keeping null distinct from '' and '0' and 0.
     */
    private const NULL_SENTINEL = "\0null";

    /** Byte placed between a key and its value inside one canonical pair. */
    private const KV_SEP = "\x1e=";

    /** Byte placed between consecutive canonical pairs. */
    private const PAIR_SEP = "\x1f";

    /**
     * Stable canonical hash of an associative array of fields.
     *
     * Normalises to a canonical form (ksort by key; each value cast to a string
     * with a fixed rule, see ::canonicalizeValue()), joins as key=value pairs
     * with collision-proof separators, and sha1()'s the result. Order-independent
     * and stable across runs / PHP versions.
     *
     * @param array<string,mixed> $fields
     * @return string 40-char lowercase hex sha1 digest.
     */
    public static function hash(array $fields): string
    {
        // Order independence: sort by key so any input ordering canonicalises
        // to the same byte stream.
        ksort($fields, SORT_STRING);

        $pairs = [];
        foreach ($fields as $key => $value) {
            $pairs[] = (string) $key . self::KV_SEP . self::canonicalizeValue($value);
        }

        return sha1(implode(self::PAIR_SEP, $pairs));
    }

    /**
     * Pick ONLY $fieldNames out of $row (a missing key is treated as null) and
     * hash that subset. This is the primary API: a caller hashes just the columns
     * the addon controls — e.g. an option's
     * ['optionname','optiontype','qtyminimum','qtymaximum','hidden'] — so that
     * WHMCS-managed columns (id, created_at, updated_at, sort order, …) that the
     * addon never sets are ignored and don't show up as spurious drift.
     *
     * @param array<string,mixed> $row        The full row (extra keys ignored).
     * @param list<string>        $fieldNames The controlled columns to hash, in
     *                                         any order (hash() re-sorts them).
     * @return string 40-char lowercase hex sha1 digest.
     */
    public static function hashFields(array $row, array $fieldNames): string
    {
        $subset = [];
        foreach ($fieldNames as $name) {
            $name = (string) $name;
            $subset[$name] = array_key_exists($name, $row) ? $row[$name] : null;
        }

        return self::hash($subset);
    }

    /**
     * Whether $currentRow's controlled columns still hash to $expectedHash, i.e.
     * the object has NOT been manually edited since the baseline was recorded.
     *
     * An empty ('') $expectedHash means "no baseline recorded / unknown" and
     * always returns false: with no baseline we can't prove the row is unchanged,
     * so we report "not matching" and let the caller decide what to do (typically:
     * treat as drifted / require explicit re-adoption rather than silently
     * clobbering). This guard also avoids accidentally treating a row whose
     * computed hash is somehow empty (it never is — sha1 is fixed-length) as a
     * match against an unset baseline.
     *
     * @param string              $expectedHash Previously recorded baseline hash.
     * @param array<string,mixed> $currentRow   The live WHMCS row.
     * @param list<string>        $fieldNames   Controlled columns to compare.
     */
    public static function matches(string $expectedHash, array $currentRow, array $fieldNames): bool
    {
        if ($expectedHash === '') {
            return false;
        }

        return self::hashFields($currentRow, $fieldNames) === $expectedHash;
    }

    /**
     * Fixed scalar → canonical string rule. Kept private so the canonical form
     * has exactly one definition.
     *
     *   null         → "\0null"   (NUL-prefixed sentinel; distinct from '' / '0')
     *   bool         → '1' / '0'
     *   int / float  → canonical numeric string via (string)(0 + $v); this
     *                  normalises e.g. the float 10.0 and the int 10 and the
     *                  string '10' to the same '10', and avoids json_encode()'s
     *                  build-dependent float formatting.
     *   everything   → (string) $v
     *
     * Note int 0 / float 0.0 / string '0' all canonicalise to '0' on purpose:
     * as raw column data they're indistinguishable and SHOULD collide. null and
     * '' remain distinct from each other and from '0'.
     *
     * @param mixed $value
     */
    private static function canonicalizeValue($value): string
    {
        if ($value === null) {
            return self::NULL_SENTINEL;
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            // 0 + $v keeps numeric type but normalises across int/float; the cast
            // then yields PHP's canonical decimal string (no locale, no exponent
            // for ordinary magnitudes), which is stable across PHP versions for
            // the integer-ish values WHMCS stores in these columns.
            return (string) (0 + $value);
        }

        return (string) $value;
    }
}
