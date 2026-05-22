<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Bitmask helper for the canonical six WHMCS recurring billing cycles.
 *
 * The Phase A.5 schema replaces three legacy per-cycle boolean flags with two
 * integer bitmasks: `catalog_cycles_mask` (which cycles SyncEngine writes to)
 * and `renewal_cycles_mask` (which cycles RenewalEngine considers). This class
 * is the single source of truth for bit assignments and translations between
 * mask integers and WHMCS cycle literals.
 *
 * Bit ordering matches WHMCS's natural cycle ordering (shortest → longest):
 *
 *   bit 0 = Monthly       (0b000001 = 1)
 *   bit 1 = Quarterly     (0b000010 = 2)
 *   bit 2 = Semi-Annually (0b000100 = 4)
 *   bit 3 = Annually      (0b001000 = 8)
 *   bit 4 = Biennially    (0b010000 = 16)
 *   bit 5 = Triennially   (0b100000 = 32)
 *
 * Non-recurring cycles (Free Account, One Time) are intentionally absent: they
 * have no representation in either mask and ::bitForCycle() returns null for
 * them. The pricing engine maps them to skip_reason = 'cycle_unsupported'
 * elsewhere.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class CycleSet
{
    public const BIT_MONTHLY     = 0;
    public const BIT_QUARTERLY   = 1;
    public const BIT_SEMIANNUAL  = 2;
    public const BIT_ANNUALLY    = 3;
    public const BIT_BIENNIALLY  = 4;
    public const BIT_TRIENNIALLY = 5;

    /** Maximum legal mask value (all six bits set). */
    public const MASK_MAX = 0b111111; // 63

    /**
     * Canonical WHMCS cycle literal → bit index. Order matches MAP iteration
     * order so toMask() and enabledCycles() are deterministic.
     *
     * @var array<string,int>
     */
    private const CYCLE_TO_BIT = [
        'Monthly'       => self::BIT_MONTHLY,
        'Quarterly'     => self::BIT_QUARTERLY,
        'Semi-Annually' => self::BIT_SEMIANNUAL,
        'Annually'      => self::BIT_ANNUALLY,
        'Biennially'    => self::BIT_BIENNIALLY,
        'Triennially'   => self::BIT_TRIENNIALLY,
    ];

    /** @var int the wrapped bitmask, always in range [0, MASK_MAX] */
    private $mask;

    private function __construct(int $mask)
    {
        // Defensive clamp: never store bits above bit 5. Callers that violate
        // the contract are silently corrected here rather than throwing — the
        // migration validation gate in Installer::migrateTo3() is where bad
        // data is supposed to be detected and refused.
        $this->mask = $mask & self::MASK_MAX;
    }

    /**
     * Build a CycleSet from a stored mask integer. Bits above bit 5 are
     * clamped away; the migration validation gate is responsible for
     * upstream rejection of out-of-range masks.
     */
    public static function fromMask(int $mask): self
    {
        return new self($mask);
    }

    /**
     * Build a CycleSet from a list of WHMCS cycle literals. Unknown / non-
     * recurring entries (e.g. 'Free Account', 'One Time') are silently
     * dropped — they have no bit assignment.
     *
     * @param list<string> $cycles
     */
    public static function fromCycles(array $cycles): self
    {
        $mask = 0;
        foreach ($cycles as $cycle) {
            $bit = self::bitForCycle($cycle);
            if ($bit !== null) {
                $mask |= (1 << $bit);
            }
        }
        return new self($mask);
    }

    /**
     * Translate the three legacy boolean flags from `mod_contabo_mapping`
     * (schema v2) into the equivalent bitmask integer used by schema v3.
     *
     * The legacy schema only ever tracked three cycles (Monthly, Semi-
     * Annually, Annually); Quarterly/Biennially/Triennially had no
     * representation and remain zero in the resulting mask.
     */
    public static function fromLegacyBooleans(bool $monthly, bool $semiannually, bool $annually): int
    {
        $mask = 0;
        if ($monthly) {
            $mask |= (1 << self::BIT_MONTHLY);
        }
        if ($semiannually) {
            $mask |= (1 << self::BIT_SEMIANNUAL);
        }
        if ($annually) {
            $mask |= (1 << self::BIT_ANNUALLY);
        }
        return $mask;
    }

    /** Return the wrapped bitmask integer. */
    public function toMask(): int
    {
        return $this->mask;
    }

    /**
     * Whether the given WHMCS cycle literal is enabled in this set.
     * Unknown / non-recurring cycles always return false.
     */
    public function contains(string $cycle): bool
    {
        $bit = self::bitForCycle($cycle);
        if ($bit === null) {
            return false;
        }
        return ($this->mask & (1 << $bit)) !== 0;
    }

    /**
     * Enumerate the enabled WHMCS cycle literals in canonical order
     * (Monthly → Triennially).
     *
     * @return list<string>
     */
    public function enabledCycles(): array
    {
        $out = [];
        foreach (self::CYCLE_TO_BIT as $cycle => $bit) {
            if (($this->mask & (1 << $bit)) !== 0) {
                $out[] = $cycle;
            }
        }
        return $out;
    }

    /**
     * Return the canonical bit index for the given WHMCS cycle literal, or
     * null for unknown / non-recurring cycles (Free Account, One Time, …).
     */
    public static function bitForCycle(string $cycle): ?int
    {
        return self::CYCLE_TO_BIT[$cycle] ?? null;
    }

    /**
     * All six canonical recurring WHMCS cycle literals in display order.
     *
     * @return list<string>
     */
    public static function allCycles(): array
    {
        return array_keys(self::CYCLE_TO_BIT);
    }
}
