<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Canonical Contabo dimension-key → WHMCS configurable-option `optiontype` map.
 *
 * WHMCS `tblproductconfigoptions.optiontype` values (verified on
 * my.securiace.com, see PHASE_A52_DESIGN_IMPACT.md §"Verified facts"):
 *
 *   0 = dropdown
 *   1 = radio
 *   2 = yes/no   (a single toggle sub-option)
 *   3 = quantity (qtyminimum / qtymaximum, one priced sub-option per unit)
 *   4 = text
 *
 * The dimension keys understood here are the *normalised* keys emitted by
 * {@see DimensionParser}, NOT the raw Contabo `dimension` strings. In
 * particular Networking is split into three independent concerns by the
 * parser, so this mapper sees `Networking:Bandwidth`, `Networking:IPv4` and
 * `Networking:Private Networking`, never a bare `Networking`.
 *
 * Mapping table (verified against cloud-vps-10 configurator data):
 *
 *   | Normalised dimension key       | optiontype | rationale                       |
 *   |--------------------------------|-----------:|---------------------------------|
 *   | Image                          |  0 dropdown| 34 mutually-exclusive values    |
 *   | Region                         |  1 radio   | one choice, modest cardinality  |
 *   | Storage Type                   |  1 radio   | one choice, ~4 tiers            |
 *   | Data Protection                |  2 yes/no  | exactly None / Auto Backup      |
 *   | Networking:Bandwidth           |  0 dropdown| one choice, several tiers       |
 *   | Networking:IPv4                |  3 qty     | additional IPs are a quantity   |
 *   | Networking:Private Networking  |  2 yes/no  | on / off toggle                 |
 *
 * Region/Data Protection fall back to a value-count heuristic when the caller
 * supplies one ({@see mapForWithValueCount}): a radio with too many values is
 * unwieldy, so >MANY_VALUES_THRESHOLD values upgrades a radio to a dropdown,
 * and a Data Protection dimension that is NOT exactly two values degrades from
 * yes/no to radio. The plain {@see mapFor} uses the documented defaults above.
 *
 * Pure class: no DB, no WHMCS calls, fully deterministic.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class OptionTypeMapper
{
    /** WHMCS optiontype literals. */
    public const TYPE_DROPDOWN = 0;
    public const TYPE_RADIO    = 1;
    public const TYPE_YESNO    = 2;
    public const TYPE_QUANTITY = 3;
    public const TYPE_TEXT     = 4;

    /** Normalised dimension keys (mirrors DimensionParser output). */
    public const DIM_IMAGE             = 'Image';
    public const DIM_REGION            = 'Region';
    public const DIM_STORAGE_TYPE      = 'Storage Type';
    public const DIM_DATA_PROTECTION   = 'Data Protection';
    public const DIM_NET_BANDWIDTH     = 'Networking:Bandwidth';
    public const DIM_NET_IPV4          = 'Networking:IPv4';
    public const DIM_NET_PRIVATE       = 'Networking:Private Networking';

    /**
     * A radio control with more than this many values is upgraded to a
     * dropdown by {@see mapForWithValueCount}. Image is always a dropdown
     * regardless because its default already exceeds this.
     */
    public const MANY_VALUES_THRESHOLD = 12;

    /**
     * Default dimension-key → optiontype map. The single source of truth for
     * {@see mapFor}; documented in the class docblock.
     *
     * @var array<string,int>
     */
    private const DIMENSION_TO_TYPE = [
        self::DIM_IMAGE           => self::TYPE_DROPDOWN,
        self::DIM_REGION          => self::TYPE_RADIO,
        self::DIM_STORAGE_TYPE    => self::TYPE_RADIO,
        self::DIM_DATA_PROTECTION => self::TYPE_YESNO,
        self::DIM_NET_BANDWIDTH   => self::TYPE_DROPDOWN,
        self::DIM_NET_IPV4        => self::TYPE_QUANTITY,
        self::DIM_NET_PRIVATE     => self::TYPE_YESNO,
    ];

    /**
     * Map a normalised dimension key to its default WHMCS optiontype.
     *
     * Unknown keys fall back to a dropdown (0) — the safest default for an
     * unrecognised mutually-exclusive dimension and never accidentally a
     * quantity or yes/no.
     */
    public static function mapFor(string $dimensionKey): int
    {
        return self::DIMENSION_TO_TYPE[$dimensionKey] ?? self::TYPE_DROPDOWN;
    }

    /**
     * Map a dimension key to an optiontype, refined by the actual number of
     * selectable values. This is what the syncer should call when it knows the
     * concrete cardinality of a plan's dimension:
     *
     *   - Region/Storage Type: radio normally, dropdown if > MANY_VALUES_THRESHOLD.
     *   - Data Protection: yes/no only when there are exactly two values
     *     (None + Auto Backup); otherwise radio.
     *   - Networking:Private Networking: yes/no when two values, else radio.
     *   - Everything else: the documented default from {@see mapFor}.
     */
    public static function mapForWithValueCount(string $dimensionKey, int $valueCount): int
    {
        $default = self::mapFor($dimensionKey);

        // A two-value mutually-exclusive dimension is a natural yes/no, but a
        // yes/no with anything other than two values is invalid in WHMCS, so
        // degrade to a radio.
        if ($default === self::TYPE_YESNO) {
            return $valueCount === 2 ? self::TYPE_YESNO : self::TYPE_RADIO;
        }

        // A radio with too many entries is unusable; promote to a dropdown.
        if ($default === self::TYPE_RADIO && $valueCount > self::MANY_VALUES_THRESHOLD) {
            return self::TYPE_DROPDOWN;
        }

        return $default;
    }

    /** True when the dimension is modelled as a WHMCS quantity option (IPv4). */
    public static function isQuantity(string $dimensionKey): bool
    {
        return self::mapFor($dimensionKey) === self::TYPE_QUANTITY;
    }

    /** True when the dimension is modelled as a WHMCS yes/no toggle. */
    public static function isYesNo(string $dimensionKey): bool
    {
        return self::mapFor($dimensionKey) === self::TYPE_YESNO;
    }

    /**
     * Expose the full default mapping table (for diagnostics / admin preview).
     *
     * @return array<string,int>
     */
    public static function table(): array
    {
        return self::DIMENSION_TO_TYPE;
    }
}
