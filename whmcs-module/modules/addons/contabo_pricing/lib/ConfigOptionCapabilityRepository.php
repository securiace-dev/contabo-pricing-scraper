<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * A.5.2 amendment #6 — sole read/write chokepoint for the per-(plan, dimension,
 * value) capability matrix table mod_contabo_option_capability (schema v5,
 * design §4). The table is created by {@see Installer} (migrateTo5); this class
 * never migrates — it only reads and writes rows.
 *
 * The capability matrix answers, for one Contabo (plan, dimension, value)
 * triple: is the change allowed at create/reinstall/post-provision/upgrade/
 * downgrade, is it destructive / does it lose data, and — crucially — how
 * trustworthy is that answer ({@see capability_source}).
 *
 * Mirrors {@see ConfigOptionLinkRepository}: plain Capsule, upsert-by-unique-key
 * then re-read for the row, every ->first() result cast to (array) (real WHMCS
 * returns stdClass; FakeCapsule returns arrays), and a whitelist of writable
 * columns so a caller can never set an unknown/dangerous column.
 *
 * ── Capability-source gating (amendment #6) ────────────────────────────────
 * capability_source is an enum-by-varchar. Only api_verified is trusted enough
 * to AUTO-APPLY a destructive or in-place change without a human in the loop;
 * every weaker source must fall back to admin approval for such changes. A
 * non-destructive, metadata-only change may always proceed regardless of source.
 * See {@see canAutoApply} for the exact rule and its truth table.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args, no str_starts_with. Runs on PHP 7.4 + 8.x.
 */
final class ConfigOptionCapabilityRepository
{
    private const T_CAP = 'mod_contabo_option_capability';

    /**
     * Valid capability_source values (amendment #6). Stored as a varchar; this
     * is the authoritative enum. Order is from most to least trustworthy.
     */
    public const SOURCE_API_VERIFIED      = 'api_verified';
    public const SOURCE_SCRAPE_VERIFIED   = 'scrape_verified';
    public const SOURCE_MANUAL_ASSUMPTION = 'manual_assumption';
    public const SOURCE_ADMIN_OVERRIDE    = 'admin_override';
    public const SOURCE_UNKNOWN           = 'unknown';

    /** @var list<string> */
    public const VALID_SOURCES = [
        self::SOURCE_API_VERIFIED,
        self::SOURCE_SCRAPE_VERIFIED,
        self::SOURCE_MANUAL_ASSUMPTION,
        self::SOURCE_ADMIN_OVERRIDE,
        self::SOURCE_UNKNOWN,
    ];

    /**
     * Boolean capability flags writable through {@see upsertCapability}. Anything
     * not in this list (plus {@see WRITABLE_SCALARS}) is dropped, so a caller can
     * never set an unknown column. Mirrors the TINYINT(1) columns in the schema.
     *
     * @var list<string>
     */
    public const BOOLEAN_FLAGS = [
        'allowed_on_create',
        'allowed_on_reinstall',
        'allowed_on_post_provision',
        'allowed_on_upgrade',
        'allowed_on_downgrade',
        'requires_reinstall',
        'requires_recreate',
        'destructive_change',
        'data_loss_expected',
        'requires_backup_warning',
        'requires_admin_approval',
        'billing_change_possible',
    ];

    /**
     * Non-boolean writable scalar columns. provisioning_action is a free-ish
     * verb string; capability_source is the enum validated against
     * {@see VALID_SOURCES}.
     *
     * @var list<string>
     */
    public const WRITABLE_SCALARS = [
        'provisioning_action',
        'capability_source',
    ];

    /**
     * Upsert a capability row keyed by (contabo_plan_slug, dimension_key,
     * value_key). $flags is whitelisted to {@see BOOLEAN_FLAGS} (cast to 0/1) and
     * {@see WRITABLE_SCALARS}; any unknown key is dropped. capability_source, when
     * present, must be one of {@see VALID_SOURCES} or it is dropped too.
     *
     * On first insert capability_source defaults to manual_assumption (the
     * conservative schema default) unless a valid one was supplied. last_verified_at
     * is stamped on every upsert. Returns the full re-read row.
     *
     * @param array<string,mixed> $flags
     * @return array<string,mixed>
     */
    public function upsertCapability(string $planSlug, string $dimensionKey, string $valueKey, array $flags): array
    {
        $now = date('Y-m-d H:i:s');
        $key = [
            'contabo_plan_slug' => $planSlug,
            'dimension_key'     => $dimensionKey,
            'value_key'         => $valueKey,
        ];

        $values = ['last_verified_at' => $now];

        foreach (self::BOOLEAN_FLAGS as $flag) {
            if (array_key_exists($flag, $flags)) {
                $values[$flag] = $flags[$flag] ? 1 : 0;
            }
        }
        if (array_key_exists('provisioning_action', $flags)) {
            $pa = $flags['provisioning_action'];
            $values['provisioning_action'] = $pa === null ? null : (string) $pa;
        }
        if (array_key_exists('capability_source', $flags)
            && in_array((string) $flags['capability_source'], self::VALID_SOURCES, true)
        ) {
            $values['capability_source'] = (string) $flags['capability_source'];
        }

        $existing = $this->find($planSlug, $dimensionKey, $valueKey);
        if ($existing === null) {
            // FakeCapsule doesn't apply schema column defaults on insert, and we
            // never want capability_source to land NULL — pin the conservative
            // default if the caller didn't supply a valid one.
            if (!array_key_exists('capability_source', $values)) {
                $values['capability_source'] = self::SOURCE_MANUAL_ASSUMPTION;
            }
        }

        Capsule::table(self::T_CAP)->updateOrInsert($key, $values);
        return $this->find($planSlug, $dimensionKey, $valueKey) ?? [];
    }

    /** @return array<string,mixed>|null */
    public function find(string $planSlug, string $dimensionKey, string $valueKey): ?array
    {
        $r = Capsule::table(self::T_CAP)
            ->where('contabo_plan_slug', $planSlug)
            ->where('dimension_key', $dimensionKey)
            ->where('value_key', $valueKey)
            ->first();
        return $r !== null ? (array) $r : null;
    }

    /**
     * All capability rows for one plan, in stable id order.
     *
     * @return list<array<string,mixed>>
     */
    public function listForPlan(string $planSlug): array
    {
        $rows = Capsule::table(self::T_CAP)
            ->where('contabo_plan_slug', $planSlug)
            ->orderBy('id')
            ->get();

        $out = [];
        foreach ($rows as $row) {
            $out[] = (array) $row;
        }
        return $out;
    }

    /**
     * Capability-source gating (amendment #6).
     *
     * Decides whether the change described by $capabilityRow may be applied
     * automatically (no admin approval) given whether it is destructive or an
     * in-place change.
     *
     *   - A non-destructive, metadata-only change ($isDestructiveOrInPlaceChange
     *     === false) may ALWAYS auto-apply, regardless of capability_source.
     *   - A destructive / in-place change may auto-apply ONLY when
     *     capability_source === api_verified. Every weaker source
     *     (scrape_verified, manual_assumption, admin_override, unknown, or any
     *     missing/invalid value) requires admin approval → returns false.
     *
     * Truth table:
     *
     *   capability_source   | destructive/in-place | non-destructive
     *   --------------------+----------------------+-----------------
     *   api_verified        | true  (auto-apply)   | true
     *   scrape_verified     | false (needs admin)  | true
     *   manual_assumption   | false (needs admin)  | true
     *   admin_override      | false (needs admin)  | true
     *   unknown             | false (needs admin)  | true
     *   (missing / invalid) | false (needs admin)  | true
     *
     * Note: admin_override records a human DECISION about capability, not a
     * verified safety guarantee, so it deliberately does NOT bypass approval for
     * a destructive change here — the approval gate is the override path.
     *
     * @param array<string,mixed> $capabilityRow
     */
    public function canAutoApply(array $capabilityRow, bool $isDestructiveOrInPlaceChange): bool
    {
        if (!$isDestructiveOrInPlaceChange) {
            return true;
        }
        $source = isset($capabilityRow['capability_source'])
            ? (string) $capabilityRow['capability_source']
            : '';
        return $source === self::SOURCE_API_VERIFIED;
    }
}
