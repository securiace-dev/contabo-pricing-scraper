<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Single facade the order / configurator / apply flow calls to ask
 * "is this option selection OK?" for a Contabo plan.
 *
 * It composes the two read-only chokepoints without touching either:
 *
 *   - {@see ConfigOptionCompatibilityRepository::validateCombination()} (design
 *     §5) — the HARD gate. Its {valid, violations} are taken verbatim; a
 *     violation here BLOCKS the selection (valid=false).
 *
 *   - {@see ConfigOptionCapabilityRepository::find()} (design §4, amendment #6)
 *     — the SOFT signal. For each selected value whose capability row marks the
 *     change destructive_change, a "capability warning" is emitted carrying the
 *     backup / admin-approval flags and the capability_source so the caller can
 *     decide how to surface it. Warnings DO NOT block — `valid` reflects
 *     compatibility only.
 *
 * Both inputs are read-only, so validate() is deterministic and side-effect-free:
 * a selection with no compatibility rule and no capability row is valid, with no
 * violations and no warnings.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args, no str_starts_with. Runs on PHP 7.4 + 8.x.
 */
final class SelectionValidator
{
    /** @var ConfigOptionCompatibilityRepository */
    private $compat;

    /** @var ConfigOptionCapabilityRepository */
    private $cap;

    public function __construct(
        ?ConfigOptionCompatibilityRepository $compat = null,
        ?ConfigOptionCapabilityRepository $cap = null
    ) {
        $this->compat = $compat !== null ? $compat : new ConfigOptionCompatibilityRepository();
        $this->cap = $cap !== null ? $cap : new ConfigOptionCapabilityRepository();
    }

    /**
     * Validate a set of configurable-option selections for one plan.
     *
     * $selections is a list of:
     *   ['dimension_key' => string, 'value_key' => string, 'qty' => int (optional)]
     *
     * Steps:
     *   1. Run the compatibility repo's validateCombination() and take its
     *      {valid, violations} verbatim — these are the HARD blockers.
     *   2. For each selected value, look up its capability row; when
     *      destructive_change is truthy, append a capability warning. Warnings
     *      never affect `valid`.
     *
     * Returns:
     *   [
     *     'valid'               => bool,   // compatibility only — warnings don't block
     *     'violations'          => list<[  // verbatim from the compatibility repo
     *         'dimension_key' => string,
     *         'value_key'     => string,
     *         'reason'        => string,
     *         'detail'        => string,
     *     ]>,
     *     'capability_warnings' => list<[
     *         'dimension_key'           => string,
     *         'value_key'               => string,
     *         'kind'                    => string,  // always 'destructive'
     *         'requires_backup_warning' => bool,
     *         'requires_admin_approval' => bool,
     *         'capability_source'       => string,
     *     ]>,
     *   ]
     *
     * @param list<array<string,mixed>> $selections
     * @return array{
     *     valid:bool,
     *     violations:list<array<string,string>>,
     *     capability_warnings:list<array<string,mixed>>
     * }
     */
    public function validate(string $planSlug, array $selections): array
    {
        // (1) Hard gate — taken verbatim. Defensive defaults in case a stub repo
        // returns a partial shape; the real repo always supplies both keys.
        $compatResult = $this->compat->validateCombination($planSlug, $selections);
        $valid = isset($compatResult['valid']) ? (bool) $compatResult['valid'] : true;
        $violations = (isset($compatResult['violations']) && is_array($compatResult['violations']))
            ? $compatResult['violations']
            : [];

        // (2) Soft signal — destructive capabilities become warnings, not blocks.
        $capabilityWarnings = [];
        foreach ($selections as $sel) {
            $dim = isset($sel['dimension_key']) ? (string) $sel['dimension_key'] : '';
            $val = isset($sel['value_key']) ? (string) $sel['value_key'] : '';

            $row = $this->cap->find($planSlug, $dim, $val);
            if ($row === null) {
                continue; // no capability row → no warning
            }

            $isDestructive = isset($row['destructive_change']) && (bool) $row['destructive_change'];
            if (!$isDestructive) {
                continue;
            }

            $capabilityWarnings[] = [
                'dimension_key'           => $dim,
                'value_key'               => $val,
                'kind'                    => 'destructive',
                'requires_backup_warning' => isset($row['requires_backup_warning'])
                    && (bool) $row['requires_backup_warning'],
                'requires_admin_approval' => isset($row['requires_admin_approval'])
                    && (bool) $row['requires_admin_approval'],
                'capability_source'       => isset($row['capability_source'])
                    ? (string) $row['capability_source']
                    : '',
            ];
        }

        return [
            'valid'               => $valid,
            'violations'          => $violations,
            'capability_warnings' => $capabilityWarnings,
        ];
    }
}
