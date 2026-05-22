<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Append-only ledger of admin actions on pricing decisions and scheduled
 * changes. Separate from DecisionLog because the immutability rules are
 * different: a decision row records "what would happen / did happen at time
 * T", an action row records "an admin (or system) did X at time T+N".
 *
 * Approving a queued decision writes:
 *   1. an `approve` action row pointing at the original decision (here), AND
 *   2. a NEW decision row with `parent_decision_id = original.id` and
 *      `applied = 1` (DecisionLog::insert).
 *
 * The original decision is never mutated.
 *
 * action_type enum (kept in sync with plan deliverable 1):
 *   approve, reject, defer, force_approve, cancel_schedule,
 *   manual_override_set, manual_override_cleared, policy_changed,
 *   phase_changed, apply (system-emitted by ServicePriceWriter).
 *
 * PHP 7.4 polyglot: no constructor promotion, no readonly, no enums.
 */
class PricingActionLog
{
    /**
     * @var list<string>
     */
    private const VALID_ACTIONS = [
        'approve',
        'reject',
        'defer',
        'force_approve',
        'cancel_schedule',
        'manual_override_set',
        'manual_override_cleared',
        'policy_changed',
        'phase_changed',
        'apply',
    ];

    /**
     * Record an action. Returns the inserted row id.
     *
     * @param string      $actionType  One of self::VALID_ACTIONS.
     * @param int         $adminId     WHMCS admin id (`tbladmins.id`); 0 = system.
     * @param int|null    $serviceId   Service the action targets, if any.
     * @param int|null    $decisionId  Decision the action acts on, if any.
     * @param int|null    $scheduleId  Scheduled-change the action acts on, if any.
     * @param string|null $reason      Free-text rationale (admin-supplied).
     * @return int Inserted action id.
     * @throws \InvalidArgumentException for an unrecognised action_type.
     */
    public function record(
        string $actionType,
        int $adminId,
        ?int $serviceId,
        ?int $decisionId,
        ?int $scheduleId,
        ?string $reason
    ): int {
        if (!in_array($actionType, self::VALID_ACTIONS, true)) {
            throw new \InvalidArgumentException(
                'PricingActionLog: unknown action_type "' . $actionType . '"'
            );
        }
        if ($adminId < 0) {
            throw new \InvalidArgumentException('PricingActionLog: admin_id must be >= 0');
        }

        $row = [
            'action_type' => $actionType,
            'service_id'  => $serviceId,
            'decision_id' => $decisionId,
            'schedule_id' => $scheduleId,
            'admin_id'    => $adminId,
            'reason'      => $reason,
            'created_at'  => date('Y-m-d H:i:s'),
        ];

        return $this->storeRow($row);
    }

    /**
     * The list of valid action_type strings. Useful for admin UI dropdowns and
     * for test assertions.
     *
     * @return list<string>
     */
    public static function validActions(): array
    {
        return self::VALID_ACTIONS;
    }

    /**
     * Backed-by-Capsule INSERT. Subclasses (tests) override.
     *
     * @param array<string, mixed> $row
     */
    protected function storeRow(array $row): int
    {
        return (int) Capsule::table('mod_contabo_pricing_action')->insertGetId($row);
    }
}
