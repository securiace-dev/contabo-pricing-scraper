<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * ServicePriceWriter — the SINGLE chokepoint that writes
 * `tblhosting.recurringamount`.
 *
 * Non-negotiable rule (Deliverable 10, items 1 & 2 of the plan):
 *   - SyncEngine NEVER touches tblhosting.
 *   - The ONLY callsite in this entire addon that mutates
 *     tblhosting.recurringamount is this class's updateRecurringAmount().
 *   - Any change introducing a new write site to tblhosting.recurringamount
 *     OUTSIDE this file MUST FAIL CODE REVIEW. The CI grep test enforces it.
 *
 * Preferred path: WHMCS LocalAPI `UpdateClientProduct`. If that returns
 * `result != success` (or throws) we fall back to a transactionally-scoped
 * direct Capsule update and log the fallback via `logActivity()`.
 *
 * Phase A semantics:
 *   The engine builds full decision rows but DOES NOT call this writer with
 *   `enabled=true`. The Installer/AdminController wires `enabled=false`, and
 *   `updateRecurringAmount()` returns immediately with a `writer_disabled_phase_a`
 *   via-marker. This keeps the production code path proven by tests while
 *   guaranteeing zero side effects until Phase B.
 *
 * PHP 7.4 + 8.x polyglot: no readonly, no constructor promotion, no match,
 * no str_starts_with, no named args, no non-capturing catch, no mixed.
 */
final class ServicePriceWriter
{
    /** @var bool */
    private $enabled;

    public function __construct(bool $enabled = false)
    {
        $this->enabled = $enabled;
    }

    /**
     * Update tblhosting.recurringamount for a single service.
     *
     * The ONLY callsite in the entire addon that touches that column.
     * Idempotent at the WHMCS level: writing the same value is a no-op.
     * The whole operation (write + action-log INSERT) runs inside a Capsule
     * transaction so a failure rolls back both sides cleanly.
     *
     * @param int    $serviceId  tblhosting.id
     * @param float  $newAmount  the new recurringamount to persist
     * @param string $reason     machine-readable: policy_used or
     *                           'manual_admin_approval'
     * @param int    $decisionId mod_contabo_price_decision.id that justifies
     *                           this write
     * @return array{applied: bool, via: string, message: string|null}
     */
    public function updateRecurringAmount(
        int $serviceId,
        float $newAmount,
        string $reason,
        int $decisionId
    ): array {
        if (!$this->enabled) {
            return [
                'applied' => false,
                'via'     => 'writer_disabled_phase_a',
                'message' => 'Phase A: writer is inert; no tblhosting mutation performed',
            ];
        }

        $self = $this;

        $result = [
            'applied' => false,
            'via'     => 'unknown',
            'message' => null,
        ];

        Capsule::connection()->transaction(static function () use (
            $self, $serviceId, $newAmount, $reason, $decisionId, &$result
        ): void {
            $via = $self->writeViaLocalApiOrFallback($serviceId, $newAmount);

            // Pair the decision with an immutable action ledger entry. Plan
            // Deliverable 8: approvals/applies INSERT a new row, never UPDATE
            // an existing decision row.
            Capsule::table('mod_contabo_pricing_action')->insert([
                'action_type' => 'apply',
                'service_id'  => $serviceId,
                'decision_id' => $decisionId,
                'admin_id'    => 0, // system
                'reason'      => $reason,
                'created_at'  => date('Y-m-d H:i:s'),
            ]);

            $result['applied'] = true;
            $result['via']     = $via['via'];
            $result['message'] = $via['message'];
        });

        return $result;
    }

    /**
     * Write the price through WHMCS LocalAPI UpdateClientProduct ONLY.
     *
     * Fails closed: a non-success result, a missing `result` key, an exception,
     * or a missing localAPI() helper all throw. There is deliberately NO raw
     * Capsule fallback — a raw `tblhosting` write bypasses WHMCS hooks and
     * invoice/recalculation logic and would mask an upstream failure. The throw
     * aborts the surrounding transaction so no action-ledger row is written.
     *
     * Visible to the transaction closure via $self. Intentionally not private
     * so the transaction closure (a static fn) can call it.
     *
     * @internal
     * @return array{via:string, message:string|null}
     * @throws \RuntimeException when the LocalAPI write did not succeed
     */
    public function writeViaLocalApiOrFallback(int $serviceId, float $newAmount): array
    {
        $formatted = number_format($newAmount, 4, '.', '');

        if (!function_exists('localAPI')) {
            $this->logFailure($serviceId, 'localAPI() unavailable');
            throw new \RuntimeException(
                'ServicePriceWriter: WHMCS localAPI() is unavailable; refusing to write service '
                . $serviceId . ' (no raw fallback).'
            );
        }

        try {
            /** @var array<string,mixed> $r */
            $r = \localAPI('UpdateClientProduct', [
                'serviceid'       => $serviceId,
                // `recurringamount` is the UpdateClientProduct API field (WHMCS
                // maps it to the `amount` column internally). It is an API
                // param, NOT a raw column.
                'recurringamount' => $formatted,
                // Notifier owns customer email; never let LocalAPI send it.
                'noemail'         => true,
            ]);
        } catch (\Throwable $e) {
            $this->logFailure($serviceId, 'UpdateClientProduct threw: ' . $e->getMessage());
            throw new \RuntimeException(
                'ServicePriceWriter: UpdateClientProduct threw for service ' . $serviceId . ': ' . $e->getMessage(),
                0,
                $e
            );
        }

        $status  = is_array($r) && isset($r['result']) ? (string) $r['result'] : '';
        $message = is_array($r) && isset($r['message']) ? (string) $r['message'] : '';
        if ($status !== 'success') {
            $this->logFailure($serviceId, 'UpdateClientProduct returned "' . $status . '": ' . $message);
            throw new \RuntimeException(
                'ServicePriceWriter: UpdateClientProduct did not succeed for service ' . $serviceId
                . ($status === '' ? ' (no result key)' : ' (' . $status . ')')
                . ($message !== '' ? ': ' . $message : '')
            );
        }
        return ['via' => 'localapi_updateclientproduct', 'message' => null];
    }

    private function logFailure(int $serviceId, string $message): void
    {
        if (function_exists('logActivity')) {
            \logActivity(
                'Contabo Pricing: ServicePriceWriter write failed (fail closed) on service '
                . $serviceId . ' — ' . $message
            );
        }
    }
}
