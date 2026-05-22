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
     * Try WHMCS LocalAPI UpdateClientProduct first; fall back to a direct
     * Capsule update if it throws or returns a non-success status.
     *
     * Visible to the transaction closure via $self. Intentionally not private
     * so the transaction closure (a static fn) can call it.
     *
     * @internal
     * @return array{via:string, message:string|null}
     */
    public function writeViaLocalApiOrFallback(int $serviceId, float $newAmount): array
    {
        $formatted = number_format($newAmount, 4, '.', '');

        if (function_exists('localAPI')) {
            try {
                /** @var array<string,mixed> $r */
                $r = \localAPI('UpdateClientProduct', [
                    'serviceid'       => $serviceId,
                    'recurringamount' => $formatted,
                    // Notifier owns customer email; never let LocalAPI send it.
                    'noemail'         => true,
                ]);
                $status  = isset($r['result']) ? (string) $r['result'] : '';
                $message = isset($r['message']) ? (string) $r['message'] : '';
                if ($status === 'success') {
                    return ['via' => 'localapi_updateclientproduct', 'message' => null];
                }
                // Non-success → fall through to raw update.
                $this->rawUpdate($serviceId, $newAmount);
                $this->logFallback($serviceId, 'UpdateClientProduct returned ' . $status . ': ' . $message);
                return ['via' => 'raw_fallback_localapi_non_success', 'message' => $message];
            } catch (\Throwable $e) {
                $this->rawUpdate($serviceId, $newAmount);
                $this->logFallback($serviceId, 'UpdateClientProduct threw: ' . $e->getMessage());
                return ['via' => 'raw_fallback_localapi_threw', 'message' => $e->getMessage()];
            }
        }

        // LocalAPI helper is not loaded (test env or stripped WHMCS).
        $this->rawUpdate($serviceId, $newAmount);
        $this->logFallback($serviceId, 'localAPI() unavailable; raw Capsule path used');
        return ['via' => 'raw_fallback_no_localapi', 'message' => 'localAPI helper unavailable'];
    }

    /**
     * The ONE place that issues a raw Capsule update against
     * `tblhosting.recurringamount`. Lives here so the grep enforcement test
     * has a single line to match.
     */
    private function rawUpdate(int $serviceId, float $newAmount): void
    {
        Capsule::table('tblhosting')
            ->where('id', $serviceId)
            ->update(['recurringamount' => $newAmount]);
    }

    private function logFallback(int $serviceId, string $message): void
    {
        if (function_exists('logActivity')) {
            \logActivity(
                'Contabo Pricing: ServicePriceWriter raw fallback on service '
                . $serviceId . ' — ' . $message
            );
        }
    }
}
