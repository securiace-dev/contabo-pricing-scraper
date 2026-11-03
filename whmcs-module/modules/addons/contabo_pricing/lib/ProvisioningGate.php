<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * PreModuleCreate guard for native VPS products.
 *
 * New orders are FAIL-CLOSED: a service may only be created from a sealed
 * paid-order snapshot. One narrow, logged exception exists for the upgrade
 * window: a service whose order was placed BEFORE the snapshot contract was
 * enforced, whose invoice is paid, and which has no snapshot row of any state
 * can never obtain a snapshot (the draft/seal flow did not exist when it was
 * ordered). Refusing it would strand a paying customer, so it is allowed with
 * a warning. Everything else aborts, and every abort is logged with a reason.
 *
 * Before refusing, the gate gives a verified draft whose invoice is Paid one
 * chance to seal (REASON_SEALED_AT_GATE). That covers an OrderPaid hook that did
 * not run or ran late, and never relaxes verification: a draft whose hashes or
 * order/invoice/service binding fail is refused, as is any unpaid or cancelled
 * order. Recovery for the remaining refusals is the admin re-quote.
 *
 * The exception can be switched off at any time with
 * mod_contabo_settings.snapshot.grandfather_enabled = 0 once the pre-upgrade
 * backlog is provisioned.
 */
final class ProvisioningGate
{
    public const SETTING_CUTOFF = 'snapshot.grandfather_before';
    public const SETTING_ENABLED = 'snapshot.grandfather_enabled';

    public const REASON_SEALED = 'sealed_snapshot';
    public const REASON_GRANDFATHERED = 'grandfathered_pre_snapshot_paid_order';
    public const REASON_SEALED_AT_GATE = 'sealed_at_gate_paid_invoice';
    public const REASON_NO_SERVICE = 'no_service_id';
    public const REASON_NO_SNAPSHOT = 'no_sealed_snapshot';
    public const REASON_LOOKUP_FAILED = 'snapshot_lookup_failed';

    private const MODULES = ['securiacevps', 'contabo_vps'];

    /**
     * Hook entry point. Returns the WHMCS hook result and logs every abort
     * and every grandfathered allow.
     *
     * @param array<string,mixed> $vars
     * @return array<string,mixed>
     */
    public function handle(array $vars): array
    {
        $params = isset($vars['params']) && is_array($vars['params']) ? $vars['params'] : [];
        $module = (string) ($params['moduletype'] ?? '');
        if (!in_array($module, self::MODULES, true)) {
            return [];
        }
        $serviceId = (int) ($params['serviceid'] ?? 0);

        try {
            $decision = $this->decide($serviceId);
        } catch (\Throwable $e) {
            $decision = [
                'allow' => false,
                'code' => self::REASON_LOOKUP_FAILED,
                'detail' => 'snapshot lookup raised ' . get_class($e) . ': ' . $this->clip($e->getMessage()),
            ];
        }

        if ($decision['allow'] && $decision['code'] === self::REASON_SEALED) {
            return [];
        }

        $line = sprintf(
            'service #%d module %s code %s: %s',
            $serviceId,
            $module,
            $decision['code'],
            $decision['detail']
        );
        if ($decision['allow']) {
            $this->log('WARNING: Contabo provisioning gate allowed ' . $line, $module, $serviceId, 'allow', $decision);
            return [];
        }
        $this->log('Contabo provisioning gate ABORTED module create: ' . $line, $module, $serviceId, 'abort', $decision);
        return ['abortcmd' => true];
    }

    /**
     * @return array{allow:bool, code:string, detail:string}
     */
    public function decide(int $serviceId): array
    {
        if ($serviceId <= 0) {
            return [
                'allow' => false,
                'code' => self::REASON_NO_SERVICE,
                'detail' => 'the create command carried no service id',
            ];
        }

        $sealed = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        if ($sealed !== null) {
            return ['allow' => true, 'code' => self::REASON_SEALED, 'detail' => 'sealed snapshot present'];
        }

        try {
            $healed = (new PaidOrderSnapshotService())->sealPaidService($serviceId);
        } catch (\Throwable $e) {
            $healed = 0;
        }
        if ($healed > 0) {
            $sealedNow = Capsule::table('mod_securiacevps_order_snapshots')
                ->where('service_id', $serviceId)
                ->where('state', 'sealed')
                ->exists();
            if ($sealedNow) {
                return [
                    'allow' => true,
                    'code' => self::REASON_SEALED_AT_GATE,
                    'detail' => 'the invoice is paid and the verified draft was sealed at create time (OrderPaid had not sealed it)',
                ];
            }
        }

        $why = $this->grandfatherRefusal($serviceId);
        if ($why === null) {
            return [
                'allow' => true,
                'code' => self::REASON_GRANDFATHERED,
                'detail' => 'paid order predates snapshot enforcement (cutoff '
                    . $this->setting(self::SETTING_CUTOFF) . ') and has no snapshot row; allowed once with a warning',
            ];
        }
        return [
            'allow' => false,
            'code' => self::REASON_NO_SNAPSHOT,
            'detail' => 'no sealed snapshot for this service and not grandfathered: ' . $why . $this->linkNote($serviceId),
        ];
    }

    /** @return string|null null when the service may be grandfathered, else the reason it may not */
    private function grandfatherRefusal(int $serviceId): ?string
    {
        if ($this->setting(self::SETTING_ENABLED, '1') !== '1') {
            return 'grandfathering is disabled (' . self::SETTING_ENABLED . ' != 1)';
        }
        $cutoff = $this->setting(self::SETTING_CUTOFF);
        $cutoffTs = $cutoff === '' ? false : strtotime($cutoff);
        if ($cutoffTs === false) {
            return 'no cutoff is recorded (' . self::SETTING_CUTOFF . ' empty); apply the upgrade first';
        }

        // Any snapshot row (draft, expired, superseded, invalid) means the order
        // went through the snapshot flow and failed to seal: that must stay closed.
        if (Capsule::table('mod_securiacevps_order_snapshots')->where('service_id', $serviceId)->exists()) {
            return 'the service has a non-sealed snapshot row, so it was ordered under the snapshot flow';
        }

        $service = Capsule::table('tblhosting')->where('id', $serviceId)->first();
        $orderId = $service === null ? 0 : (int) (((array) $service)['orderid'] ?? 0);
        if ($orderId <= 0) {
            return 'the service has no order';
        }
        $order = Capsule::table('tblorders')->where('id', $orderId)->first();
        if ($order === null) {
            return 'order #' . $orderId . ' was not found';
        }
        $order = (array) $order;
        $orderedAt = strtotime((string) ($order['date'] ?? ''));
        if ($orderedAt === false) {
            return 'order #' . $orderId . ' has no usable date';
        }
        if ($orderedAt >= $cutoffTs) {
            return 'order #' . $orderId . ' was placed at or after the cutoff';
        }
        // The order's own invoice, or the invoice linked to it that was
        // actually paid (mass pay, merged, re-invoice).
        $paying = (new PaidOrderSnapshotService())->payingInvoice($orderId, $serviceId);
        if ($paying['invoice_id'] <= 0) {
            return 'order #' . $orderId . ' has no paid invoice'
                . ($paying['refusal'] !== '' ? ' (' . $paying['refusal'] . ')' : '');
        }
        if (in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
            return 'order #' . $orderId . ' is ' . strtolower((string) $order['status']);
        }
        return null;
    }

    private function linkNote(int $serviceId): string
    {
        try {
            $note = (new PaidOrderSnapshotService())->unsealedNote($serviceId);
        } catch (\Throwable $e) {
            return '';
        }
        return $note === '' ? '' : ' [' . $this->clip($note) . ']';
    }

    private function setting(string $key, string $default = ''): string
    {
        $value = Capsule::table('mod_contabo_settings')->where('key', $key)->value('value');
        return $value === null ? $default : trim((string) $value);
    }

    /**
     * Writes the admin activity log and the WHMCS module log. Never throws.
     *
     * @param array{allow:bool, code:string, detail:string} $decision
     */
    private function log(string $message, string $module, int $serviceId, string $outcome, array $decision): void
    {
        try {
            if (function_exists('logActivity')) {
                logActivity($message);
            }
        } catch (\Throwable $e) {
            // logging must never change the gate outcome
        }
        try {
            if (function_exists('logModuleCall')) {
                logModuleCall(
                    $module,
                    'PreModuleCreate',
                    ['serviceid' => $serviceId],
                    ['outcome' => $outcome, 'code' => $decision['code'], 'detail' => $decision['detail']]
                );
            }
        } catch (\Throwable $e) {
            // logging must never change the gate outcome
        }
    }

    private function clip(string $text): string
    {
        $text = preg_replace('/\s+/', ' ', $text) ?? '';
        return strlen($text) > 200 ? substr($text, 0, 200) . '...' : $text;
    }
}
