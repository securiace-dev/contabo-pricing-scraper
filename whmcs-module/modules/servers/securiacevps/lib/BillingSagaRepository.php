<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Evidence ledger for workflows that cross WHMCS billing and Contabo.
 * It never issues refunds or mutates invoices automatically.
 */
final class BillingSagaRepository
{
    /**
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $snapshotPayload
     */
    public function recordProvisioning(array $operation, array $snapshotPayload): void
    {
        $operationUuid = (string) ($operation['operation_uuid'] ?? '');
        if ($operationUuid === '') {
            return;
        }
        $pricing = isset($snapshotPayload['pricing']) && is_array($snapshotPayload['pricing'])
            ? $snapshotPayload['pricing']
            : [];
        $whmcs = isset($snapshotPayload['whmcs']) && is_array($snapshotPayload['whmcs'])
            ? $snapshotPayload['whmcs']
            : [];
        $operationState = (string) ($operation['state'] ?? 'accepted');
        $state = 'provider_pending';
        $compensation = null;
        if ($operationState === 'succeeded') {
            $state = 'completed';
            $compensation = 'not_required';
        } elseif (in_array($operationState, ['manual_review', 'failed_terminal'], true)) {
            $state = 'attention_required';
            $compensation = 'operator_decision_required';
        } elseif ($operationState === 'unknown_outcome') {
            $state = 'provider_outcome_unknown';
            $compensation = 'reconcile_before_compensation';
        }
        $evidence = [
            'operation_state' => $operationState,
            'snapshot_uuid' => (string) ($operation['snapshot_uuid'] ?? ''),
            'safe_error_code' => (string) ($operation['safe_error_code'] ?? ''),
            'unknown_outcome' => !empty($operation['unknown_outcome']),
            'policy' => 'no_automatic_refund_or_duplicate_create',
        ];
        $now = date('Y-m-d H:i:s');
        $existing = Capsule::table('mod_securiacevps_billing_sagas')
            ->where('operation_uuid', $operationUuid)
            ->where('saga_type', 'new_provisioning')
            ->first();
        $values = [
            'service_id' => (int) ($operation['service_id'] ?? 0),
            'state' => $state,
            'invoice_id' => isset($whmcs['invoice_id']) ? (int) $whmcs['invoice_id'] : null,
            'currency' => isset($pricing['currency'])
                ? strtoupper(substr((string) $pricing['currency'], 0, 3))
                : null,
            'amount' => $this->moneyString(
                (string) ($whmcs['total_due'] ?? ($pricing['recurring'] ?? ''))
            ),
            'compensation_state' => $compensation,
            'evidence_json' => CanonicalJson::encode($evidence),
            'completed_at' => $state === 'completed' ? $now : null,
            'updated_at' => $now,
        ];
        if ($existing === null) {
            Capsule::table('mod_securiacevps_billing_sagas')->insert(array_merge(
                [
                    'saga_uuid' => Uuid::v4(),
                    'operation_uuid' => $operationUuid,
                    'saga_type' => 'new_provisioning',
                    'created_at' => $now,
                ],
                $values
            ));
        } else {
            Capsule::table('mod_securiacevps_billing_sagas')
                ->where('id', (int) (((array) $existing)['id'] ?? 0))
                ->update($values);
        }
    }

    private function moneyString(string $value): ?string
    {
        $value = trim($value);
        if ($value === '' || !preg_match('/^-?\d+(?:\.\d{1,6})?$/', $value)) {
            return null;
        }
        return $value;
    }
}
