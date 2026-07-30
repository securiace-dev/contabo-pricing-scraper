<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class CommunicationService
{
    /** @var array<string,string> */
    private const TEMPLATES = [
        'provisioning_ready' => 'SecuriAce VPS Ready',
        'provisioning_delayed' => 'SecuriAce VPS Provisioning Delayed',
        'provisioning_review' => 'SecuriAce VPS Provisioning Review',
        'password_reset_complete' => 'SecuriAce VPS Password Reset Complete',
        'reinstall_complete' => 'SecuriAce VPS Reinstall Complete',
    ];

    /** @param array<string,mixed> $operation */
    public function queueForOperation(array $operation): void
    {
        $type = (string) ($operation['operation_type'] ?? '');
        $state = (string) ($operation['state'] ?? '');
        $messageType = null;
        if ($type === 'create' && $state === 'succeeded') {
            $messageType = 'provisioning_ready';
        } elseif ($type === 'create'
            && in_array($state, ['provider_pending', 'reconciling', 'unknown_outcome'], true)
            && (int) ($operation['attempt_count'] ?? 0) >= 2
        ) {
            $messageType = 'provisioning_delayed';
        } elseif ($type === 'create' && in_array($state, ['manual_review', 'failed_terminal'], true)) {
            $messageType = 'provisioning_review';
        } elseif ($type === 'reset_password' && $state === 'succeeded') {
            $messageType = 'password_reset_complete';
        } elseif ($type === 'reinstall' && $state === 'succeeded') {
            $messageType = 'reinstall_complete';
        }
        if ($messageType === null) {
            return;
        }
        $operationUuid = (string) ($operation['operation_uuid'] ?? '');
        if (Capsule::table('mod_securiacevps_communications')
            ->where('operation_uuid', $operationUuid)
            ->where('message_type', $messageType)
            ->count() > 0
        ) {
            return;
        }
        $payload = [
            'operation_reference' => (string) ($operation['correlation_id'] ?? ''),
            'operation_type' => $type,
            'operation_state' => $state,
        ];
        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_communications')->insert([
            'communication_uuid' => Uuid::v4(),
            'service_id' => (int) ($operation['service_id'] ?? 0),
            'operation_uuid' => $operationUuid !== '' ? $operationUuid : null,
            'message_type' => $messageType,
            'state' => 'pending',
            'template_name' => self::TEMPLATES[$messageType],
            'payload_hash' => hash('sha256', CanonicalJson::encode($payload)),
            'attempt_count' => 0,
            'next_attempt_at' => $now,
            'claim_token' => null,
            'claim_expires_at' => null,
            'sent_at' => null,
            'safe_error_code' => null,
            'created_at' => $now,
            'updated_at' => $now,
        ]);
    }

    public function processQueue(int $limit = 25): void
    {
        $now = date('Y-m-d H:i:s');
        $rows = Capsule::table('mod_securiacevps_communications')
            ->whereIn('state', ['pending', 'retry_scheduled', 'sending'])
            ->orderBy('id')
            ->limit(max(1, min(300, $limit * 3)))
            ->get();
        $processed = 0;
        foreach ($rows as $item) {
            $row = (array) $item;
            $state = (string) ($row['state'] ?? '');
            $dueAt = $state === 'sending'
                ? (string) ($row['claim_expires_at'] ?? '')
                : (string) ($row['next_attempt_at'] ?? '');
            if ($dueAt !== '' && strtotime($dueAt) > time()) {
                continue;
            }
            if ($processed >= max(1, min(100, $limit))) {
                break;
            }
            $id = (int) ($row['id'] ?? 0);
            $claimToken = Uuid::v4();
            $claimExpiresAt = date(
                'Y-m-d H:i:s',
                time() + max(60, (int) SchemaGuard::setting('communication_lease_seconds', '300'))
            );
            $claimQuery = Capsule::table('mod_securiacevps_communications')
                ->where('id', $id)
                ->where('state', $state);
            if ($state === 'sending') {
                $claimQuery->where('claim_token', $row['claim_token'] ?? null)
                    ->where('claim_expires_at', $row['claim_expires_at'] ?? null);
            }
            $claimed = $claimQuery->update([
                'state' => 'sending',
                'claim_token' => $claimToken,
                'claim_expires_at' => $claimExpiresAt,
                'updated_at' => $now,
            ]);
            if ($claimed !== 1) {
                continue;
            }
            $processed++;
            $attempt = ((int) ($row['attempt_count'] ?? 0)) + 1;
            try {
                if (!function_exists('localAPI')) {
                    throw new \RuntimeException('WHMCS LocalAPI is unavailable');
                }
                $operationObject = Capsule::table('mod_securiacevps_operations')
                    ->where('operation_uuid', (string) ($row['operation_uuid'] ?? ''))
                    ->first();
                $operation = $operationObject !== null ? (array) $operationObject : [];
                $customVars = [
                    'operation_reference' => (string) ($operation['correlation_id'] ?? ''),
                ];
                $response = \localAPI('SendEmail', [
                    'messagename' => (string) ($row['template_name'] ?? ''),
                    'id' => (int) ($row['service_id'] ?? 0),
                    'customtype' => 'product',
                    'customvars' => base64_encode(serialize($customVars)),
                ]);
                if ((string) ($response['result'] ?? '') !== 'success') {
                    throw new \RuntimeException('WHMCS SendEmail did not accept the message');
                }
                Capsule::table('mod_securiacevps_communications')
                    ->where('id', $id)
                    ->where('state', 'sending')
                    ->where('claim_token', $claimToken)
                    ->update([
                        'state' => 'sent',
                        'attempt_count' => $attempt,
                        'next_attempt_at' => null,
                        'claim_token' => null,
                        'claim_expires_at' => null,
                        'sent_at' => date('Y-m-d H:i:s'),
                        'safe_error_code' => null,
                        'updated_at' => date('Y-m-d H:i:s'),
                    ]);
            } catch (\Throwable $e) {
                $terminal = $attempt >= 3;
                Capsule::table('mod_securiacevps_communications')
                    ->where('id', $id)
                    ->where('state', 'sending')
                    ->where('claim_token', $claimToken)
                    ->update([
                        'state' => $terminal ? 'failed' : 'retry_scheduled',
                        'attempt_count' => $attempt,
                        'next_attempt_at' => $terminal
                            ? null
                            : date('Y-m-d H:i:s', time() + (60 * $attempt)),
                        'claim_token' => null,
                        'claim_expires_at' => null,
                        'safe_error_code' => 'email_delivery_failed',
                        'updated_at' => date('Y-m-d H:i:s'),
                    ]);
            }
        }
    }
}
