<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class AuditLogger
{
    /**
     * @param array<string,mixed> $metadata
     */
    public function record(
        string $eventType,
        string $outcome,
        int $serviceId,
        string $correlationId,
        array $metadata = [],
        string $actorType = 'system',
        ?int $actorId = null
    ): void {
        $previous = Capsule::table('mod_securiacevps_audit_events')
            ->orderByDesc('id')
            ->first();
        $previous = $previous !== null ? (array) $previous : [];
        $previousHash = (string) ($previous['event_hash'] ?? '');
        $safeMetadata = $this->sanitize($metadata);
        $createdAt = date('Y-m-d H:i:s');
        $event = [
            'event_uuid' => Uuid::v4(),
            'correlation_id' => $correlationId !== '' ? $correlationId : null,
            'actor_type' => $actorType,
            'actor_id' => $actorId,
            'service_id' => $serviceId > 0 ? $serviceId : null,
            'event_type' => $eventType,
            'outcome' => $outcome,
            'previous_event_hash' => $previousHash !== '' ? $previousHash : null,
            'metadata_json' => CanonicalJson::encode($safeMetadata),
            'created_at' => $createdAt,
        ];
        $event['event_hash'] = hash(
            'sha256',
            $previousHash . '|' . CanonicalJson::encode($event)
        );
        Capsule::table('mod_securiacevps_audit_events')->insert($event);
    }

    /** @param mixed $value @return mixed */
    private function sanitize($value)
    {
        if (!is_array($value)) {
            return $value;
        }
        $out = [];
        foreach ($value as $key => $item) {
            $lower = is_string($key) ? strtolower($key) : '';
            if (preg_match('/password|secret|token|authorization|credential|accesshash/', $lower) === 1) {
                $out[$key] = '***REDACTED***';
            } else {
                $out[$key] = is_array($item) ? $this->sanitize($item) : $item;
            }
        }
        return $out;
    }
}
