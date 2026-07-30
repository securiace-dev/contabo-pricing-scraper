<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OwnershipGuard
{
    public static function assertVerified(int $serviceId): void
    {
        $resource = Capsule::table('mod_securiacevps_resources')
            ->where('service_id', $serviceId)
            ->first();
        $adoption = Capsule::table('mod_securiacevps_adoption')
            ->where('service_id', $serviceId)
            ->first();
        $resource = $resource !== null ? (array) $resource : [];
        $adoption = $adoption !== null ? (array) $adoption : [];
        if ((string) ($resource['ownership_state'] ?? '') !== 'verified'
            || (string) ($adoption['state'] ?? '') !== 'verified'
            || trim((string) ($resource['provider_resource_id'] ?? '')) === ''
        ) {
            throw new ContaboProvisioningException(
                'Provider resource ownership requires administrator verification',
                'resource_ownership_not_adopted',
                'manual_review'
            );
        }
    }
}
