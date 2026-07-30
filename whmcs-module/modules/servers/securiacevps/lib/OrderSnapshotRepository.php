<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

final class OrderSnapshotRepository
{
    /**
     * @return array{row:array<string,mixed>,payload:array<string,mixed>}
     */
    public function sealedForService(int $serviceId): array
    {
        SchemaGuard::assertReady();
        $row = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        if ($row === null) {
            throw new ContaboProvisioningException(
                'No sealed paid-order snapshot exists for this service; provisioning is blocked'
            );
        }
        $row = (array) $row;
        if (trim((string) ($row['sealed_at'] ?? '')) === '') {
            throw new ContaboProvisioningException('The paid-order snapshot is not sealed');
        }
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            throw new ContaboProvisioningException('The paid-order snapshot payload is invalid');
        }
        foreach (['provider', 'configuration', 'pricing', 'whmcs'] as $key) {
            if (!isset($payload[$key]) || !is_array($payload[$key])) {
                throw new ContaboProvisioningException(
                    'The paid-order snapshot is missing its ' . $key . ' contract'
                );
            }
        }
        if ((string) ($row['configuration_hash'] ?? '') !== hash('sha256', CanonicalJson::encode($payload['configuration']))) {
            throw new ContaboProvisioningException('The paid-order configuration hash does not match its sealed payload');
        }
        if ((string) ($row['price_hash'] ?? '') !== hash('sha256', CanonicalJson::encode($payload['pricing']))) {
            throw new ContaboProvisioningException('The paid-order price hash does not match its sealed payload');
        }
        if ((string) ($row['cart_total_hash'] ?? '') !== hash('sha256', CanonicalJson::encode($payload['whmcs']))) {
            throw new ContaboProvisioningException('The paid-order cart hash does not match its sealed payload');
        }
        return ['row' => $row, 'payload' => $payload];
    }
}
