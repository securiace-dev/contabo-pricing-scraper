<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Builds the customer service page entirely from WHMCS-owned projections.
 * Rendering never calls Contabo; explicit Refresh is the only read path.
 */
final class ClientAreaPresenter
{
    /** @var CapabilityRegistry */
    private $capabilities;

    public function __construct(?CapabilityRegistry $capabilities = null)
    {
        $this->capabilities = $capabilities !== null ? $capabilities : new CapabilityRegistry();
    }

    /**
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function present(array $params): array
    {
        SchemaGuard::assertReady();
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $hostingObject = Capsule::table('tblhosting')->where('id', $serviceId)->first();
        $resourceObject = Capsule::table('mod_securiacevps_resources')
            ->where('service_id', $serviceId)
            ->first();
        $adoptionObject = Capsule::table('mod_securiacevps_adoption')
            ->where('service_id', $serviceId)
            ->first();
        $operationObject = Capsule::table('mod_securiacevps_operations')
            ->where('service_id', $serviceId)
            ->orderByDesc('id')
            ->first();
        $snapshotObject = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        $hosting = $hostingObject !== null ? (array) $hostingObject : [];
        $resource = $resourceObject !== null ? (array) $resourceObject : [];
        $adoption = $adoptionObject !== null ? (array) $adoptionObject : [];
        $operation = $operationObject !== null ? (array) $operationObject : [];
        $snapshotRow = $snapshotObject !== null ? (array) $snapshotObject : [];
        $snapshot = json_decode((string) ($snapshotRow['payload_json'] ?? '{}'), true);
        $snapshot = is_array($snapshot) ? $snapshot : [];
        $provider = isset($snapshot['provider']) && is_array($snapshot['provider'])
            ? $snapshot['provider']
            : [];
        $providerAccountId = (string) (
            $resource['provider_account_id'] ?? ProviderAccount::id($params)
        );
        $verified = (string) ($resource['ownership_state'] ?? '') === 'verified'
            && (string) ($adoption['state'] ?? '') === 'verified';
        $operationState = (string) ($operation['state'] ?? '');
        $busy = in_array($operationState, [
            'accepted',
            'claimed',
            'submitted',
            'provider_pending',
            'reconciling',
            'retry_scheduled',
            'failed_retryable',
            'unknown_outcome',
        ], true);
        $status = strtolower((string) ($resource['provider_state'] ?? 'unknown'));
        $actions = [];
        if ($verified && !$busy) {
            if ($status === 'stopped' && $this->canWrite($providerAccountId, 'start')) {
                $actions['start'] = ['label' => 'Start server', 'tone' => 'primary'];
            }
            if ($status === 'running' && $this->canWrite($providerAccountId, 'stop')) {
                $actions['stop'] = ['label' => 'Stop server', 'tone' => 'secondary'];
            }
            if ($status === 'running' && $this->canWrite($providerAccountId, 'restart')) {
                $actions['restart'] = ['label' => 'Restart server', 'tone' => 'secondary'];
            }
            if ($this->canWrite($providerAccountId, 'password_reset')) {
                $actions['reset_password'] = [
                    'label' => 'Reset root password',
                    'tone' => 'warning',
                    'confirmation' => 'RESET PASSWORD',
                ];
            }
            if ($this->canWrite($providerAccountId, 'reinstall')) {
                $actions['reinstall'] = [
                    'label' => 'Reinstall server',
                    'tone' => 'danger',
                    'confirmation' => 'REINSTALL',
                ];
            }
        }
        $ips = $this->serviceIps($hosting);
        $credential = (new OneTimeSecretStore())->availableForService($serviceId);
        $snapshotListCertified = $this->capabilities->canRead(
            $providerAccountId,
            'snapshot_list'
        );
        $snapshots = [];
        if ($snapshotListCertified) {
            $snapshotRows = Capsule::table('mod_securiacevps_snapshot_inventory')
                ->where('service_id', $serviceId)
                ->where('provider_account_id', $providerAccountId)
                ->where(
                    'provider_resource_id',
                    (string) ($resource['provider_resource_id'] ?? '')
                )
                ->orderByDesc('provider_created_at')
                ->get();
            foreach ($snapshotRows as $snapshotRow) {
                $snapshots[] = (array) $snapshotRow;
            }
        }
        return [
            'service_id' => $serviceId,
            'instance_id' => (string) ($resource['provider_resource_id'] ?? ''),
            'status' => $status,
            'provisioning_state' => (string) ($resource['provisioning_state'] ?? 'not_requested'),
            'ownership_state' => (string) ($adoption['state'] ?? 'unassessed'),
            'verified_ownership' => $verified,
            'region' => (string) ($provider['region_id'] ?? ''),
            'image' => (string) ($provider['image_id'] ?? ''),
            'ipv4' => $ips['ipv4'],
            'ipv6' => $ips['ipv6'],
            'synced_at' => (string) ($resource['last_observed_at'] ?? ''),
            'operation' => $operation,
            'busy' => $busy,
            'actions' => $actions,
            'credential' => $credential,
            'writes_enabled' => SchemaGuard::setting('provider_writes_enabled', '0') === '1',
            'snapshots' => $snapshots,
            'snapshot_list_certified' => $snapshotListCertified,
            'snapshot_actions' => [
                'create' => $verified && !$busy
                    && $snapshotListCertified
                    && $this->canWrite($providerAccountId, 'snapshot_create'),
                'delete' => $verified && !$busy
                    && $snapshotListCertified
                    && $this->canWrite($providerAccountId, 'snapshot_delete'),
                'rollback' => $verified && !$busy
                    && $snapshotListCertified
                    && $this->canWrite($providerAccountId, 'snapshot_rollback'),
            ],
        ];
    }

    private function canWrite(string $providerAccountId, string $capability): bool
    {
        try {
            $this->capabilities->assertWriteAllowed($providerAccountId, $capability);
            return true;
        } catch (\Throwable $e) {
            return false;
        }
    }

    /**
     * @param array<string,mixed> $hosting
     * @return array{ipv4:list<string>,ipv6:list<string>}
     */
    private function serviceIps(array $hosting): array
    {
        $values = [];
        $dedicated = trim((string) ($hosting['dedicatedip'] ?? ''));
        if ($dedicated !== '') {
            $values[] = $dedicated;
        }
        $assigned = preg_split('/[\s,]+/', trim((string) ($hosting['assignedips'] ?? ''))) ?: [];
        foreach ($assigned as $ip) {
            $ip = trim((string) $ip);
            if ($ip !== '') {
                $values[] = $ip;
            }
        }
        $values = array_values(array_unique($values));
        $ipv4 = [];
        $ipv6 = [];
        foreach ($values as $ip) {
            if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6)) {
                $ipv6[] = $ip;
            } elseif (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)) {
                $ipv4[] = $ip;
            }
        }
        return ['ipv4' => $ipv4, 'ipv6' => $ipv6];
    }
}
