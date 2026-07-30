<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Provider adapter used only by the durable LifecycleOrchestrator. Public
 * submit methods require an operation identity and mutating callbacks never
 * call the provider directly.
 *
 * Safety invariants (see docs/PROVISIONING_CONTRACT.md):
 *   - Create reads only a sealed order snapshot.
 *   - Every mutation carries the durable operation's request identity.
 *   - Every existing-resource mutation verifies the WHMCS ownership tag.
 *   - Legacy direct mutation methods fail closed without making an API call.
 *   - A different stored instance id is never silently overwritten.
 */
final class InstanceService
{
    private const VIEW_TIMEOUT_SEC = 10;

    /** @var ContaboApiClient */ private $client;
    /** @var InstanceLinker */ private $linker;
    /** @var SecretManager */ private $secrets;
    /** @var ContaboInstanceMapper */ private $mapper;

    public function __construct(
        ContaboApiClient $client,
        InstanceLinker $linker,
        SecretManager $secrets,
        ConfigOptionResolver $options,
        ImageResolver $images,
        ContaboInstanceMapper $mapper
    ) {
        $this->client  = $client;
        $this->linker  = $linker;
        $this->secrets = $secrets;
        $this->mapper  = $mapper;
    }

    /**
     * Compatibility fail-safe for integrations that instantiated the old
     * service directly. The canonical WHMCS callback uses LifecycleOrchestrator.
     *
     * @param array<string,mixed> $params
     */
    public function create(array $params): string
    {
        throw $this->directMutationDisabled();
    }

    /**
     * Submit a create request exclusively from the sealed order snapshot.
     * Mutable product and configurable-option rows are deliberately ignored.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $snapshotPayload
     * @param callable|null $beforeMutation Invoked after preflight, immediately
     *        before the customer-resource mutation.
     * @return array<string,mixed>
     */
    public function submitCreateFromSnapshot(
        array $params,
        array $snapshotPayload,
        string $requestIdentity,
        string $operationUuid,
        ?callable $beforeMutation = null
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $productId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));
        $fieldId = $this->linker->ensureCustomField($productId);

        $storedId = $this->linker->readInstanceId($params);
        if ($storedId !== '') {
            $owned = $this->linker->verifyOwnership($this->client, $storedId, $serviceId);
            if ($owned['exists'] && $owned['tagMatches']) {
                return ['instance_id' => $storedId, 'recovered' => true];
            }
            throw new ContaboProvisioningException(
                'The service has an existing provider link that could not be verified',
                'existing_resource_unverified',
                'manual_review'
            );
        }

        $orphan = $this->linker->findByTag($this->client, $serviceId);
        if ($orphan !== null) {
            $orphanId = trim((string) ($orphan['instanceId'] ?? ''));
            if ($orphanId !== '') {
                $this->linker->storeInstanceId($serviceId, $fieldId, $orphanId);
                return ['instance_id' => $orphanId, 'recovered' => true];
            }
        }

        $provider = isset($snapshotPayload['provider']) && is_array($snapshotPayload['provider'])
            ? $snapshotPayload['provider']
            : [];
        $configuration = isset($snapshotPayload['configuration']) && is_array($snapshotPayload['configuration'])
            ? $snapshotPayload['configuration']
            : [];
        $pricing = isset($snapshotPayload['pricing']) && is_array($snapshotPayload['pricing'])
            ? $snapshotPayload['pricing']
            : [];

        $preparedSecret = (new OneTimeSecretStore())->prepareForOperation(
            $serviceId,
            'root_password',
            $operationUuid
        );
        $password = $preparedSecret['plaintext'];
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $password);
        $sealedParams = [
            'serviceid' => $serviceId,
            'pid' => $productId,
            'domain' => (string) ($snapshotPayload['whmcs']['service_label'] ?? ($params['domain'] ?? '')),
            'billingcycle' => (string) ($pricing['billing_cycle'] ?? ($configuration['billing_cycle'] ?? '')),
            'configoption1' => (string) ($provider['image_id'] ?? $configuration['image_id'] ?? ''),
            'configoption2' => (string) ($provider['region_id'] ?? $configuration['region'] ?? ''),
            'configoption3' => (string) ($provider['ssh_secret_id'] ?? ''),
            'configoption4' => (string) ($provider['sku_id'] ?? ''),
            'configoption5' => (string) ($configuration['cloud_init'] ?? ''),
            'configoption6' => isset($provider['add_ons'])
                ? CanonicalJson::encode($provider['add_ons'])
                : '',
        ];
        $body = $this->mapper->mapCreate($sealedParams, [], $secretId);
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $resp = $this->client->postWithIdentity('/v1/compute/instances', $body, $requestIdentity);
        $instanceId = trim((string) ($resp['data'][0]['instanceId'] ?? ''));
        if ($instanceId === '') {
            throw new ContaboProvisioningException(
                'Provider accepted the create request but returned no resource identity',
                'provider_resource_id_missing',
                'manual_review',
                true
            );
        }
        $this->linker->storeInstanceId($serviceId, $fieldId, $instanceId);
        Capsule::table('mod_securiacevps_resources')->updateOrInsert(
            ['service_id' => $serviceId],
            [
                'installation_id' => SchemaGuard::installationId(),
                'provider_account_id' => ProviderAccount::id($params),
                'provider_resource_id' => $instanceId,
                'provider_state' => 'creating',
                'provisioning_state' => 'creating',
                'ownership_state' => 'verified',
                'resource_version' => 1,
                'updated_at' => date('Y-m-d H:i:s'),
            ]
        );
        $this->log('CreateAccount', _securiacevps_sanitize($body), ['instance_id' => $instanceId]);
        return [
            'instance_id' => $instanceId,
            'recovered' => false,
            'secret_uuid' => $preparedSecret['secret_uuid'],
            'reveal_token_ciphertext' => function_exists('encrypt')
                ? encrypt($preparedSecret['reveal_token'])
                : '',
            'secret_expires_at' => $preparedSecret['expires_at'],
        ];
    }

    /**
     * Verify provider identity, ownership, expected configuration and network
     * readiness. A non-ready state is returned, not treated as failure.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $snapshotPayload
     * @return array{ready:bool,state:string,snapshot:array<string,mixed>}
     */
    public function verifyReady(array $params, array $snapshotPayload): array
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return ['ready' => false, 'state' => 'creating', 'snapshot' => []];
        }
        if (!$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        $provider = isset($snapshotPayload['provider']) && is_array($snapshotPayload['provider'])
            ? $snapshotPayload['provider']
            : [];
        $instance = $owned['instance'];
        $expectedRegion = (string) ($provider['region_id'] ?? '');
        $expectedImage = (string) ($provider['image_id'] ?? '');
        if ($expectedRegion !== '' && isset($instance['region'])
            && strcasecmp($expectedRegion, (string) $instance['region']) !== 0
        ) {
            throw new ContaboProvisioningException(
                'Provider resource region does not match the paid order',
                'resource_configuration_mismatch',
                'manual_review'
            );
        }
        if ($expectedImage !== '' && isset($instance['imageId'])
            && (string) $instance['imageId'] !== $expectedImage
        ) {
            throw new ContaboProvisioningException(
                'Provider resource image does not match the paid order',
                'resource_configuration_mismatch',
                'manual_review'
            );
        }
        $snapshot = $this->sync($params);
        $state = strtolower((string) ($snapshot['status'] ?? 'unknown'));
        $ready = $state === 'running' && !empty($snapshot['ipv4']);
        return ['ready' => $ready, 'state' => $state, 'snapshot' => $snapshot];
    }

    /** @param array<string,mixed> $params */
    public function recoverCreateByTag(array $params): ?string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $productId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));
        $found = $this->linker->findByTag($this->client, $serviceId);
        if ($found === null) {
            return null;
        }
        $instanceId = trim((string) ($found['instanceId'] ?? ''));
        if ($instanceId === '') {
            return null;
        }
        $this->linker->storeInstanceId(
            $serviceId,
            $this->linker->ensureCustomField($productId),
            $instanceId
        );
        return $instanceId;
    }

    /** @param array<string,mixed> $params */
    public function submitPowerWithIdentity(
        array $params,
        string $action,
        string $requestIdentity,
        ?callable $beforeMutation = null
    ): void {
        if (!in_array($action, ['start', 'stop', 'restart', 'shutdown'], true)) {
            throw new ContaboProvisioningException('Unsupported power action');
        }
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists'] || !$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->postWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/actions/' . $action,
            [],
            $requestIdentity
        );
    }

    /**
     * Submit a durable password reset without writing plaintext to tblhosting.
     *
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function submitPasswordResetWithIdentity(
        array $params,
        string $requestIdentity,
        string $operationUuid,
        ?callable $beforeMutation = null
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->verifiedInstanceId($params);
        $prepared = (new OneTimeSecretStore())->prepareForOperation(
            $serviceId,
            'root_password',
            $operationUuid
        );
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $prepared['plaintext']);
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->postWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/actions/resetPassword',
            ['rootPassword' => $secretId],
            $requestIdentity
        );
        return $this->safeSecretResult($prepared);
    }

    /**
     * Reinstall only to the image sealed in the paid-order snapshot. Mutable
     * current configurable options are deliberately ignored.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $snapshotPayload
     * @return array<string,mixed>
     */
    public function submitReinstallWithIdentity(
        array $params,
        array $snapshotPayload,
        string $requestIdentity,
        string $operationUuid,
        ?callable $beforeMutation = null
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->verifiedInstanceId($params);
        $provider = isset($snapshotPayload['provider']) && is_array($snapshotPayload['provider'])
            ? $snapshotPayload['provider']
            : [];
        $imageId = trim((string) ($provider['image_id'] ?? ''));
        if ($imageId === '') {
            throw new ContaboProvisioningException(
                'The sealed order snapshot has no provider image identifier',
                'sealed_image_missing',
                'manual_review'
            );
        }
        $prepared = (new OneTimeSecretStore())->prepareForOperation(
            $serviceId,
            'root_password',
            $operationUuid
        );
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $prepared['plaintext']);
        $body = ['imageId' => $imageId, 'rootPassword' => $secretId];
        if (!empty($provider['ssh_secret_id'])) {
            $body['sshKeys'] = [(int) $provider['ssh_secret_id']];
        }
        $configuration = isset($snapshotPayload['configuration'])
            && is_array($snapshotPayload['configuration'])
            ? $snapshotPayload['configuration']
            : [];
        if (!empty($configuration['cloud_init'])) {
            $body['userData'] = (string) $configuration['cloud_init'];
        }
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->putWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId),
            $body,
            $requestIdentity
        );
        return $this->safeSecretResult($prepared);
    }

    /**
     * @param array<string,mixed> $params
     * @return array{exists:bool,state:string,snapshot:array<string,mixed>}
     */
    public function verifyOwnedResource(array $params): array
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return ['exists' => false, 'state' => 'missing', 'snapshot' => []];
        }
        if (!$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        $snapshot = $this->sync($params);
        return [
            'exists' => true,
            'state' => strtolower((string) ($snapshot['status'] ?? 'unknown')),
            'snapshot' => $snapshot,
        ];
    }

    /**
     * Verify a reinstall from provider observations without issuing another
     * mutation. The expected image is read exclusively from the sealed order
     * snapshot carried by the durable operation.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $snapshotPayload
     * @return array{ready:bool,state:string,image:string,snapshot:array<string,mixed>}
     */
    public function verifyReinstall(array $params, array $snapshotPayload): array
    {
        $provider = isset($snapshotPayload['provider']) && is_array($snapshotPayload['provider'])
            ? $snapshotPayload['provider']
            : [];
        $expectedImage = trim((string) ($provider['image_id'] ?? ''));
        if ($expectedImage === '') {
            throw new ContaboProvisioningException(
                'The sealed order snapshot has no provider image identifier',
                'sealed_image_missing',
                'manual_review'
            );
        }
        $observed = $this->verifyOwnedResource($params);
        if (!$observed['exists']) {
            throw new ContaboProvisioningException(
                'The provider resource disappeared during reinstall',
                'resource_missing_during_reinstall',
                'manual_review'
            );
        }
        $snapshot = $observed['snapshot'];
        $image = trim((string) ($snapshot['image'] ?? ''));
        $state = strtolower((string) ($observed['state'] ?? 'unknown'));
        return [
            'ready' => hash_equals($expectedImage, $image) && $state === 'running',
            'state' => $state,
            'image' => $image,
            'snapshot' => $snapshot,
        ];
    }

    /** @param array<string,mixed> $params */
    private function verifiedInstanceId(array $params): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists'] || !$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        return $instanceId;
    }

    /**
     * @param array<string,mixed> $prepared
     * @return array<string,mixed>
     */
    private function safeSecretResult(array $prepared): array
    {
        return [
            'secret_uuid' => $prepared['secret_uuid'] ?? null,
            'reveal_token_ciphertext' => function_exists('encrypt')
                ? encrypt((string) ($prepared['reveal_token'] ?? ''))
                : '',
            'secret_expires_at' => $prepared['expires_at'] ?? null,
        ];
    }

    /** @param array<string,mixed> $params */
    public function submitTerminateWithIdentity(
        array $params,
        string $requestIdentity,
        ?callable $beforeMutation = null
    ): bool {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return false;
        }
        if (!$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->postWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/cancel',
            [],
            $requestIdentity
        );
        return true;
    }

    /** @param array<string,mixed> $params */
    public function verifyAbsent(array $params): bool
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        return !$owned['exists'];
    }

    /** @param array<string,mixed> $params */
    public function cleanupAfterTermination(array $params): void
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $this->secrets->cleanupServiceSecrets($serviceId);
        Capsule::table('mod_securiacevps_snapshot_inventory')
            ->where('service_id', $serviceId)
            ->delete();
        Capsule::table('mod_securiacevps_resources')
            ->where('service_id', $serviceId)
            ->update([
                'provider_state' => 'deleted',
                'provisioning_state' => 'ready',
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
    }

    /** @param array<string,mixed> $params */
    public function terminate(array $params): string
    {
        throw $this->directMutationDisabled();
    }

    /**
     * Persist a complete local projection of the provider's snapshot inventory.
     * Provider data is collected first; the prior projection is replaced only
     * inside one local transaction after all pages have been validated.
     *
     * @param array<string,mixed> $params
     * @return list<array<string,mixed>>
     */
    public function refreshSnapshotsProjection(array $params): array
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->verifiedInstanceId($params);
        $providerAccountId = ProviderAccount::id($params);
        $observedAt = date('Y-m-d H:i:s');
        $rows = [];
        foreach ($this->providerSnapshots($instanceId) as $snapshot) {
            $snapshotId = trim((string) ($snapshot['snapshotId'] ?? ''));
            if ($snapshotId === '') {
                throw new ContaboProvisioningException(
                    'Provider snapshot inventory contained no snapshot identity',
                    'snapshot_identity_missing',
                    'manual_review'
                );
            }
            $snapshotInstanceId = trim((string) ($snapshot['instanceId'] ?? ''));
            if ($snapshotInstanceId !== '' && !hash_equals($instanceId, $snapshotInstanceId)) {
                throw new ContaboProvisioningException(
                    'Provider snapshot inventory did not match the owned server',
                    'snapshot_ownership_mismatch',
                    'manual_review'
                );
            }
            $safe = [
                'snapshot_id' => $snapshotId,
                'name' => substr(trim((string) ($snapshot['name'] ?? '')), 0, 30),
                'description' => substr(trim((string) ($snapshot['description'] ?? '')), 0, 255),
                'image_id' => substr(trim((string) ($snapshot['imageId'] ?? '')), 0, 160),
                'image_name' => substr(trim((string) ($snapshot['imageName'] ?? '')), 0, 191),
                'provider_created_at' => $this->providerTimestamp(
                    (string) ($snapshot['createdDate'] ?? '')
                ),
                'provider_auto_delete_at' => $this->providerTimestamp(
                    (string) ($snapshot['autoDeleteDate'] ?? '')
                ),
            ];
            $rows[] = array_merge($safe, [
                'service_id' => $serviceId,
                'provider_account_id' => $providerAccountId,
                'provider_resource_id' => $instanceId,
                'payload_hash' => hash('sha256', CanonicalJson::encode($safe)),
                'observed_at' => $observedAt,
                'created_at' => $observedAt,
                'updated_at' => $observedAt,
            ]);
        }

        Capsule::connection()->transaction(static function () use ($serviceId, $rows): void {
            Capsule::table('mod_securiacevps_snapshot_inventory')
                ->where('service_id', $serviceId)
                ->delete();
            foreach ($rows as $row) {
                Capsule::table('mod_securiacevps_snapshot_inventory')->insert($row);
            }
        });
        return $rows;
    }

    /**
     * @param array<string,mixed> $params
     * @param array<string,mixed> $payload
     * @return array<string,mixed>
     */
    public function submitSnapshotCreateWithIdentity(
        array $params,
        array $payload,
        string $requestIdentity,
        ?callable $beforeMutation = null
    ): array {
        $instanceId = $this->verifiedInstanceId($params);
        $body = ['name' => (string) ($payload['name'] ?? '')];
        if (trim((string) ($payload['description'] ?? '')) !== '') {
            $body['description'] = (string) $payload['description'];
        }
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        return $this->client->postWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/snapshots',
            $body,
            $requestIdentity
        );
    }

    /** @param array<string,mixed> $params */
    public function submitSnapshotDeleteWithIdentity(
        array $params,
        string $snapshotId,
        string $requestIdentity,
        ?callable $beforeMutation = null
    ): void {
        $instanceId = $this->verifiedInstanceId($params);
        $this->assertSnapshotId($snapshotId);
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->deleteWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId)
                . '/snapshots/' . rawurlencode($snapshotId),
            $requestIdentity
        );
    }

    /** @param array<string,mixed> $params */
    public function submitSnapshotRollbackWithIdentity(
        array $params,
        string $snapshotId,
        string $requestIdentity,
        ?callable $beforeMutation = null
    ): void {
        $instanceId = $this->verifiedInstanceId($params);
        $this->assertSnapshotId($snapshotId);
        if ($beforeMutation !== null) {
            $beforeMutation();
        }
        $this->client->postWithIdentity(
            '/v1/compute/instances/' . rawurlencode($instanceId)
                . '/snapshots/' . rawurlencode($snapshotId) . '/rollback',
            [],
            $requestIdentity
        );
    }

    /**
     * Find the provider audit record for an exact durable request identity.
     *
     * @param array<string,mixed> $params
     * @return array<string,mixed>|null
     */
    public function snapshotAudit(
        array $params,
        string $snapshotId,
        string $requestIdentity
    ): ?array {
        $instanceId = $this->verifiedInstanceId($params);
        $requestId = ContaboApiClient::requestIdForIdentity($requestIdentity);
        $query = http_build_query([
            'page' => 1,
            'size' => 100,
            'instanceId' => $instanceId,
            'requestId' => $requestId,
        ], '', '&', PHP_QUERY_RFC3986);
        $response = $this->client->get('/v1/compute/snapshots/audits?' . $query);
        $rows = isset($response['data']) && is_array($response['data'])
            ? $response['data']
            : [];
        foreach ($rows as $row) {
            if (!is_array($row)
                || strcasecmp((string) ($row['requestId'] ?? ''), $requestId) !== 0
                || (string) ($row['instanceId'] ?? '') !== $instanceId
            ) {
                continue;
            }
            $auditedSnapshotId = trim((string) ($row['snapshotId'] ?? ''));
            if ($snapshotId === '' || hash_equals($snapshotId, $auditedSnapshotId)) {
                return $row;
            }
        }
        return null;
    }

    /** @param array<string,mixed> $params */
    public function resetPassword(array $params): string
    {
        throw $this->directMutationDisabled();
    }

    /** @param array<string,mixed> $params */
    public function reinstall(array $params): string
    {
        throw $this->directMutationDisabled();
    }

    /**
     * @param array<string,mixed> $params
     * @param string $action start|stop|restart|shutdown
     */
    public function powerAction(array $params, string $action): string
    {
        throw $this->directMutationDisabled();
    }

    // ── sync ─────────────────────────────────────────────────────────────────

    /**
     * Pull the live instance state and reconcile WHMCS with it:
     *   - tblhosting.dedicatedip ← primary IPv4, assignedips ← extra IPv4s +
     *     all IPv6s (written only when changed);
     *   - re-assert a drifted displayName tag (stored id is the anchor, so a
     *     rename in the Contabo panel cannot detach the service).
     *
     * Returns a snapshot for the caller's UI.
     *
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function sync(array $params, bool $viewContext = false): array
    {
        if ($viewContext) {
            $this->client->setTimeout(self::VIEW_TIMEOUT_SEC);
        }
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);

        $resp = $this->client->get('/v1/compute/instances/' . rawurlencode($instanceId));
        $inst = isset($resp['data'][0]) && is_array($resp['data'][0]) ? $resp['data'][0] : [];
        if ($inst === []) {
            throw new ContaboProvisioningException('Instance ' . $instanceId . ' not found at Contabo');
        }

        $display = (string) ($inst['displayName'] ?? '');
        if (!InstanceLinker::displayNameMatchesTag($display, $serviceId)) {
            throw new ContaboProvisioningException(
                'Provider resource ownership could not be verified',
                'resource_ownership_mismatch',
                'manual_review'
            );
        }
        $v4 = $this->extractIps($inst, 'v4');
        $v6 = $this->extractIps($inst, 'v6');
        $this->writeServiceIps($serviceId, $v4, $v6);

        return [
            'instance_id' => $instanceId,
            'status'      => (string) ($inst['status'] ?? 'unknown'),
            'region'      => (string) ($inst['region'] ?? ''),
            'image'       => (string) ($inst['imageId'] ?? ''),
            'ipv4'        => $v4,
            'ipv6'        => $v6,
            'created'     => (string) ($inst['createdDate'] ?? ''),
            'display_name'=> $display,
            'synced_at'   => date('Y-m-d H:i:s'),
        ];
    }

    /**
     * Explicit read-only refresh. Ownership is assessed first; only a verified
     * service projection may update local WHMCS network fields.
     *
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function refreshProjection(array $params): array
    {
        $assessment = (new AdoptionService($this->client, $this->linker))->assess($params);
        if ((string) ($assessment['state'] ?? '') !== 'verified') {
            throw new ContaboProvisioningException(
                'Provider ownership is not verified; review the adoption finding',
                'resource_ownership_not_adopted',
                'manual_review'
            );
        }
        $snapshot = $this->sync($params, true);
        $safeObservation = [
            'instance_id' => (string) ($snapshot['instance_id'] ?? ''),
            'status' => strtolower((string) ($snapshot['status'] ?? 'unknown')),
            'region' => (string) ($snapshot['region'] ?? ''),
            'image' => (string) ($snapshot['image'] ?? ''),
            'synced_at' => (string) ($snapshot['synced_at'] ?? ''),
        ];
        Capsule::table('mod_securiacevps_resources')
            ->where('service_id', (int) ($params['serviceid'] ?? 0))
            ->update([
                'provider_state' => $safeObservation['status'],
                'observed_payload_hash' => hash(
                    'sha256',
                    CanonicalJson::encode($safeObservation)
                ),
                'last_observed_at' => (string) $safeObservation['synced_at'],
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
        return $snapshot;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** @param array<string,mixed> $params */
    private function requireInstanceId(array $params): string
    {
        $id = $this->linker->readInstanceId($params);
        if ($id === '') {
            throw new ContaboProvisioningException('Service has no linked Contabo instance — provision it first (or run "Sync from Contabo" after fixing the link)');
        }
        return $id;
    }

    /**
     * @return list<array<string,mixed>>
     */
    private function providerSnapshots(string $instanceId): array
    {
        $page = 1;
        $totalPages = 1;
        $snapshots = [];
        do {
            $query = http_build_query([
                'page' => $page,
                'size' => 100,
                'orderBy' => 'createdDate:desc',
            ], '', '&', PHP_QUERY_RFC3986);
            $response = $this->client->get(
                '/v1/compute/instances/' . rawurlencode($instanceId)
                    . '/snapshots?' . $query
            );
            $data = isset($response['data']) && is_array($response['data'])
                ? $response['data']
                : [];
            foreach ($data as $snapshot) {
                if (is_array($snapshot)) {
                    $snapshots[] = $snapshot;
                }
            }
            $pagination = isset($response['_pagination']) && is_array($response['_pagination'])
                ? $response['_pagination']
                : [];
            $totalPages = max(1, (int) ($pagination['totalPages'] ?? 1));
            $page++;
        } while ($page <= min($totalPages, 100));

        if ($totalPages > 100) {
            throw new ContaboProvisioningException(
                'Provider snapshot inventory exceeded the safe pagination bound',
                'snapshot_inventory_too_large',
                'manual_review'
            );
        }
        return $snapshots;
    }

    private function providerTimestamp(string $value): ?string
    {
        if (trim($value) === '') {
            return null;
        }
        $timestamp = strtotime($value);
        return $timestamp === false ? null : gmdate('Y-m-d H:i:s', $timestamp);
    }

    private function assertSnapshotId(string $snapshotId): void
    {
        if ($snapshotId === ''
            || strlen($snapshotId) > 160
            || preg_match('/^[A-Za-z0-9._:-]+$/', $snapshotId) !== 1
        ) {
            throw new ContaboProvisioningException(
                'The provider snapshot identity is invalid',
                'snapshot_identity_invalid',
                'terminal'
            );
        }
    }

    /**
     * Extract the IPs of one family ('v4' or 'v6') from an instance's ipConfig.
     * Contabo has used both a single object and a list of objects under each
     * family key, so both shapes are handled.
     *
     * @param array<string,mixed> $inst
     * @return list<string>
     */
    private function extractIps(array $inst, string $family): array
    {
        $ips = [];
        if (isset($inst['ipConfig'][$family])) {
            $entries = $inst['ipConfig'][$family];
            if (isset($entries['ip'])) {
                $entries = [$entries];
            }
            if (is_array($entries)) {
                foreach ($entries as $entry) {
                    $ip = is_array($entry) ? trim((string) ($entry['ip'] ?? '')) : '';
                    if ($ip !== '') {
                        $ips[] = $ip;
                    }
                }
            }
        }
        return $ips;
    }

    /**
     * Reconcile WHMCS with the instance's IPs: dedicatedip = primary IPv4, and
     * assignedips = any additional IPv4s followed by all IPv6s (newline-joined),
     * so a dual-stack VPS surfaces its v6 addresses too. Writes only on change.
     *
     * @param list<string> $v4
     * @param list<string> $v6
     */
    private function writeServiceIps(int $serviceId, array $v4, array $v6): void
    {
        if ($serviceId <= 0 || ($v4 === [] && $v6 === [])) {
            return;
        }
        try {
            $row = Capsule::table('tblhosting')->where('id', $serviceId)->first();
            $row = $row !== null ? (array) $row : null;
            if ($row === null) {
                return;
            }
            $dedicated = $v4 !== [] ? $v4[0] : '';
            $assigned  = implode("\n", array_merge(array_slice($v4, 1), $v6));
            $update = [];
            if ($dedicated !== '' && (string) ($row['dedicatedip'] ?? '') !== $dedicated) {
                $update['dedicatedip'] = $dedicated;
            }
            if ((string) ($row['assignedips'] ?? '') !== $assigned) {
                $update['assignedips'] = $assigned;
            }
            if ($update !== []) {
                Capsule::table('tblhosting')->where('id', $serviceId)->update($update);
            }
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo VPS: could not write IPs for service #' . $serviceId . ' — ' . $e->getMessage());
            }
        }
    }

    private function directMutationDisabled(): ContaboProvisioningException
    {
        return new ContaboProvisioningException(
            'Direct provider mutation is disabled; use the durable lifecycle operation',
            'direct_mutation_bypass_disabled',
            'manual_review'
        );
    }

    /**
     * @param mixed $request
     * @param mixed $response
     */
    private function log(string $action, $request, $response, string $status = 'success'): void
    {
        _securiacevps_log($action, $request, $response, $status);
    }
}
