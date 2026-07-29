<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Orchestrates every WHMCS lifecycle action against the Contabo API. The
 * entry-file functions in securiacevps.php are thin wrappers around this class
 * (built via Runtime) so the whole flow is unit-testable.
 *
 * Safety invariants (see docs/PROVISIONING_CONTRACT.md):
 *   - create() is IDEMPOTENT: a WHMCS retry can never double-provision.
 *   - Instance-id storage is guaranteed BEFORE the create API call.
 *   - Destructive actions (terminate, reset password) require the instance's
 *     displayName tag to match the service; power actions require existence.
 *   - A different stored instance id is never silently overwritten.
 */
final class InstanceService
{
    private const VIEW_TIMEOUT_SEC = 10;

    /** @var ContaboApiClient */ private $client;
    /** @var InstanceLinker */ private $linker;
    /** @var SecretManager */ private $secrets;
    /** @var ConfigOptionResolver */ private $options;
    /** @var ImageResolver */ private $images;
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
        $this->options = $options;
        $this->images  = $images;
        $this->mapper  = $mapper;
    }

    // ── create ───────────────────────────────────────────────────────────────

    /**
     * @param array<string,mixed> $params
     */
    public function create(array $params): string
    {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $productId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));

        // 1. Guarantee we can store the instance id BEFORE creating anything —
        //    an instance we cannot link is an orphan we can never manage.
        $fieldId = $this->linker->ensureCustomField($productId);

        // 2. Idempotency: an already-linked service is a retry, not an order.
        $storedId = $this->linker->readInstanceId($params);
        if ($storedId !== '') {
            $owned = $this->linker->verifyOwnership($this->client, $storedId, $serviceId);
            if ($owned['exists'] && $owned['tagMatches']) {
                $this->log('CreateAccount', $storedId, 'already provisioned — idempotent no-op');
                return 'success';
            }
            if ($owned['exists']) {
                return 'Service already linked to Contabo instance ' . $storedId
                    . ' but its display name no longer carries the tag "' . InstanceLinker::tag($serviceId)
                    . '" — run "Sync from Contabo" to verify and restore the link before retrying.';
            }
            return 'Service is linked to Contabo instance ' . $storedId
                . ' which no longer exists at Contabo. Clear the "contabo_instance_id" custom field deliberately, then retry.';
        }

        // 3. Crash recovery: an instance may exist from a previous attempt that
        //    died between the API call and the DB write. Adopt an EXACT single
        //    tag match; never guess between multiple.
        $orphan = $this->linker->findByTag($this->client, $serviceId);
        if ($orphan !== null) {
            $orphanId = (string) ($orphan['instanceId'] ?? '');
            if ($orphanId !== '') {
                $this->linker->storeInstanceId($serviceId, $fieldId, $orphanId);
                $this->log('CreateAccount', $orphanId, 'adopted existing tagged instance (recovered from an interrupted create)');
                if (function_exists('logActivity')) {
                    logActivity('Contabo VPS: service #' . $serviceId . ' re-linked to existing instance ' . $orphanId . ' found by tag.');
                }
                return 'success';
            }
        }

        // 4. Fresh provision. Root password: WHMCS's generated service password
        //    when present, else our own — pushed to the vault so the password
        //    WHMCS shows the customer actually works on the server.
        $password  = (string) ($params['password'] ?? '');
        $generated = false;
        if ($password === '') {
            $password  = SecretManager::generatePassword();
            $generated = true;
        }
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $password);

        $resolved = $this->resolveSelections($params);
        $body     = $this->mapper->mapCreate($params, $resolved, $secretId);

        $resp = $this->client->post('/v1/compute/instances', $body);
        $instanceId = (string) ($resp['data'][0]['instanceId'] ?? '');
        if ($instanceId === '') {
            throw new ContaboProvisioningException('API returned no instanceId');
        }

        $this->linker->storeInstanceId($serviceId, $fieldId, $instanceId);
        if ($generated) {
            $this->persistServicePassword($serviceId, $password);
        }
        $this->log('CreateAccount', $body, $resp);

        // Build is async on Contabo's side; sync surfaces (admin tab, client
        // area, daily cron) backfill the IP as soon as it is assigned.
        return 'success';
    }

    /**
     * Submit a create request exclusively from the sealed order snapshot.
     * Mutable product and configurable-option rows are deliberately ignored.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $snapshotPayload
     * @return array<string,mixed>
     */
    public function submitCreateFromSnapshot(
        array $params,
        array $snapshotPayload,
        string $requestIdentity,
        string $operationUuid
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
    public function submitPowerWithIdentity(array $params, string $action, string $requestIdentity): void
    {
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
        string $operationUuid
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->verifiedInstanceId($params);
        $prepared = (new OneTimeSecretStore())->prepareForOperation(
            $serviceId,
            'root_password',
            $operationUuid
        );
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $prepared['plaintext']);
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
        string $operationUuid
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
    public function submitTerminateWithIdentity(array $params, string $requestIdentity): bool
    {
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
        Capsule::table('mod_securiacevps_resources')
            ->where('service_id', $serviceId)
            ->update([
                'provider_state' => 'deleted',
                'provisioning_state' => 'ready',
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
    }

    /**
     * Map customer selections (already round-tripped to Contabo labels by
     * ConfigOptionResolver) onto API values. Image and Region resolve
     * fail-closed; dimensions without a create-API mapping are acknowledged in
     * the activity log — never silently dropped, never blocking.
     *
     * @param array<string,mixed> $params
     * @return array{imageId?:string, region?:string, addOns?:array<string,mixed>}
     */
    private function resolveSelections(array $params): array
    {
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $selections = $this->options->selectionsForService($serviceId);
        $resolved   = [];

        foreach ($selections as $sel) {
            $dim = $sel['dimension_key'];
            $value = $sel['value_key'] !== '' ? $sel['value_key'] : $sel['label'];
            if ($dim === 'Image') {
                $resolved['imageId'] = $this->images->resolveImageId($value);
            } elseif ($dim === 'Region') {
                $resolved['region'] = ContaboInstanceMapper::resolveRegionSlug($value);
            } elseif ($dim === 'Networking:Private Networking' || $dim === 'Private Networking') {
                if (stripos($value, 'enable') !== false || stripos($value, 'yes') !== false) {
                    if (!isset($resolved['addOns']) || !is_array($resolved['addOns'])) {
                        $resolved['addOns'] = [];
                    }
                    $resolved['addOns']['privateNetworking'] = new \stdClass();
                }
            } else {
                // v1 boundary: no clean create-API mapping (IPv4 qty, Data
                // Protection, Storage Type, …). Acknowledge loudly.
                if (function_exists('logActivity')) {
                    logActivity('Contabo VPS: service #' . $serviceId . ' selection "' . $dim . ' = ' . $sel['label']
                        . '" has no automated Contabo mapping — apply it manually in the Contabo panel if required.');
                }
            }
        }
        return $resolved;
    }

    // ── lifecycle ────────────────────────────────────────────────────────────

    /** @param array<string,mixed> $params */
    public function terminate(array $params): string
    {
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);

        // Destructive: the Contabo-side tag must confirm this is our instance.
        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            // Already gone at Contabo — treat as terminated, keep the audit trail.
            $this->log('TerminateAccount', $instanceId, 'instance already absent at Contabo');
            $this->secrets->cleanupServiceSecrets($serviceId);
            return 'success';
        }
        if (!$owned['tagMatches']) {
            return 'Refusing to cancel Contabo instance ' . $instanceId . ': its display name does not carry the tag "'
                . InstanceLinker::tag($serviceId) . '". Run "Sync from Contabo" to verify the link first.';
        }

        $resp = $this->client->post('/v1/compute/instances/' . rawurlencode($instanceId) . '/cancel', []);
        $cancelDate = (string) ($resp['data'][0]['cancelDate'] ?? '');
        $this->log('TerminateAccount', $instanceId, $resp);
        if (function_exists('logActivity')) {
            logActivity('Contabo VPS: instance ' . $instanceId . ' (service #' . $serviceId . ') cancelled'
                . ($cancelDate !== '' ? ' — Contabo cancel date ' . $cancelDate : '') . '.');
        }
        $this->secrets->cleanupServiceSecrets($serviceId);
        // The custom-field value is kept for audit (recoverable-by-design).
        return 'success';
    }

    /**
     * Reset the instance's root password to a fresh generated one, via a vault
     * secret, and persist it as the WHMCS service password so the customer
     * sees a password that actually works.
     *
     * @param array<string,mixed> $params
     */
    public function resetPassword(array $params): string
    {
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);

        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return 'Contabo instance ' . $instanceId . ' no longer exists';
        }
        if (!$owned['tagMatches']) {
            return 'Refusing to reset the password on instance ' . $instanceId . ': its display name does not carry the tag "'
                . InstanceLinker::tag($serviceId) . '". Run "Sync from Contabo" to verify the link first.';
        }

        $password = SecretManager::generatePassword();
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $password);
        $resp = $this->client->post(
            '/v1/compute/instances/' . rawurlencode($instanceId) . '/actions/resetPassword',
            ['rootPassword' => $secretId]
        );
        // Only persist after the API accepted the action.
        $this->persistServicePassword($serviceId, $password);
        $this->log('ResetPassword', $instanceId, $resp);
        return 'success';
    }

    /**
     * Rebuild the instance's OS via the reinstall endpoint (PUT), to the
     * product/selection image, with a fresh vaulted root password. Destructive
     * (wipes the disk), so it requires the displayName tag to match.
     *
     * @param array<string,mixed> $params
     */
    public function reinstall(array $params): string
    {
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);

        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return 'Contabo instance ' . $instanceId . ' no longer exists';
        }
        if (!$owned['tagMatches']) {
            return 'Refusing to reinstall instance ' . $instanceId . ': its display name does not carry the tag "'
                . InstanceLinker::tag($serviceId) . '". Run "Sync from Contabo" to verify the link first.';
        }

        // Image: the customer's selection wins, else the product's config option.
        $resolved = $this->resolveSelections($params);
        $imageId  = isset($resolved['imageId']) ? (string) $resolved['imageId'] : trim((string) ($params['configoption1'] ?? ''));
        if ($imageId === '') {
            return 'Cannot reinstall: no image resolved from the product or the customer selection';
        }

        $password = SecretManager::generatePassword();
        $secretId = $this->secrets->ensureRootPasswordSecret($serviceId, $password);
        $body = ['imageId' => $imageId, 'rootPassword' => $secretId];

        $secretOpt = trim((string) ($params['configoption3'] ?? ''));
        if ($secretOpt !== '' && ctype_digit($secretOpt)) {
            $body['sshKeys'] = [(int) $secretOpt];
        }
        $userData = trim((string) ($params['configoption5'] ?? ''));
        if ($userData !== '') {
            $body['userData'] = $userData;
        }

        $resp = $this->client->put('/v1/compute/instances/' . rawurlencode($instanceId), $body);
        // The rebuild resets the root password to the one we just vaulted.
        $this->persistServicePassword($serviceId, $password);
        $this->log('Reinstall', $instanceId, $resp);
        return 'success';
    }

    /**
     * @param array<string,mixed> $params
     * @param string $action start|stop|restart|shutdown
     */
    public function powerAction(array $params, string $action): string
    {
        if (!in_array($action, ['start', 'stop', 'restart', 'shutdown'], true)) {
            return 'Unsupported power action "' . $action . '"';
        }
        $serviceId  = (int) ($params['serviceid'] ?? 0);
        $instanceId = $this->requireInstanceId($params);

        $owned = $this->linker->verifyOwnership($this->client, $instanceId, $serviceId);
        if (!$owned['exists']) {
            return 'Contabo instance ' . $instanceId . ' no longer exists';
        }
        if (!$owned['tagMatches'] && function_exists('logActivity')) {
            // Recoverable action — warn but don't strand power control.
            logActivity('Contabo VPS: instance ' . $instanceId . ' display name lost its "'
                . InstanceLinker::tag($serviceId) . '" tag; power action proceeding — run "Sync from Contabo" to restore it.');
        }

        $resp = $this->client->post('/v1/compute/instances/' . rawurlencode($instanceId) . '/actions/' . $action, []);
        $this->log('PowerAction:' . $action, $instanceId, $resp);
        return 'success';
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

        $v4 = $this->extractIps($inst, 'v4');
        $v6 = $this->extractIps($inst, 'v6');
        $this->writeServiceIps($serviceId, $v4, $v6);

        $display = (string) ($inst['displayName'] ?? '');
        if (!InstanceLinker::displayNameMatchesTag($display, $serviceId)) {
            // The stored id is authoritative here (it was written under the
            // no-silent-overwrite policy); restore the Contabo-side tag.
            try {
                $this->client->patch(
                    '/v1/compute/instances/' . rawurlencode($instanceId),
                    ['displayName' => InstanceLinker::displayName($serviceId, (string) ($params['domain'] ?? ''))]
                );
                if (function_exists('logActivity')) {
                    logActivity('Contabo VPS: restored the "' . InstanceLinker::tag($serviceId) . '" tag on instance ' . $instanceId . '.');
                }
            } catch (\Throwable $e) {
                if (function_exists('logActivity')) {
                    logActivity('Contabo VPS: could not restore the display-name tag on instance ' . $instanceId . ' — ' . $e->getMessage());
                }
            }
        }

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

    private function persistServicePassword(int $serviceId, string $password): void
    {
        if ($serviceId <= 0 || !function_exists('encrypt')) {
            return;
        }
        try {
            Capsule::table('tblhosting')->where('id', $serviceId)->update([
                'password' => encrypt($password),
            ]);
        } catch (\Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo VPS: could not persist the service password for service #' . $serviceId . ' — ' . $e->getMessage());
            }
        }
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
