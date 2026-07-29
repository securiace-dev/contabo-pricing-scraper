<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Read-only provider discovery and explicit existing-service adoption.
 * assess() never changes Contabo. approveCandidate() performs only local WHMCS
 * linkage after repeating the live ownership read.
 */
final class AdoptionService
{
    private const INVENTORY_PAGE_SIZE = 100;
    private const INVENTORY_MAX_PAGES = 20;

    /** @var ContaboApiClient */
    private $client;
    /** @var InstanceLinker */
    private $linker;
    /** @var AuditLogger */
    private $audit;

    public function __construct(
        ContaboApiClient $client,
        ?InstanceLinker $linker = null,
        ?AuditLogger $audit = null
    ) {
        $this->client = $client;
        $this->linker = $linker !== null ? $linker : new InstanceLinker();
        $this->audit = $audit !== null ? $audit : new AuditLogger();
    }

    /**
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function assess(array $params): array
    {
        SchemaGuard::assertReady();
        $serviceId = (int) ($params['serviceid'] ?? 0);
        if ($serviceId <= 0) {
            throw new ContaboProvisioningException('A WHMCS service id is required');
        }
        $providerAccountId = ProviderAccount::id($params);
        $linkedId = $this->linker->readInstanceId($params);
        $candidate = [];
        $state = 'missing_upstream';
        $confidence = '0.0000';
        $source = $linkedId !== '' ? 'whmcs_custom_field' : 'provider_tag_search';

        if ($linkedId !== '') {
            $owned = $this->linker->verifyOwnership($this->client, $linkedId, $serviceId);
            $candidate = is_array($owned['instance'] ?? null) ? $owned['instance'] : [];
            if (!$owned['exists']) {
                $state = 'missing_upstream';
            } elseif (!$owned['tagMatches']) {
                $state = 'conflict';
            } else {
                $state = 'verified';
                $confidence = '1.0000';
            }
        } else {
            try {
                $found = $this->linker->findByTag($this->client, $serviceId);
                if ($found !== null) {
                    $candidate = $found;
                    $linkedId = trim((string) ($found['instanceId'] ?? ''));
                    $state = $linkedId !== '' ? 'probable' : 'ambiguous';
                    $confidence = $linkedId !== '' ? '0.9000' : '0.0000';
                }
            } catch (ContaboProvisioningException $e) {
                $state = $e->safeCode() === 'duplicate_service_tag' ? 'ambiguous' : 'conflict';
            }
        }

        $evidence = [
            'source' => $source,
            'provider_resource_id' => $linkedId !== '' ? $linkedId : null,
            'tag_matches' => $candidate !== []
                ? InstanceLinker::displayNameMatchesTag(
                    (string) ($candidate['displayName'] ?? ''),
                    $serviceId
                )
                : false,
            'provider_state' => strtolower((string) ($candidate['status'] ?? 'unknown')),
            'provider_region' => (string) ($candidate['region'] ?? ''),
            'provider_image' => (string) ($candidate['imageId'] ?? ''),
            'observed_at' => date('Y-m-d H:i:s'),
        ];
        $evidenceJson = CanonicalJson::encode($evidence);
        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_adoption')->updateOrInsert(
            ['service_id' => $serviceId],
            [
                'provider_account_id' => $providerAccountId,
                'provider_resource_id' => $linkedId !== '' ? $linkedId : null,
                'state' => $state,
                'confidence' => $confidence,
                'evidence_json' => $evidenceJson,
                'updated_at' => $now,
                'created_at' => $now,
            ]
        );
        Capsule::table('mod_securiacevps_resources')->updateOrInsert(
            ['service_id' => $serviceId],
            [
                'installation_id' => SchemaGuard::installationId(),
                'provider_account_id' => $providerAccountId,
                'provider_resource_id' => $linkedId !== '' ? $linkedId : null,
                'provider_state' => (string) $evidence['provider_state'],
                'provisioning_state' => $state === 'verified' ? 'ready' : 'manual_review',
                'ownership_state' => $state === 'verified' ? 'verified' : $state,
                'resource_version' => 1,
                'observed_payload_hash' => hash('sha256', $evidenceJson),
                'last_observed_at' => $now,
                'updated_at' => $now,
                'created_at' => $now,
            ]
        );
        $this->recordFinding($serviceId, $providerAccountId, $linkedId, $state, $evidenceJson);
        return [
            'service_id' => $serviceId,
            'provider_account_id' => $providerAccountId,
            'provider_resource_id' => $linkedId,
            'state' => $state,
            'confidence' => $confidence,
            'evidence_hash' => hash('sha256', $evidenceJson),
            'evidence' => $evidence,
        ];
    }

    /**
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function approveCandidate(
        array $params,
        string $providerResourceId,
        string $expectedEvidenceHash,
        int $adminId
    ): array {
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $currentObject = Capsule::table('mod_securiacevps_adoption')
            ->where('service_id', $serviceId)
            ->first();
        $current = $currentObject !== null ? (array) $currentObject : [];
        $currentEvidence = (string) ($current['evidence_json'] ?? '');
        if ($adminId <= 0
            || (string) ($current['state'] ?? '') !== 'probable'
            || !hash_equals(trim($providerResourceId), (string) ($current['provider_resource_id'] ?? ''))
            || !hash_equals($expectedEvidenceHash, hash('sha256', $currentEvidence))
        ) {
            throw new ContaboProvisioningException(
                'The adoption candidate changed and must be reviewed again',
                'adoption_candidate_stale',
                'manual_review'
            );
        }
        $owned = $this->linker->verifyOwnership(
            $this->client,
            trim($providerResourceId),
            $serviceId
        );
        if (!$owned['exists'] || !$owned['tagMatches']) {
            throw new ContaboProvisioningException(
                'The adoption candidate no longer proves service ownership',
                'adoption_candidate_unverified',
                'manual_review'
            );
        }
        $productId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));
        $fieldId = $this->linker->ensureCustomField($productId);
        $this->linker->storeInstanceId($serviceId, $fieldId, trim($providerResourceId));
        $assessment = $this->assess($params);
        if ((string) ($assessment['state'] ?? '') !== 'verified') {
            throw new ContaboProvisioningException(
                'The approved adoption did not verify after linkage',
                'adoption_verification_failed',
                'manual_review'
            );
        }
        Capsule::table('mod_securiacevps_adoption')
            ->where('service_id', $serviceId)
            ->update([
                'reviewed_by_admin_id' => $adminId,
                'reviewed_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
        $this->audit->record(
            'service.adopted',
            'verified',
            $serviceId,
            '',
            ['provider_resource_id' => trim($providerResourceId)],
            'admin',
            $adminId
        );
        return $assessment;
    }

    /**
     * Read-only, bounded provider-account inventory. Only resources carrying a
     * SecuriAce `whmcs-{serviceId}` ownership tag are considered managed
     * candidates; unrelated Contabo resources are intentionally ignored.
     *
     * @param array<string,mixed> $params
     * @return array{observed:int,managed:int,orphans:int,truncated:bool}
     */
    public function inventoryProviderAccount(array $params): array
    {
        SchemaGuard::assertReady();
        $providerAccountId = ProviderAccount::id($params);
        $localRows = Capsule::table('mod_securiacevps_resources')
            ->where('provider_account_id', $providerAccountId)
            ->get();
        $localByResource = [];
        foreach ($localRows as $item) {
            $row = (array) $item;
            $resourceId = trim((string) ($row['provider_resource_id'] ?? ''));
            if ($resourceId !== '') {
                $localByResource[$resourceId] = $row;
            }
        }

        $observed = 0;
        $managed = 0;
        $orphans = 0;
        $truncated = false;
        $seenOrphans = [];
        for ($page = 1; $page <= self::INVENTORY_MAX_PAGES; $page++) {
            $response = $this->client->get(
                '/v1/compute/instances?size=' . self::INVENTORY_PAGE_SIZE . '&page=' . $page
            );
            $rows = isset($response['data']) && is_array($response['data'])
                ? $response['data']
                : [];
            $observed += count($rows);
            foreach ($rows as $item) {
                if (!is_array($item)) {
                    continue;
                }
                $resourceId = trim((string) ($item['instanceId'] ?? ''));
                $serviceId = $this->taggedServiceId((string) ($item['displayName'] ?? ''));
                if ($resourceId === '' || $serviceId === null) {
                    continue;
                }
                $managed++;
                if (isset($localByResource[$resourceId])) {
                    $this->resolveOrphanFinding($providerAccountId, $resourceId);
                    continue;
                }
                $orphans++;
                $seenOrphans[$resourceId] = true;
                $this->recordOrphanFinding(
                    $serviceId,
                    $providerAccountId,
                    $resourceId,
                    $item
                );
            }
            if (count($rows) < self::INVENTORY_PAGE_SIZE) {
                break;
            }
            if ($page === self::INVENTORY_MAX_PAGES) {
                $truncated = true;
            }
        }

        if ($truncated) {
            $this->recordInventoryTruncation($providerAccountId);
        } else {
            $this->resolveInventoryTruncation($providerAccountId);
            $open = Capsule::table('mod_securiacevps_reconciliation')
                ->where('provider_account_id', $providerAccountId)
                ->where('finding_type', 'orphan_upstream')
                ->where('state', 'open')
                ->get();
            foreach ($open as $item) {
                $finding = (array) $item;
                $resourceId = trim((string) ($finding['provider_resource_id'] ?? ''));
                if ($resourceId !== '' && !isset($seenOrphans[$resourceId])) {
                    $this->resolveOrphanFinding($providerAccountId, $resourceId);
                }
            }
        }

        return [
            'observed' => $observed,
            'managed' => $managed,
            'orphans' => $orphans,
            'truncated' => $truncated,
        ];
    }

    private function taggedServiceId(string $displayName): ?int
    {
        if (preg_match('/(?:^|\s)whmcs-(\d+)(?=\s|$)/', trim($displayName), $matches) !== 1) {
            return null;
        }
        $serviceId = (int) $matches[1];
        return $serviceId > 0 ? $serviceId : null;
    }

    /** @param array<string,mixed> $providerResource */
    private function recordOrphanFinding(
        int $serviceId,
        string $providerAccountId,
        string $resourceId,
        array $providerResource
    ): void {
        $now = date('Y-m-d H:i:s');
        $serviceExists = Capsule::table('tblhosting')->where('id', $serviceId)->count() === 1;
        $evidence = [
            'provider_resource_id' => $resourceId,
            'tagged_service_id' => $serviceId,
            'whmcs_service_exists' => $serviceExists,
            'provider_state' => strtolower((string) ($providerResource['status'] ?? 'unknown')),
            'provider_region' => (string) ($providerResource['region'] ?? ''),
            'observed_at' => $now,
        ];
        $evidenceJson = CanonicalJson::encode($evidence);
        $existing = Capsule::table('mod_securiacevps_reconciliation')
            ->where('provider_account_id', $providerAccountId)
            ->where('provider_resource_id', $resourceId)
            ->where('finding_type', 'orphan_upstream')
            ->where('state', 'open')
            ->first();
        $values = [
            'service_id' => $serviceExists ? $serviceId : null,
            'severity' => 'critical',
            'evidence_hash' => hash('sha256', $evidenceJson),
            'evidence_json' => $evidenceJson,
            'safe_next_action' => $serviceExists ? 'review_adoption' : 'identify_or_cancel_orphan',
            'last_seen_at' => $now,
            'updated_at' => $now,
        ];
        if ($existing === null) {
            Capsule::table('mod_securiacevps_reconciliation')->insert(array_merge(
                [
                    'finding_uuid' => Uuid::v4(),
                    'provider_account_id' => $providerAccountId,
                    'provider_resource_id' => $resourceId,
                    'finding_type' => 'orphan_upstream',
                    'state' => 'open',
                    'first_seen_at' => $now,
                    'created_at' => $now,
                ],
                $values
            ));
        } else {
            Capsule::table('mod_securiacevps_reconciliation')
                ->where('id', (int) (((array) $existing)['id'] ?? 0))
                ->update($values);
        }
    }

    private function resolveOrphanFinding(string $providerAccountId, string $resourceId): void
    {
        Capsule::table('mod_securiacevps_reconciliation')
            ->where('provider_account_id', $providerAccountId)
            ->where('provider_resource_id', $resourceId)
            ->where('finding_type', 'orphan_upstream')
            ->where('state', 'open')
            ->update([
                'state' => 'resolved',
                'resolved_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
    }

    private function recordInventoryTruncation(string $providerAccountId): void
    {
        $now = date('Y-m-d H:i:s');
        $evidence = CanonicalJson::encode([
            'page_size' => self::INVENTORY_PAGE_SIZE,
            'page_limit' => self::INVENTORY_MAX_PAGES,
            'safe_code' => 'provider_inventory_scan_truncated',
        ]);
        $existing = Capsule::table('mod_securiacevps_reconciliation')
            ->where('provider_account_id', $providerAccountId)
            ->where('finding_type', 'provider_inventory_incomplete')
            ->where('state', 'open')
            ->first();
        $values = [
            'severity' => 'warning',
            'evidence_hash' => hash('sha256', $evidence),
            'evidence_json' => $evidence,
            'safe_next_action' => 'run_scoped_provider_inventory',
            'last_seen_at' => $now,
            'updated_at' => $now,
        ];
        if ($existing === null) {
            Capsule::table('mod_securiacevps_reconciliation')->insert(array_merge(
                [
                    'finding_uuid' => Uuid::v4(),
                    'service_id' => null,
                    'provider_account_id' => $providerAccountId,
                    'provider_resource_id' => null,
                    'finding_type' => 'provider_inventory_incomplete',
                    'state' => 'open',
                    'first_seen_at' => $now,
                    'created_at' => $now,
                ],
                $values
            ));
        } else {
            Capsule::table('mod_securiacevps_reconciliation')
                ->where('id', (int) (((array) $existing)['id'] ?? 0))
                ->update($values);
        }
    }

    private function resolveInventoryTruncation(string $providerAccountId): void
    {
        Capsule::table('mod_securiacevps_reconciliation')
            ->where('provider_account_id', $providerAccountId)
            ->where('finding_type', 'provider_inventory_incomplete')
            ->where('state', 'open')
            ->update([
                'state' => 'resolved',
                'resolved_at' => date('Y-m-d H:i:s'),
                'updated_at' => date('Y-m-d H:i:s'),
            ]);
    }

    private function recordFinding(
        int $serviceId,
        string $providerAccountId,
        string $resourceId,
        string $state,
        string $evidenceJson
    ): void {
        $now = date('Y-m-d H:i:s');
        if ($state === 'verified') {
            Capsule::table('mod_securiacevps_reconciliation')
                ->where('service_id', $serviceId)
                ->where('state', 'open')
                ->update(['state' => 'resolved', 'resolved_at' => $now, 'updated_at' => $now]);
            return;
        }
        $existing = Capsule::table('mod_securiacevps_reconciliation')
            ->where('service_id', $serviceId)
            ->where('finding_type', 'adoption_' . $state)
            ->where('state', 'open')
            ->first();
        $values = [
            'provider_account_id' => $providerAccountId,
            'provider_resource_id' => $resourceId !== '' ? $resourceId : null,
            'severity' => in_array($state, ['conflict', 'ambiguous'], true) ? 'critical' : 'warning',
            'evidence_hash' => hash('sha256', $evidenceJson),
            'evidence_json' => $evidenceJson,
            'safe_next_action' => $state === 'probable' ? 'review_adoption' : 'inspect_ownership',
            'last_seen_at' => $now,
            'updated_at' => $now,
        ];
        if ($existing === null) {
            $values = array_merge($values, [
                'finding_uuid' => Uuid::v4(),
                'service_id' => $serviceId,
                'finding_type' => 'adoption_' . $state,
                'state' => 'open',
                'first_seen_at' => $now,
                'created_at' => $now,
            ]);
            Capsule::table('mod_securiacevps_reconciliation')->insert($values);
        } else {
            Capsule::table('mod_securiacevps_reconciliation')
                ->where('id', (int) (((array) $existing)['id'] ?? 0))
                ->update($values);
        }
    }
}
