<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * The single authority on WHICH Contabo instance belongs to WHICH WHMCS
 * service. Everything that could touch the wrong customer's server funnels
 * through here.
 *
 * Identity model (two independent anchors, both must be maintained):
 *   1. The service custom field "contabo_instance_id" (auto-created on the
 *      product) — WHMCS-side pointer to the instance.
 *   2. The instance displayName tag "whmcs-{serviceid}" — Contabo-side back
 *      pointer to the service. Verified before destructive actions and
 *      re-asserted by sync when it drifts.
 *
 * Write policy:
 *   - storeInstanceId() refuses to overwrite a DIFFERENT non-empty id unless
 *     forced (same-id writes are no-ops).
 *   - Adoption of an unlinked instance happens only on an EXACT single
 *     displayName-tag match; ambiguity throws — never guess.
 */
final class InstanceLinker
{
    public const FIELD_NAME = 'contabo_instance_id';
    private const FIELD_INSERT_NAME = 'contabo_instance_id|Contabo Instance ID';
    private const FIND_MAX_PAGES = 10;
    private const FIND_PAGE_SIZE = 100;

    // ── identity tag ─────────────────────────────────────────────────────────

    public static function tag(int $serviceId): string
    {
        return 'whmcs-' . $serviceId;
    }

    /**
     * Tagged Contabo displayName: "whmcs-{sid} domain". Contabo restricts the
     * charset, so anything outside [A-Za-z0-9 ._-] is squashed to '-'.
     */
    public static function displayName(int $serviceId, string $domain): string
    {
        $safe = preg_replace('/[^A-Za-z0-9 ._-]/', '-', $domain);
        $name = trim(self::tag($serviceId) . ' ' . trim((string) $safe));
        return substr($name, 0, 255);
    }

    /** Does this displayName carry the service's tag? ("whmcs-12" must not match "whmcs-123") */
    public static function displayNameMatchesTag(string $displayName, int $serviceId): bool
    {
        $tag = self::tag($serviceId);
        if (strpos($displayName, $tag) !== 0) {
            return false;
        }
        $next = substr($displayName, strlen($tag), 1);
        return $next === '' || $next === ' ' || $next === false;
    }

    // ── custom field: ensure / read / store ──────────────────────────────────

    /**
     * Find — or create — the product's "contabo_instance_id" custom field and
     * return its id. Called BEFORE any create API call: if instance-id storage
     * cannot be guaranteed we refuse to provision (an unlinked instance is an
     * orphan the module can never manage again).
     */
    public function ensureCustomField(int $productId): int
    {
        if ($productId <= 0) {
            throw new ContaboProvisioningException('Cannot resolve the product id for the instance-id custom field');
        }

        $fieldId = $this->findFieldId($productId);
        if ($fieldId !== null) {
            return $fieldId;
        }

        Capsule::table('tblcustomfields')->insert([
            'type'         => 'product',
            'relid'        => $productId,
            'fieldname'    => self::FIELD_INSERT_NAME,
            'fieldtype'    => 'text',
            'description'  => 'Contabo instance id managed by the securiacevps module — do not edit.',
            'fieldoptions' => '',
            'regexpr'      => '',
            'adminonly'    => 'on',
            'required'     => '',
            'showorder'    => '',
            'showinvoice'  => '',
            'sortorder'    => 0,
        ]);

        $fieldId = $this->findFieldId($productId);
        if ($fieldId === null) {
            throw new ContaboProvisioningException('Could not create the "contabo_instance_id" custom field on product #' . $productId);
        }
        if (function_exists('logActivity')) {
            logActivity('Contabo VPS: auto-created the "contabo_instance_id" custom field on product #' . $productId . '.');
        }
        return $fieldId;
    }

    /**
     * The instance id linked to the service, or '' if none. Reads the
     * customfields module param first (tolerating both "name" and
     * "name|Friendly Name" keys), then falls back to the DB.
     *
     * @param array<string,mixed> $params WHMCS module params
     */
    public function readInstanceId(array $params): string
    {
        $fields = $params['customfields'] ?? [];
        if (is_array($fields)) {
            foreach ($fields as $key => $value) {
                if ($this->isInstanceFieldName((string) $key) && trim((string) $value) !== '') {
                    return trim((string) $value);
                }
            }
        }

        $serviceId = (int) ($params['serviceid'] ?? 0);
        $productId = (int) ($params['pid'] ?? ($params['packageid'] ?? 0));
        if ($serviceId <= 0) {
            return '';
        }
        $fieldId = $productId > 0 ? $this->findFieldId($productId) : $this->findFieldIdAnyProduct();
        if ($fieldId === null) {
            return '';
        }
        $value = Capsule::table('tblcustomfieldsvalues')
            ->where('fieldid', $fieldId)
            ->where('relid', $serviceId)
            ->value('value');
        return $value === null ? '' : trim((string) $value);
    }

    /**
     * Persist the instance id. Same-id writes are no-ops; a DIFFERENT stored
     * id is never silently replaced (that is how services end up pointing at
     * someone else's server) — it throws unless $force.
     */
    public function storeInstanceId(int $serviceId, int $fieldId, string $instanceId, bool $force = false): void
    {
        if ($serviceId <= 0 || $fieldId <= 0 || trim($instanceId) === '') {
            throw new ContaboProvisioningException('storeInstanceId called with incomplete linkage data');
        }
        $instanceId = trim($instanceId);

        $existing = Capsule::table('tblcustomfieldsvalues')
            ->where('fieldid', $fieldId)
            ->where('relid', $serviceId)
            ->first();
        $existing = $existing !== null ? (array) $existing : null;

        if ($existing === null) {
            Capsule::table('tblcustomfieldsvalues')->insert([
                'fieldid' => $fieldId,
                'relid'   => $serviceId,
                'value'   => $instanceId,
            ]);
            return;
        }

        $current = trim((string) ($existing['value'] ?? ''));
        if ($current === $instanceId) {
            return;
        }
        if ($current !== '' && !$force) {
            throw new ContaboProvisioningException(
                'Service #' . $serviceId . ' is already linked to Contabo instance ' . $current
                . ' — refusing to silently relink to ' . $instanceId . '. Clear the custom field deliberately if this is intended.'
            );
        }

        Capsule::table('tblcustomfieldsvalues')
            ->where('fieldid', $fieldId)
            ->where('relid', $serviceId)
            ->update(['value' => $instanceId]);
    }

    // ── Contabo-side verification ────────────────────────────────────────────

    /**
     * Fetch the instance and report whether it exists and whether its
     * displayName still carries this service's tag.
     *
     * @return array{exists:bool, tagMatches:bool, instance:array<string,mixed>}
     */
    public function verifyOwnership(ContaboApiClient $client, string $instanceId, int $serviceId): array
    {
        try {
            $resp = $client->get('/v1/compute/instances/' . rawurlencode($instanceId));
        } catch (ContaboProvisioningException $e) {
            if (strpos($e->getMessage(), 'HTTP 404') !== false) {
                return ['exists' => false, 'tagMatches' => false, 'instance' => []];
            }
            throw $e;
        }
        $inst = isset($resp['data'][0]) && is_array($resp['data'][0]) ? $resp['data'][0] : [];
        if ($inst === []) {
            return ['exists' => false, 'tagMatches' => false, 'instance' => []];
        }
        $display = (string) ($inst['displayName'] ?? '');
        return [
            'exists'     => true,
            'tagMatches' => self::displayNameMatchesTag($display, $serviceId),
            'instance'   => $inst,
        ];
    }

    /**
     * Recovery path: find the account's instance carrying this service's
     * displayName tag. Exactly one match → that instance (safe to adopt);
     * zero → null; more than one → throw (never guess between candidates).
     *
     * Primary strategy is a server-side `search` filter on the tag, so this
     * does not depend on scanning the whole account (important for large /
     * reseller accounts that exceed the page cap). If the filtered query is
     * unavailable it falls back to a bounded paginated scan, and logs a warning
     * if that scan is truncated — silent truncation could otherwise let a
     * duplicate tag slip through undetected.
     *
     * @return array<string,mixed>|null
     */
    public function findByTag(ContaboApiClient $client, int $serviceId): ?array
    {
        $tag = self::tag($serviceId);

        // Primary: server-side full-text search on the tag (matches displayName).
        $matches = $this->collectTagMatches($client, $serviceId, '&search=' . rawurlencode($tag), 2, false);
        if ($matches === []) {
            // Fallback: bounded unfiltered scan (search unsupported / proxied away).
            $matches = $this->collectTagMatches($client, $serviceId, '', self::FIND_MAX_PAGES, true);
        }

        if (count($matches) > 1) {
            $ids = [];
            foreach ($matches as $m) {
                $ids[] = (string) ($m['instanceId'] ?? '?');
            }
            throw new ContaboProvisioningException(
                'Multiple Contabo instances carry the tag "' . $tag . '" (' . implode(', ', $ids)
                . ') — refusing to guess. Resolve the duplicates in the Contabo panel, then retry.'
            );
        }
        return $matches !== [] ? $matches[0] : null;
    }

    /**
     * Paginate the instances list (optionally filtered by $extraQuery),
     * collecting rows whose displayName carries the service tag, deduped by
     * instanceId. When $warnOnCap and the scan stops because it hit $maxPages
     * (a still-full page), log a truncation warning.
     *
     * @return list<array<string,mixed>>
     */
    private function collectTagMatches(ContaboApiClient $client, int $serviceId, string $extraQuery, int $maxPages, bool $warnOnCap): array
    {
        $byId = [];
        $truncated = false;
        for ($page = 1; $page <= $maxPages; $page++) {
            $resp = $client->get('/v1/compute/instances?size=' . self::FIND_PAGE_SIZE . '&page=' . $page . $extraQuery);
            $rows = isset($resp['data']) && is_array($resp['data']) ? $resp['data'] : [];
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                if (self::displayNameMatchesTag((string) ($row['displayName'] ?? ''), $serviceId)) {
                    $byId[(string) ($row['instanceId'] ?? count($byId))] = $row;
                }
            }
            if (count($rows) < self::FIND_PAGE_SIZE) {
                break;
            }
            if ($page === $maxPages) {
                $truncated = true;
            }
        }
        if ($truncated && $warnOnCap && function_exists('logActivity')) {
            logActivity('Contabo VPS: instance search for tag "' . self::tag($serviceId) . '" hit the '
                . ($maxPages * self::FIND_PAGE_SIZE) . '-instance scan cap — result may be incomplete. '
                . 'If provisioning cannot find an existing instance, resolve it manually in the Contabo panel.');
        }
        return array_values($byId);
    }

    // ── internals ────────────────────────────────────────────────────────────

    private function isInstanceFieldName(string $name): bool
    {
        return $name === self::FIELD_NAME || strpos($name, self::FIELD_NAME . '|') === 0;
    }

    private function findFieldId(int $productId): ?int
    {
        $rows = Capsule::table('tblcustomfields')
            ->where('type', 'product')
            ->where('relid', $productId)
            ->get();
        foreach ($rows as $row) {
            $row = (array) $row;
            if ($this->isInstanceFieldName((string) ($row['fieldname'] ?? ''))) {
                return (int) ($row['id'] ?? 0) > 0 ? (int) $row['id'] : null;
            }
        }
        return null;
    }

    /** Fallback when the product id isn't in the params (e.g. some hook paths). */
    private function findFieldIdAnyProduct(): ?int
    {
        $rows = Capsule::table('tblcustomfields')
            ->where('type', 'product')
            ->get();
        foreach ($rows as $row) {
            $row = (array) $row;
            if ($this->isInstanceFieldName((string) ($row['fieldname'] ?? ''))) {
                return (int) ($row['id'] ?? 0) > 0 ? (int) $row['id'] : null;
            }
        }
        return null;
    }
}
