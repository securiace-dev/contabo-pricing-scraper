<?php
declare(strict_types=1);

namespace SecuriAceVps;

/**
 * Translates WHMCS provisioning $params + resolved customer selections into a
 * Contabo POST /v1/compute/instances request body. Pure class (no DB, no API)
 * — InstanceService assembles the inputs; this validates and shapes them.
 *
 * Body fields per the official API contract: imageId, productId, region,
 * period (REQUIRED, 1|3|6|12), displayName, sshKeys[secretId], rootPassword
 * (secretId), userData (cloud-init), addOns.
 *
 * Sources of truth, in priority order:
 *   1. Customer selections (round-tripped via the addon link tables, already
 *      resolved to API values by InstanceService): Image → imageId,
 *      Region → region.
 *   2. Product config options: configoption1 imageId, configoption2 region,
 *      configoption3 SSH secret id, configoption4 Contabo productId,
 *      configoption5 cloud-init userData, configoption6 addOns JSON.
 */
final class ContaboInstanceMapper
{
    /**
     * Contabo retail region label → API region slug. The scraper's canonical
     * labels (option_catalog "Region" dimension) on the left. Already-slug
     * values pass through via the values list. Unknown labels FAIL CLOSED.
     *
     * @var array<string,string>
     */
    private const REGION_SLUGS = [
        'european union'         => 'EU',
        'united kingdom'         => 'UK',
        'united states (central)'=> 'US-central',
        'united states (east)'   => 'US-east',
        'united states (west)'   => 'US-west',
        'singapore'              => 'SIN',
        'australia'              => 'AUS',
        'india'                  => 'IND',
        'japan'                  => 'JPN',
    ];

    /**
     * Resolve a region selection (retail label, addon value key
     * "Europe:European Union", or an API slug) to the API slug. Fail-closed.
     */
    public static function resolveRegionSlug(string $selection): string
    {
        $value = trim($selection);
        if ($value === '') {
            throw new ContaboProvisioningException('Empty region selection');
        }
        $colon = strpos($value, ':');
        if ($colon !== false) {
            $value = trim(substr($value, $colon + 1));
        }
        foreach (self::REGION_SLUGS as $slug) {
            if (strcasecmp($slug, $value) === 0) {
                return $slug; // already an API slug
            }
        }
        $key = strtolower((string) preg_replace('/\s+/', ' ', $value));
        if (isset(self::REGION_SLUGS[$key])) {
            return self::REGION_SLUGS[$key];
        }
        throw new ContaboProvisioningException(
            'Cannot resolve region "' . $selection . '" to a Contabo region slug. Known: '
            . implode(', ', array_unique(array_values(self::REGION_SLUGS)))
        );
    }

    /**
     * @param array<string,mixed> $params   WHMCS module params
     * @param array{imageId?:string, region?:string, addOns?:array<string,mixed>} $resolved
     *        Selection-derived API values assembled by InstanceService
     *        (already resolved via ImageResolver / resolveRegionSlug).
     * @param int|null $rootPasswordSecretId Vault secret carrying the root password.
     * @return array<string,mixed>
     */
    public function mapCreate(array $params, array $resolved = [], ?int $rootPasswordSecretId = null): array
    {
        $imageId   = trim((string) ($resolved['imageId'] ?? ''));
        if ($imageId === '') {
            $imageId = trim((string) ($params['configoption1'] ?? ''));
        }
        $region    = trim((string) ($resolved['region'] ?? ''));
        if ($region === '') {
            $region = trim((string) ($params['configoption2'] ?? ''));
        }
        $secretOpt = trim((string) ($params['configoption3'] ?? ''));
        $productId = trim((string) ($params['configoption4'] ?? ''));
        $userData  = trim((string) ($params['configoption5'] ?? ''));
        $addOnsRaw = trim((string) ($params['configoption6'] ?? ''));
        $serviceId = (int) ($params['serviceid'] ?? 0);
        $domain    = (string) ($params['domain'] ?? '');

        if ($imageId === '') {
            throw new ContaboProvisioningException('Product misconfigured: no OS image — set config option 1 (imageId) or expose an Image configurable option');
        }
        if ($region === '') {
            throw new ContaboProvisioningException('Product misconfigured: no region — set config option 2 or expose a Region configurable option');
        }
        if ($productId === '') {
            throw new ContaboProvisioningException('Product misconfigured: Contabo product id (config option 4) not set');
        }
        if ($serviceId <= 0) {
            throw new ContaboProvisioningException('Missing WHMCS service id');
        }

        $body = [
            'imageId'     => $imageId,
            'productId'   => $productId,
            'region'      => $region,
            'period'      => BillingCycleMapper::toPeriod((string) ($params['billingcycle'] ?? '')),
            'displayName' => InstanceLinker::displayName($serviceId, $domain),
        ];

        if ($secretOpt !== '') {
            if (!ctype_digit($secretOpt)) {
                throw new ContaboProvisioningException('Product misconfigured: SSH secret id (config option 3) must be the numeric secretId from the Contabo vault, got "' . $secretOpt . '"');
            }
            $body['sshKeys'] = [(int) $secretOpt];
        }
        if ($rootPasswordSecretId !== null && $rootPasswordSecretId > 0) {
            $body['rootPassword'] = $rootPasswordSecretId;
        }
        if ($userData !== '') {
            $body['userData'] = $userData;
        }

        $addOns = [];
        if ($addOnsRaw !== '') {
            $decoded = json_decode($addOnsRaw, true);
            if (!is_array($decoded)) {
                throw new ContaboProvisioningException('Product misconfigured: add-ons (config option 6) is not valid JSON');
            }
            $addOns = $decoded;
        }
        if (isset($resolved['addOns']) && is_array($resolved['addOns'])) {
            $addOns = array_merge($addOns, $resolved['addOns']);
        }
        if ($addOns !== []) {
            $body['addOns'] = $addOns;
        }

        return $body;
    }
}
