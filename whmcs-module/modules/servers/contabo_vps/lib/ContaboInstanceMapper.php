<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Translates WHMCS provisioning $params into a Contabo
 * POST /v1/compute/instances request body. Validates the product is fully
 * configured (image + region) before any API call is made.
 */
class ContaboInstanceMapper
{
    /**
     * @param array<string,mixed> $params
     * @return array<string,mixed>
     */
    public function mapCreate(array $params): array
    {
        $imageId   = trim((string) ($params['configoption1'] ?? ''));
        $region    = trim((string) ($params['configoption2'] ?? 'EU1'));
        $secretId  = trim((string) ($params['configoption3'] ?? ''));
        $productId = trim((string) ($params['configoption4'] ?? 'V1'));
        $domain    = (string) ($params['domain'] ?? 'whmcs-vps');

        if ($imageId === '') {
            throw new ContaboProvisioningException('Product misconfigured: OS image ID (config option 1) not set');
        }
        if ($region === '') {
            throw new ContaboProvisioningException('Product misconfigured: region (config option 2) not set');
        }

        $body = [
            'imageId'     => $imageId,
            'productId'   => $productId,
            'region'      => $region,
            'displayName' => substr($domain, 0, 255),
        ];

        // Optional SSH secret from the Contabo secrets vault.
        if ($secretId !== '') {
            $body['sshKeys'] = [(int) $secretId];
        }

        return $body;
    }
}
