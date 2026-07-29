<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Fail-closed validation for VPS products immediately before WHMCS creates an
 * order. The guard never calls the Rust catalog API or Contabo: checkout is
 * validated exclusively against the last approved, locally published contract.
 */
final class CheckoutGuard
{
    private const CUSTOMER_ERROR =
        'This VPS configuration has changed or is temporarily unavailable. '
        . 'Return to the VPS configuration step and review your selections.';

    /**
     * @param list<array<string,mixed>> $products
     * @return list<string>
     */
    public function validateProducts(array $products): array
    {
        $errors = [];
        foreach ($products as $index => $cartProduct) {
            $productId = (int) ($cartProduct['pid'] ?? 0);
            if ($productId <= 0 || !$this->isVpsProduct($productId)) {
                continue;
            }

            try {
                $this->assertProductContract($productId, $cartProduct);
            } catch (\Throwable $e) {
                $errors[] = self::CUSTOMER_ERROR;
                if (function_exists('logActivity')) {
                    logActivity(
                        'Contabo Pricing checkout guard rejected VPS cart item '
                        . (int) $index . ' (product #' . $productId . '): '
                        . $this->safeReason($e)
                    );
                }
            }
        }

        return array_values(array_unique($errors));
    }

    private function isVpsProduct(int $productId): bool
    {
        $product = Capsule::table('tblproducts')->where('id', $productId)->first();
        if ($product === null) {
            return false;
        }
        $module = (string) (((array) $product)['servertype'] ?? '');
        return in_array($module, ['securiacevps', 'contabo_vps'], true);
    }

    /**
     * @param array<string,mixed> $cartProduct
     */
    private function assertProductContract(int $productId, array $cartProduct): void
    {
        $mappingObject = Capsule::table('mod_contabo_mapping')
            ->where('product_id', $productId)
            ->where('active', 1)
            ->where('mapping_state', 'published')
            ->orderByDesc('id')
            ->first();
        if ($mappingObject === null) {
            throw new \RuntimeException('published_mapping_missing');
        }
        $mapping = (array) $mappingObject;
        $mappingVersion = trim((string) ($mapping['published_mapping_version'] ?? ''));
        if ($mappingVersion === '') {
            throw new \RuntimeException('mapping_version_missing');
        }

        $publicationObject = Capsule::table('mod_contabo_mapping_publications')
            ->where('mapping_version', $mappingVersion)
            ->where('product_id', $productId)
            ->where('state', 'published')
            ->first();
        if ($publicationObject === null) {
            throw new \RuntimeException('publication_missing');
        }
        $publication = (array) $publicationObject;
        $payloadJson = (string) ($publication['payload_json'] ?? '');
        $storedHash = strtolower(trim((string) ($publication['payload_hash'] ?? '')));
        if (
            $payloadJson === ''
            || !preg_match('/^[a-f0-9]{64}$/', $storedHash)
            || !hash_equals($storedHash, hash('sha256', $payloadJson))
        ) {
            throw new \RuntimeException('publication_integrity_failed');
        }
        $payload = json_decode($payloadJson, true);
        if (!is_array($payload)) {
            throw new \RuntimeException('publication_payload_invalid');
        }

        $provider = isset($payload['provider']) && is_array($payload['provider'])
            ? $payload['provider']
            : [];
        foreach (['sku_id', 'region_id', 'image_id'] as $required) {
            if (trim((string) ($provider[$required] ?? '')) === '') {
                throw new \RuntimeException('provider_identifier_missing');
            }
        }

        $this->assertCatalogAvailability(
            (int) ($publication['catalog_version_id'] ?? 0),
            (string) $provider['sku_id']
        );
        $selections = $this->resolveSelections(
            (int) ($mapping['profile_id'] ?? 0),
            isset($cartProduct['configoptions']) && is_array($cartProduct['configoptions'])
                ? $cartProduct['configoptions']
                : []
        );

        $validation = (new ConfigOptionCompatibilityRepository())->validateCombination(
            (string) $provider['sku_id'],
            $selections
        );
        if (empty($validation['valid'])) {
            throw new \RuntimeException('configuration_incompatible');
        }
        $this->assertManagementSelection(
            (string) ($payload['management_code'] ?? 'self_managed'),
            $selections
        );
    }

    private function assertCatalogAvailability(int $catalogVersionId, string $providerSkuId): void
    {
        if ($catalogVersionId <= 0) {
            throw new \RuntimeException('catalog_version_missing');
        }
        $catalogObject = Capsule::table('mod_contabo_catalog_versions')
            ->where('id', $catalogVersionId)
            ->first();
        if ($catalogObject === null) {
            throw new \RuntimeException('catalog_not_imported');
        }
        $catalog = (array) $catalogObject;
        if (in_array(strtolower((string) ($catalog['state'] ?? '')), ['invalid', 'retired'], true)) {
            throw new \RuntimeException('catalog_not_publishable');
        }

        $itemObject = Capsule::table('mod_contabo_catalog_items')
            ->where('catalog_version_id', $catalogVersionId)
            ->where('provider_id', $providerSkuId)
            ->first();
        if ($itemObject === null) {
            throw new \RuntimeException('catalog_sku_missing');
        }
        $item = (array) $itemObject;
        $availability = strtolower((string) ($item['availability_state'] ?? ''));
        if (
            !empty($item['deprecated'])
            || in_array($availability, ['unavailable', 'out_of_stock', 'disabled', 'retired'], true)
        ) {
            throw new \RuntimeException('catalog_sku_unavailable');
        }
    }

    /**
     * @param array<int|string,mixed> $cartOptions
     * @return list<array<string,mixed>>
     */
    private function resolveSelections(int $profileId, array $cartOptions): array
    {
        $selections = [];
        foreach ($cartOptions as $configId => $selected) {
            $optionObject = Capsule::table('mod_contabo_config_option_link')
                ->where('profile_id', $profileId)
                ->where('whmcs_option_id', (int) $configId)
                ->first();
            if ($optionObject === null) {
                throw new \RuntimeException('cart_option_unmapped');
            }
            $option = (array) $optionObject;
            if (
                empty($option['enabled'])
                || empty($option['expose_to_customer'])
                || empty($option['allowed_for_new_orders'])
            ) {
                throw new \RuntimeException('cart_option_not_orderable');
            }

            $optionType = (int) ($option['optiontype'] ?? 0);
            if ($optionType === 4) {
                $selections[] = [
                    'dimension_key' => (string) ($option['dimension_key'] ?? ''),
                    'value_key' => (string) (
                        $option['default_value'] ?? ($option['dimension_key'] ?? 'quantity')
                    ),
                    'qty' => max(0, (int) $selected),
                ];
                continue;
            }

            $valueObject = Capsule::table('mod_contabo_config_option_value_link')
                ->where('option_link_id', (int) ($option['id'] ?? 0))
                ->where('whmcs_sub_id', (int) $selected)
                ->first();
            if ($valueObject === null) {
                throw new \RuntimeException('cart_option_value_unmapped');
            }
            $value = (array) $valueObject;
            $selections[] = [
                'dimension_key' => (string) ($option['dimension_key'] ?? ''),
                'value_key' => (string) ($value['contabo_value_key'] ?? ''),
                'qty' => 1,
            ];
        }

        return $selections;
    }

    /**
     * @param list<array<string,mixed>> $selections
     */
    private function assertManagementSelection(string $expected, array $selections): void
    {
        $expected = $expected !== '' ? $expected : 'self_managed';
        $selected = [];
        foreach ($selections as $selection) {
            if (stripos((string) ($selection['dimension_key'] ?? ''), 'management') !== false) {
                $selected[] = (string) ($selection['value_key'] ?? '');
            }
        }

        if ($expected === 'self_managed') {
            if ($selected !== [] && $selected !== ['self_managed']) {
                throw new \RuntimeException('self_managed_product_has_paid_management');
            }
            return;
        }
        if (count($selected) !== 1 || $selected[0] !== $expected) {
            throw new \RuntimeException('managed_tier_selection_mismatch');
        }
    }

    private function safeReason(\Throwable $e): string
    {
        $reason = preg_replace('/[^a-z0-9_.-]+/i', '_', $e->getMessage());
        return substr((string) $reason, 0, 80);
    }
}
