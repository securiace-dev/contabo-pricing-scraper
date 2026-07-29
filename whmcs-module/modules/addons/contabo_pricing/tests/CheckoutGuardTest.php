<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CheckoutGuard;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class CheckoutGuardTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        $this->seed();
    }

    public function testPublishedAvailableCompatibleVpsCanCheckout(): void
    {
        $this->assertSame([], (new CheckoutGuard())->validateProducts([$this->cartProduct()]));
    }

    public function testNonVpsProductsAreOutsideTheGuard(): void
    {
        Capsule::$tables['tblproducts'][0]['servertype'] = 'cpanel';
        $this->assertSame([], (new CheckoutGuard())->validateProducts([$this->cartProduct()]));
    }

    public function testUnavailableSkuFailsClosedWithCustomerSafeMessage(): void
    {
        Capsule::$tables['mod_contabo_catalog_items'][0]['availability_state'] = 'unavailable';
        $errors = (new CheckoutGuard())->validateProducts([$this->cartProduct()]);

        $this->assertCount(1, $errors);
        $this->assertStringNotContainsString('V45', $errors[0]);
        $this->assertStringContainsString('review your selections', $errors[0]);
    }

    public function testPublicationPayloadTamperingFailsClosed(): void
    {
        Capsule::$tables['mod_contabo_mapping_publications'][0]['payload_json'] .= ' ';
        $this->assertCount(
            1,
            (new CheckoutGuard())->validateProducts([$this->cartProduct()])
        );
    }

    public function testUnmappedCartOptionFailsClosed(): void
    {
        $product = $this->cartProduct();
        $product['configoptions'][999] = 777;
        $this->assertCount(1, (new CheckoutGuard())->validateProducts([$product]));
    }

    public function testCompatibilityViolationFailsClosed(): void
    {
        Capsule::$tables['mod_contabo_option_compatibility'][] = [
            'plan_slug' => 'V45',
            'dimension_key' => 'Image',
            'value_key' => 'ubuntu-24-04',
            'incompatible_with_json' => '[]',
            'required_values_json' => '["eu-west"]',
        ];
        $this->assertCount(
            1,
            (new CheckoutGuard())->validateProducts([$this->cartProduct()])
        );
    }

    public function testManagedProductRequiresItsExactTier(): void
    {
        $publication = &Capsule::$tables['mod_contabo_mapping_publications'][0];
        $payload = json_decode((string) $publication['payload_json'], true);
        $payload['management_code'] = 'pro';
        $publication['payload_json'] = $this->canonicalJson($payload);
        $publication['payload_hash'] = hash('sha256', $publication['payload_json']);

        $this->assertCount(
            1,
            (new CheckoutGuard())->validateProducts([$this->cartProduct()])
        );
    }

    /** @return array<string,mixed> */
    private function cartProduct(): array
    {
        return [
            'pid' => 50,
            'billingcycle' => 'monthly',
            'configoptions' => [100 => 101],
        ];
    }

    private function seed(): void
    {
        $payload = [
            'schema_version' => 1,
            'provider' => [
                'sku_id' => 'V45',
                'region_id' => 'EU',
                'image_id' => 'image-1',
            ],
            'management_code' => 'self_managed',
        ];
        $payloadJson = $this->canonicalJson($payload);
        Capsule::$tables['tblproducts'] = [[
            'id' => 50,
            'servertype' => 'securiacevps',
        ]];
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id' => 70,
            'profile_id' => 80,
            'product_id' => 50,
            'active' => 1,
            'mapping_state' => 'published',
            'published_mapping_version' => 'mapping-v1',
        ]];
        Capsule::$tables['mod_contabo_mapping_publications'] = [[
            'id' => 90,
            'mapping_version' => 'mapping-v1',
            'product_id' => 50,
            'catalog_version_id' => 91,
            'state' => 'published',
            'payload_hash' => hash('sha256', $payloadJson),
            'payload_json' => $payloadJson,
        ]];
        Capsule::$tables['mod_contabo_catalog_versions'] = [[
            'id' => 91,
            'state' => 'imported',
        ]];
        Capsule::$tables['mod_contabo_catalog_items'] = [[
            'catalog_version_id' => 91,
            'provider_id' => 'V45',
            'item_type' => 'plan',
            'availability_state' => 'observed',
            'deprecated' => 0,
        ]];
        Capsule::$tables['mod_contabo_config_option_link'] = [[
            'id' => 102,
            'profile_id' => 80,
            'dimension_key' => 'Image',
            'whmcs_option_id' => 100,
            'optiontype' => 1,
            'enabled' => 1,
            'expose_to_customer' => 1,
            'allowed_for_new_orders' => 1,
        ]];
        Capsule::$tables['mod_contabo_config_option_value_link'] = [[
            'option_link_id' => 102,
            'whmcs_sub_id' => 101,
            'contabo_value_key' => 'ubuntu-24-04',
            'contabo_label' => 'Ubuntu 24.04',
        ]];
        Capsule::$tables['mod_contabo_option_compatibility'] = [];
    }

    /** @param mixed $value */
    private function canonicalJson($value): string
    {
        if (is_array($value)) {
            $expected = 0;
            $list = true;
            foreach ($value as $key => $_unused) {
                if ($key !== $expected++) {
                    $list = false;
                    break;
                }
            }
            if (!$list) {
                ksort($value, SORT_STRING);
            }
            foreach ($value as $key => $item) {
                $value[$key] = json_decode($this->canonicalJson($item), true);
            }
        }
        $encoded = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return is_string($encoded) ? $encoded : '';
    }
}
