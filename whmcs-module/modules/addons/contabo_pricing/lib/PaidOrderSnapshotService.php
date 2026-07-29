<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Creates and seals the immutable provisioning contract for a paid WHMCS
 * service. It reads only a published mapping version and the order/service
 * records created by WHMCS. Provisioning never reads the current catalog.
 */
final class PaidOrderSnapshotService
{
    /**
     * @return string snapshot UUID
     */
    public function createDraft(
        int $orderId,
        int $invoiceId,
        int $serviceId,
        int $quoteTtlSeconds = 1800
    ): string {
        $service = $this->row('tblhosting', 'id', $serviceId);
        if ($service === null) {
            throw new \RuntimeException('Service #' . $serviceId . ' was not found');
        }
        $sealed = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('order_id', $orderId)
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        if ($sealed !== null) {
            return (string) (((array) $sealed)['snapshot_uuid'] ?? '');
        }
        $productId = (int) ($service['packageid'] ?? 0);
        $product = $this->row('tblproducts', 'id', $productId);
        if ($product === null || !in_array((string) ($product['servertype'] ?? ''), ['securiacevps', 'contabo_vps'], true)) {
            return '';
        }
        $mapping = Capsule::table('mod_contabo_mapping')
            ->where('product_id', $productId)
            ->where('active', 1)
            ->where('mapping_state', 'published')
            ->orderByDesc('id')
            ->first();
        if ($mapping === null) {
            throw new \RuntimeException(
                'Product #' . $productId . ' has no published Contabo mapping'
            );
        }
        $mapping = (array) $mapping;
        $mappingVersion = trim((string) ($mapping['published_mapping_version'] ?? ''));
        if ($mappingVersion === '') {
            throw new \RuntimeException('The published mapping has no immutable version');
        }
        $publication = Capsule::table('mod_contabo_mapping_publications')
            ->where('mapping_version', $mappingVersion)
            ->where('state', 'published')
            ->first();
        if ($publication === null) {
            throw new \RuntimeException(
                'Mapping publication "' . $mappingVersion . '" was not found or is not published'
            );
        }
        $publication = (array) $publication;
        $publicationPayload = json_decode((string) ($publication['payload_json'] ?? ''), true);
        if (!is_array($publicationPayload)) {
            throw new \RuntimeException('Published mapping payload is invalid');
        }
        $provider = isset($publicationPayload['provider']) && is_array($publicationPayload['provider'])
            ? $publicationPayload['provider']
            : [];
        foreach (['sku_id', 'region_id', 'image_id'] as $required) {
            if (trim((string) ($provider[$required] ?? '')) === '') {
                throw new \RuntimeException('Published mapping is missing provider.' . $required);
            }
        }

        $order = $this->row('tblorders', 'id', $orderId);
        $invoice = $this->row('tblinvoices', 'id', $invoiceId);
        $client = $this->row('tblclients', 'id', (int) ($service['userid'] ?? 0));
        $currency = $client !== null
            ? $this->row('tblcurrencies', 'id', (int) ($client['currency'] ?? 0))
            : null;
        $configuration = [
            'billing_cycle' => (string) ($service['billingcycle'] ?? ''),
            'domain' => (string) ($service['domain'] ?? ''),
            'image_id' => (string) $provider['image_id'],
            'region' => (string) $provider['region_id'],
            'options' => $this->selectedOptions($serviceId),
            'compatibility_version' => (string) ($publicationPayload['compatibility_version'] ?? ''),
            'management_code' => (string) ($publicationPayload['management_code'] ?? 'self_managed'),
        ];
        $pricing = [
            'billing_cycle' => (string) ($service['billingcycle'] ?? ''),
            'currency' => (string) ($currency['code'] ?? ''),
            'setup' => $this->money($service['firstpaymentamount'] ?? '0'),
            'recurring' => $this->money($service['amount'] ?? '0'),
            'renewal' => $this->money($publicationPayload['renewal_amount'] ?? ($service['amount'] ?? '0')),
            'discount_code' => (string) ($order['promocode'] ?? ''),
            'tax_policy' => (string) ($publicationPayload['tax_policy'] ?? 'whmcs'),
            'tax_1' => $this->money($invoice['tax'] ?? '0'),
            'tax_2' => $this->money($invoice['tax2'] ?? '0'),
        ];
        $whmcs = [
            'installation_id' => $this->installationId(),
            'order_id' => $orderId,
            'invoice_id' => $invoiceId,
            'service_id' => $serviceId,
            'product_id' => $productId,
            'product_group_id' => (int) ($product['gid'] ?? 0),
            'service_label' => (string) ($service['domain'] ?? ''),
            'subtotal' => $this->money($invoice['subtotal'] ?? '0'),
            'credit' => $this->money($invoice['credit'] ?? '0'),
            'total_due' => $this->money($invoice['total'] ?? ($order['amount'] ?? '0')),
            'order_status' => (string) ($order['status'] ?? ''),
        ];
        $payload = [
            'provider' => $provider,
            'configuration' => $configuration,
            'pricing' => $pricing,
            'whmcs' => $whmcs,
            'labels' => isset($publicationPayload['labels']) && is_array($publicationPayload['labels'])
                ? $publicationPayload['labels']
                : [],
        ];
        $configurationHash = hash('sha256', $this->canonicalJson($configuration));
        $priceHash = hash('sha256', $this->canonicalJson($pricing));
        $cartHash = hash('sha256', $this->canonicalJson($whmcs));
        $payloadJson = $this->canonicalJson($payload);

        $existing = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('order_id', $orderId)
            ->where('service_id', $serviceId)
            ->where('state', 'draft')
            ->orderByDesc('id')
            ->first();
        if ($existing !== null) {
            $existing = (array) $existing;
            if ((string) ($existing['payload_json'] ?? '') === $payloadJson) {
                return (string) $existing['snapshot_uuid'];
            }
            Capsule::table('mod_securiacevps_order_snapshots')
                ->where('snapshot_uuid', (string) $existing['snapshot_uuid'])
                ->update(['state' => 'superseded', 'updated_at' => date('Y-m-d H:i:s')]);
        }

        $uuid = $this->uuidV4();
        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_order_snapshots')->insert([
            'snapshot_uuid' => $uuid,
            'installation_id' => $this->installationId(),
            'order_id' => $orderId,
            'service_id' => $serviceId,
            'product_id' => $productId,
            'product_group_id' => (int) ($product['gid'] ?? 0),
            'mapping_version' => $mappingVersion,
            'catalog_version' => (string) ($mapping['rust_catalog_version'] ?? ''),
            'pricing_profile_version' => (string) ($publicationPayload['pricing_profile_version'] ?? ''),
            'state' => 'draft',
            'payload_json' => $payloadJson,
            'configuration_hash' => $configurationHash,
            'price_hash' => $priceHash,
            'cart_total_hash' => $cartHash,
            'quote_expires_at' => date('Y-m-d H:i:s', time() + max(60, $quoteTtlSeconds)),
            'paid_at' => null,
            'sealed_at' => null,
            'supersedes_snapshot_uuid' => $existing !== null
                ? (string) ($existing['snapshot_uuid'] ?? '')
                : null,
            'created_at' => $now,
            'updated_at' => $now,
        ]);
        return $uuid;
    }

    public function sealOrder(int $orderId, int $invoiceId): int
    {
        $invoice = $this->row('tblinvoices', 'id', $invoiceId);
        $order = $this->row('tblorders', 'id', $orderId);
        if ($invoice === null || strcasecmp((string) ($invoice['status'] ?? ''), 'Paid') !== 0) {
            return 0;
        }
        if ($order !== null && in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
            return 0;
        }
        $rows = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('order_id', $orderId)
            ->where('state', 'draft')
            ->get();
        $sealed = 0;
        $now = date('Y-m-d H:i:s');
        foreach ($rows as $item) {
            $row = (array) $item;
            if (strtotime((string) ($row['quote_expires_at'] ?? '1970-01-01')) < time()) {
                Capsule::table('mod_securiacevps_order_snapshots')
                    ->where('snapshot_uuid', (string) $row['snapshot_uuid'])
                    ->update(['state' => 'invalid', 'updated_at' => $now]);
                continue;
            }
            Capsule::table('mod_securiacevps_order_snapshots')
                ->where('snapshot_uuid', (string) $row['snapshot_uuid'])
                ->where('state', 'draft')
                ->update([
                    'state' => 'sealed',
                    'paid_at' => $now,
                    'sealed_at' => $now,
                    'updated_at' => $now,
                ]);
            $sealed++;
        }
        return $sealed;
    }

    /** @return list<array<string,mixed>> */
    private function selectedOptions(int $serviceId): array
    {
        $rows = Capsule::table('tblhostingconfigoptions')->where('relid', $serviceId)->get();
        $out = [];
        foreach ($rows as $item) {
            $selected = (array) $item;
            $value = Capsule::table('mod_contabo_config_option_value_link')
                ->where('whmcs_sub_id', (int) ($selected['optionid'] ?? 0))
                ->first();
            $value = $value !== null ? (array) $value : [];
            $option = $value !== []
                ? Capsule::table('mod_contabo_config_option_link')
                    ->where('id', (int) ($value['option_link_id'] ?? 0))
                    ->first()
                : null;
            $option = $option !== null ? (array) $option : [];
            $out[] = [
                'whmcs_config_id' => (int) ($selected['configid'] ?? 0),
                'whmcs_sub_id' => (int) ($selected['optionid'] ?? 0),
                'quantity' => max(1, (int) ($selected['qty'] ?? 1)),
                'machine_code' => (string) ($value['contabo_value_key'] ?? ''),
                'label' => (string) ($value['contabo_label'] ?? ''),
                'dimension' => (string) ($option['dimension_key'] ?? ''),
            ];
        }
        usort($out, static function (array $a, array $b): int {
            return $a['whmcs_config_id'] <=> $b['whmcs_config_id'];
        });
        return $out;
    }

    /** @return array<string,mixed>|null */
    private function row(string $table, string $column, int $id): ?array
    {
        if ($id <= 0) {
            return null;
        }
        $row = Capsule::table($table)->where($column, $id)->first();
        return $row !== null ? (array) $row : null;
    }

    /** @param mixed $value */
    private function money($value): string
    {
        $raw = trim((string) $value);
        return $raw === '' ? '0.00' : $raw;
    }

    private function installationId(): string
    {
        $value = Capsule::table('mod_securiacevps_schema')
            ->where('key', 'installation_id')
            ->value('value');
        if ($value === null || trim((string) $value) === '') {
            throw new \RuntimeException('SecuriAce VPS installation identity is missing');
        }
        return (string) $value;
    }

    /** @param mixed $value */
    private function canonicalJson($value): string
    {
        $encoded = json_encode($this->normalize($value), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if (!is_string($encoded)) {
            throw new \RuntimeException('Unable to encode paid-order snapshot');
        }
        return $encoded;
    }

    /** @param mixed $value @return mixed */
    private function normalize($value)
    {
        if (!is_array($value)) {
            return $value;
        }
        $expected = 0;
        $list = true;
        foreach ($value as $key => $_item) {
            if ($key !== $expected++) {
                $list = false;
                break;
            }
        }
        if (!$list) {
            ksort($value, SORT_STRING);
        }
        foreach ($value as $key => $item) {
            $value[$key] = $this->normalize($item);
        }
        return $value;
    }

    private function uuidV4(): string
    {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
