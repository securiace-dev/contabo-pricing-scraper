<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Creates and seals the immutable provisioning contract for a paid WHMCS
 * service. It reads only a published mapping version and the order/service
 * records created by WHMCS. Provisioning never reads the current catalog.
 *
 * A draft is bound to its invoice: it stays sealable until that invoice is
 * paid or cancelled. `quote_expires_at` only marks when a payment counts as
 * "late" (default: the later of `snapshot.quote_ttl_seconds` after checkout and
 * the invoice due date). A late payment is still sealed, after the same
 * integrity checks as an on-time one plus a logged price-drift comparison,
 * because the invoice the customer paid is the binding record. A draft whose
 * hashes or order/invoice/service binding do not verify is never sealed.
 */
final class PaidOrderSnapshotService
{
    public const SETTING_QUOTE_TTL = 'snapshot.quote_ttl_seconds';
    public const DEFAULT_QUOTE_TTL = 1800;
    public const MIN_QUOTE_TTL = 60;
    public const MAX_QUOTE_TTL = 2592000;

    private const NATIVE_MODULES = ['securiacevps', 'contabo_vps'];

    /**
     * Seconds a checkout draft stays "on time" before the invoice due date is
     * considered. Reads `mod_contabo_settings.snapshot.quote_ttl_seconds`;
     * anything missing, non-numeric or out of range falls back to the default
     * or the nearest bound.
     */
    public function configuredQuoteTtl(): int
    {
        try {
            $raw = Capsule::table('mod_contabo_settings')->where('key', self::SETTING_QUOTE_TTL)->value('value');
        } catch (\Throwable $e) {
            return self::DEFAULT_QUOTE_TTL;
        }
        $raw = $raw === null ? '' : trim((string) $raw);
        if ($raw === '' || !ctype_digit($raw)) {
            return self::DEFAULT_QUOTE_TTL;
        }
        return max(self::MIN_QUOTE_TTL, min(self::MAX_QUOTE_TTL, (int) $raw));
    }

    /**
     * @param int|null $quoteTtlSeconds exact lifetime to use; null = configured TTL
     *                                  extended to the invoice due date
     * @return string snapshot UUID
     */
    public function createDraft(
        int $orderId,
        int $invoiceId,
        int $serviceId,
        ?int $quoteTtlSeconds = null
    ): string {
        SchemaHealth::requirePublicationSupport();
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
        if ($product === null || !in_array((string) ($product['servertype'] ?? ''), self::NATIVE_MODULES, true)) {
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
            'quote_expires_at' => date('Y-m-d H:i:s', $this->quoteExpiry($invoice, $quoteTtlSeconds)),
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

    /**
     * Seals the order's drafts once its invoice is Paid. Never refuses a
     * paid order for being late; refuses (marks `invalid`) only a draft that
     * fails verification. Every refusal and every late or drifted seal is
     * written to the Activity Log.
     */
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
            $uuid = (string) ($row['snapshot_uuid'] ?? '');
            $fault = $this->integrityFault($row, $orderId, $invoiceId);
            if ($fault !== null) {
                Capsule::table('mod_securiacevps_order_snapshots')
                    ->where('snapshot_uuid', $uuid)
                    ->where('state', 'draft')
                    ->update(['state' => 'invalid', 'updated_at' => $now]);
                $this->activity(sprintf(
                    'Contabo paid-order snapshot REFUSED to seal: order #%d invoice #%d service #%d snapshot %s failed verification (%s). The service stays blocked; an admin can re-quote it from Maintenance.',
                    $orderId,
                    $invoiceId,
                    (int) ($row['service_id'] ?? 0),
                    $uuid,
                    $fault
                ));
                continue;
            }
            $expires = strtotime((string) ($row['quote_expires_at'] ?? ''));
            $late = $expires === false || $expires < time();
            $drift = $this->priceDrift($row, $invoice);
            Capsule::table('mod_securiacevps_order_snapshots')
                ->where('snapshot_uuid', $uuid)
                ->where('state', 'draft')
                ->update([
                    'state' => 'sealed',
                    'paid_at' => $now,
                    'sealed_at' => $now,
                    'updated_at' => $now,
                ]);
            $sealed++;
            if ($late) {
                $this->activity(sprintf(
                    'WARNING: Contabo late payment sealed: order #%d invoice #%d service #%d snapshot %s was paid after its quote window (%s); sealed at the quoted terms.',
                    $orderId,
                    $invoiceId,
                    (int) ($row['service_id'] ?? 0),
                    $uuid,
                    (string) ($row['quote_expires_at'] ?? 'no expiry recorded')
                ));
            }
            if ($drift !== []) {
                $this->activity(sprintf(
                    'WARNING: Contabo price drift at seal: order #%d invoice #%d service #%d snapshot %s keeps the quoted amounts (%s). The sealed price is never raised.',
                    $orderId,
                    $invoiceId,
                    (int) ($row['service_id'] ?? 0),
                    $uuid,
                    implode('; ', $drift)
                ));
            }
        }
        return $sealed;
    }

    /**
     * Seals any verified draft of one service whose invoice is Paid. Used by
     * the PreModuleCreate gate as a safety net when OrderPaid did not seal
     * (hook failure, ordering gap). Returns how many drafts were sealed.
     */
    public function sealPaidService(int $serviceId): int
    {
        $service = $this->row('tblhosting', 'id', $serviceId);
        $orderId = $service === null ? 0 : (int) ($service['orderid'] ?? 0);
        $order = $this->row('tblorders', 'id', $orderId);
        $invoiceId = $order === null ? 0 : (int) ($order['invoiceid'] ?? 0);
        if ($invoiceId <= 0) {
            return 0;
        }
        return $this->sealOrder($orderId, $invoiceId);
    }

    /**
     * Marks the drafts of an order whose invoice was cancelled as `invalid`.
     * A cancelled invoice ends the draft's validity; recovery after an
     * un-cancel and payment is the admin re-quote.
     */
    public function invalidateForCancelledInvoice(int $invoiceId): int
    {
        if ($invoiceId <= 0) {
            return 0;
        }
        $count = 0;
        $now = date('Y-m-d H:i:s');
        $orders = Capsule::table('tblorders')->where('invoiceid', $invoiceId)->get();
        foreach ($orders as $order) {
            $orderId = (int) (((array) $order)['id'] ?? 0);
            if ($orderId <= 0) {
                continue;
            }
            $count += (int) Capsule::table('mod_securiacevps_order_snapshots')
                ->where('order_id', $orderId)
                ->where('state', 'draft')
                ->update(['state' => 'invalid', 'updated_at' => $now]);
        }
        if ($count > 0) {
            $this->activity(sprintf(
                'Contabo paid-order snapshot: invoice #%d was cancelled, %d draft snapshot(s) marked invalid.',
                $invoiceId,
                $count
            ));
        }
        return $count;
    }

    /**
     * Admin recovery for a paid service that has no sealed snapshot (a
     * tampered or invalidated draft, or one left over from 1.1.3). Rebuilds the
     * contract from the live order/service/invoice records, so the price is the
     * one WHMCS billed and the customer paid, and the current published mapping,
     * then seals it. Refuses unless the invoice is Paid and the order is not
     * Fraud/Cancelled. Older non-sealed rows are kept as `superseded`.
     *
     * @return array{ok:bool, code:string, message:string, snapshot_uuid:string}
     */
    public function requote(int $serviceId, int $adminId = 0): array
    {
        $fail = static function (string $code, string $message): array {
            return ['ok' => false, 'code' => $code, 'message' => $message, 'snapshot_uuid' => ''];
        };
        $service = $this->row('tblhosting', 'id', $serviceId);
        if ($service === null) {
            return $fail('service_not_found', 'Service #' . $serviceId . ' was not found.');
        }
        $product = $this->row('tblproducts', 'id', (int) ($service['packageid'] ?? 0));
        if ($product === null || !in_array((string) ($product['servertype'] ?? ''), self::NATIVE_MODULES, true)) {
            return $fail('not_native_product', 'Service #' . $serviceId . ' is not a native VPS product.');
        }
        $sealed = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        if ($sealed !== null) {
            return [
                'ok' => true,
                'code' => 'already_sealed',
                'message' => 'Service #' . $serviceId . ' already has a sealed snapshot; nothing to do.',
                'snapshot_uuid' => (string) (((array) $sealed)['snapshot_uuid'] ?? ''),
            ];
        }
        $orderId = (int) ($service['orderid'] ?? 0);
        $order = $this->row('tblorders', 'id', $orderId);
        if ($order === null) {
            return $fail('no_order', 'Service #' . $serviceId . ' has no order.');
        }
        if (in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
            return $fail('order_blocked', 'Order #' . $orderId . ' is ' . strtolower((string) $order['status']) . '.');
        }
        $invoiceId = (int) ($order['invoiceid'] ?? 0);
        $invoice = $this->row('tblinvoices', 'id', $invoiceId);
        if ($invoice === null || strcasecmp((string) ($invoice['status'] ?? ''), 'Paid') !== 0) {
            return $fail('invoice_not_paid', 'Order #' . $orderId . ' has no Paid invoice; a snapshot can only be re-quoted after payment.');
        }

        $previous = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->whereIn('state', ['draft', 'invalid'])
            ->orderByDesc('id')
            ->first();
        $previous = $previous !== null ? (array) $previous : null;
        try {
            $uuid = $this->createDraft($orderId, $invoiceId, $serviceId, $this->configuredQuoteTtl());
        } catch (\Throwable $e) {
            $this->activity('Contabo re-quote of service #' . $serviceId . ' failed: ' . substr($e->getMessage(), 0, 200));
            return $fail('draft_failed', 'Could not rebuild the snapshot: ' . substr($e->getMessage(), 0, 200));
        }
        if ($uuid === '') {
            return $fail('draft_failed', 'Could not rebuild the snapshot for service #' . $serviceId . '.');
        }
        $now = date('Y-m-d H:i:s');
        Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'invalid')
            ->update(['state' => 'superseded', 'updated_at' => $now]);
        $count = $this->sealOrder($orderId, $invoiceId);
        $sealedNow = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('snapshot_uuid', $uuid)
            ->where('state', 'sealed')
            ->exists();
        if ($count < 1 || !$sealedNow) {
            return $fail('seal_refused', 'The rebuilt snapshot did not seal; see the Activity Log.');
        }
        $note = $previous === null
            ? 'no earlier snapshot row'
            : 'replaced a ' . (string) ($previous['state'] ?? '') . ' row, mapping '
                . (string) ($previous['mapping_version'] ?? '?');
        $newRow = Capsule::table('mod_securiacevps_order_snapshots')->where('snapshot_uuid', $uuid)->first();
        $mapping = $newRow !== null ? (string) (((array) $newRow)['mapping_version'] ?? '?') : '?';
        $this->activity(sprintf(
            'Contabo snapshot re-quoted by admin #%d: service #%d order #%d invoice #%d sealed as %s on mapping %s (%s). Amounts are those billed on the paid invoice.',
            $adminId,
            $serviceId,
            $orderId,
            $invoiceId,
            $uuid,
            $mapping,
            $note
        ));
        return [
            'ok' => true,
            'code' => 'requoted',
            'message' => 'Service #' . $serviceId . ' re-quoted and sealed (' . $uuid . '). Run Create on the service again.',
            'snapshot_uuid' => $uuid,
        ];
    }

    /**
     * Paid native-VPS services that have only non-sealed snapshot rows, for the
     * Maintenance re-quote panel.
     *
     * @return list<array{service_id:int, order_id:int, invoice_id:int, states:string}>
     */
    public function stuckPaidServices(int $limit = 50): array
    {
        $rows = Capsule::table('mod_securiacevps_order_snapshots')
            ->whereIn('state', ['draft', 'invalid'])
            ->orderByDesc('id')
            ->get();
        $byService = [];
        foreach ($rows as $item) {
            $row = (array) $item;
            $serviceId = (int) ($row['service_id'] ?? 0);
            if ($serviceId > 0) {
                $byService[$serviceId][] = (string) ($row['state'] ?? '');
            }
        }
        $out = [];
        foreach ($byService as $serviceId => $states) {
            if (count($out) >= $limit) {
                break;
            }
            $hasSealed = Capsule::table('mod_securiacevps_order_snapshots')
                ->where('service_id', $serviceId)
                ->where('state', 'sealed')
                ->exists();
            if ($hasSealed) {
                continue;
            }
            $service = $this->row('tblhosting', 'id', (int) $serviceId);
            $order = $service === null ? null : $this->row('tblorders', 'id', (int) ($service['orderid'] ?? 0));
            $invoice = $order === null ? null : $this->row('tblinvoices', 'id', (int) ($order['invoiceid'] ?? 0));
            if ($invoice === null || strcasecmp((string) ($invoice['status'] ?? ''), 'Paid') !== 0) {
                continue;
            }
            if (in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
                continue;
            }
            $out[] = [
                'service_id' => (int) $serviceId,
                'order_id' => (int) ($order['id'] ?? 0),
                'invoice_id' => (int) ($invoice['id'] ?? 0),
                'states' => implode(', ', array_values(array_unique($states))),
            ];
        }
        return $out;
    }

    /**
     * Null when the draft verifies; otherwise the reason it must not be sealed.
     *
     * @param array<string,mixed> $row
     */
    private function integrityFault(array $row, int $orderId, int $invoiceId): ?string
    {
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            return 'payload is not valid JSON';
        }
        foreach (['provider', 'configuration', 'pricing', 'whmcs'] as $key) {
            if (!isset($payload[$key]) || !is_array($payload[$key])) {
                return 'payload is missing its ' . $key . ' contract';
            }
        }
        $checks = [
            'configuration_hash' => 'configuration',
            'price_hash' => 'pricing',
            'cart_total_hash' => 'whmcs',
        ];
        foreach ($checks as $column => $section) {
            if ((string) ($row[$column] ?? '') !== hash('sha256', $this->canonicalJson($payload[$section]))) {
                return $column . ' does not match the payload';
            }
        }
        $whmcs = $payload['whmcs'];
        if ((int) ($row['order_id'] ?? 0) !== $orderId || (int) ($whmcs['order_id'] ?? 0) !== $orderId) {
            return 'draft is bound to a different order';
        }
        if ((int) ($whmcs['invoice_id'] ?? 0) !== $invoiceId) {
            return 'draft is bound to a different invoice';
        }
        if ((int) ($whmcs['service_id'] ?? 0) !== (int) ($row['service_id'] ?? 0)) {
            return 'draft is bound to a different service';
        }
        return null;
    }

    /**
     * Differences between the quoted amounts and what WHMCS bills now.
     *
     * @param array<string,mixed> $row
     * @param array<string,mixed> $invoice
     * @return list<string>
     */
    private function priceDrift(array $row, array $invoice): array
    {
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            return [];
        }
        $service = $this->row('tblhosting', 'id', (int) ($row['service_id'] ?? 0));
        $pairs = [
            'invoice total' => [$payload['whmcs']['total_due'] ?? null, $invoice['total'] ?? null],
            'first payment' => [$payload['pricing']['setup'] ?? null, $service['firstpaymentamount'] ?? null],
            'recurring' => [$payload['pricing']['recurring'] ?? null, $service['amount'] ?? null],
        ];
        $out = [];
        foreach ($pairs as $label => [$quoted, $current]) {
            if ($quoted === null || $current === null) {
                continue;
            }
            if (round((float) $quoted, 2) !== round((float) $current, 2)) {
                $out[] = $label . ' quoted ' . $this->money($quoted) . ' now ' . $this->money($current);
            }
        }
        return $out;
    }

    /** @param array<string,mixed>|null $invoice */
    private function quoteExpiry(?array $invoice, ?int $explicitTtl): int
    {
        if ($explicitTtl !== null) {
            return time() + max(self::MIN_QUOTE_TTL, $explicitTtl);
        }
        $expires = time() + $this->configuredQuoteTtl();
        $due = trim((string) ($invoice['duedate'] ?? ''));
        if ($due !== '' && strncmp($due, '0000', 4) !== 0) {
            $dueTs = strtotime(substr($due, 0, 10) . ' 23:59:59');
            if ($dueTs !== false && $dueTs > $expires) {
                $expires = $dueTs;
            }
        }
        return $expires;
    }

    private function activity(string $message): void
    {
        try {
            if (function_exists('logActivity')) {
                logActivity($message);
            }
        } catch (\Throwable $e) {
            // logging must never change a seal outcome
        }
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
                : Capsule::table('mod_contabo_config_option_link')
                    ->where('whmcs_option_id', (int) ($selected['configid'] ?? 0))
                    ->first();
            $option = $option !== null ? (array) $option : [];
            if ($option === []) {
                throw new \RuntimeException(
                    'A selected configurable option has no canonical Contabo mapping'
                );
            }
            $isQuantity = (int) ($option['optiontype'] ?? 0) === 4;
            if (!$isQuantity && $value === []) {
                throw new \RuntimeException(
                    'A selected configurable-option value has no canonical Contabo mapping'
                );
            }
            $dimension = (string) ($option['dimension_key'] ?? '');
            $quantityCode = trim((string) ($option['default_value'] ?? ''));
            if ($quantityCode === '') {
                $quantityCode = strtolower(trim((string) preg_replace(
                    '/[^a-z0-9]+/i',
                    '_',
                    $dimension
                ), '_'));
            }
            $out[] = [
                'whmcs_config_id' => (int) ($selected['configid'] ?? 0),
                'whmcs_sub_id' => $isQuantity ? 0 : (int) ($selected['optionid'] ?? 0),
                'quantity' => $isQuantity
                    ? max(0, (int) ($selected['qty'] ?? 0))
                    : max(1, (int) ($selected['qty'] ?? 1)),
                'machine_code' => $isQuantity
                    ? $quantityCode
                    : (string) ($value['contabo_value_key'] ?? ''),
                'label' => $isQuantity
                    ? $dimension
                    : (string) ($value['contabo_label'] ?? ''),
                'dimension' => $dimension,
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
