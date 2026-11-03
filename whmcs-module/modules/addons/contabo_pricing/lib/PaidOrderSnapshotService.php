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
     * @param array{mapping_version:string, catalog_version:string, payload:array<string,mixed>}|null $recordedSource
     *        rebuild from the customer's own recorded mapping/snapshot instead
     *        of the product's current published mapping (admin re-quote only)
     * @return string snapshot UUID
     */
    public function createDraft(
        int $orderId,
        int $invoiceId,
        int $serviceId,
        ?int $quoteTtlSeconds = null,
        ?array $recordedSource = null
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
        if ($recordedSource !== null) {
            $mappingVersion = (string) $recordedSource['mapping_version'];
            $catalogVersion = (string) $recordedSource['catalog_version'];
            $publicationPayload = (array) $recordedSource['payload'];
            $provider = isset($publicationPayload['provider']) && is_array($publicationPayload['provider'])
                ? $publicationPayload['provider']
                : [];
            foreach (['sku_id', 'region_id', 'image_id'] as $required) {
                if (trim((string) ($provider[$required] ?? '')) === '') {
                    throw new \RuntimeException('Recorded quote is missing provider.' . $required);
                }
            }
        } else {
            $current = $this->currentPublication($productId);
            $mappingVersion = $current['mapping_version'];
            $catalogVersion = $current['catalog_version'];
            $publicationPayload = $current['payload'];
            $provider = $publicationPayload['provider'];
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
            'catalog_version' => $catalogVersion,
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
     * Which paid WHMCS invoice settles this service's order.
     *
     * WHMCS does not always leave the order's own invoice as the one the
     * customer pays. The recognised, verifiable links are:
     *
     *  - `order`     the invoice the draft was quoted on (normal case);
     *  - `mass_pay`  a Mass Pay invoice whose `tblinvoiceitems` row has type
     *                `Invoice` and `relid` = the quoted/order invoice;
     *  - `merged`    a Merge Invoices result: a Paid invoice carrying a
     *                `Hosting`/`Setup` item for this service while the quoted
     *                invoice no longer exists;
     *  - `reinvoice` a replacement invoice for the same hosting (type
     *                `Hosting`/`Setup`, `relid` = service id) while the quoted
     *                invoice still exists.
     *
     * A non-direct invoice must be Paid and belong to the service owner;
     * otherwise it is ignored (and reported as a refusal when it was
     * discovered through a link). `$hintInvoiceId` (the hook's invoice) only
     * adds a candidate; it is never trusted without a link.
     *
     * @return array{invoice_id:int, via:string, bound_invoice_id:int, refusal:string}
     */
    public function payingInvoice(
        int $orderId,
        int $serviceId,
        int $hintInvoiceId = 0,
        ?int $draftInvoiceId = null
    ): array {
        $result = ['invoice_id' => 0, 'via' => '', 'bound_invoice_id' => 0, 'refusal' => ''];
        $service = $this->row('tblhosting', 'id', $serviceId);
        $order = $this->row('tblorders', 'id', $orderId);
        if ($service === null || $order === null) {
            return $result;
        }
        $userId = (int) ($service['userid'] ?? 0);
        $orderInvoice = (int) ($order['invoiceid'] ?? 0);
        $bound = ($draftInvoiceId ?? 0) > 0 ? (int) $draftInvoiceId : $orderInvoice;
        $result['bound_invoice_id'] = $bound;
        $own = array_values(array_unique(array_filter([$bound, $orderInvoice], static fn (int $id): bool => $id > 0)));
        $discovered = [];
        if ($own !== []) {
            $items = Capsule::table('tblinvoiceitems')->where('type', 'Invoice')->whereIn('relid', $own)->get();
            foreach ($items as $item) {
                $discovered[] = (int) (((array) $item)['invoiceid'] ?? 0);
            }
        }
        $items = Capsule::table('tblinvoiceitems')->where('type', 'Hosting')->where('relid', $serviceId)->get();
        foreach ($items as $item) {
            $discovered[] = (int) (((array) $item)['invoiceid'] ?? 0);
        }
        $items = Capsule::table('tblinvoiceitems')->where('type', 'Setup')->where('relid', $serviceId)->get();
        foreach ($items as $item) {
            $discovered[] = (int) (((array) $item)['invoiceid'] ?? 0);
        }
        $candidates = array_values(array_unique(array_filter(
            array_merge([$hintInvoiceId], $own, $discovered),
            static fn (int $id): bool => $id > 0
        )));

        $rank = ['order' => 0, 'mass_pay' => 1, 'merged' => 2, 'reinvoice' => 3];
        $best = null;
        foreach ($candidates as $id) {
            $invoice = $this->row('tblinvoices', 'id', $id);
            if ($invoice === null || strcasecmp((string) ($invoice['status'] ?? ''), 'Paid') !== 0) {
                continue;
            }
            [$via, $why] = $this->invoiceLink($invoice, $bound, $own, $serviceId, $userId);
            if ($via === '') {
                if ($why !== '' && ($id !== $hintInvoiceId || in_array($id, $own, true) || in_array($id, $discovered, true))) {
                    $result['refusal'] = $why;
                }
                continue;
            }
            if ($best === null || $rank[$via] < $rank[$best[1]]) {
                $best = [$id, $via];
            }
        }
        if ($best !== null) {
            $result['invoice_id'] = $best[0];
            $result['via'] = $best[1];
            $result['refusal'] = '';
        }
        return $result;
    }

    /**
     * @param array<string,mixed> $invoice
     * @param list<int> $own
     * @return array{0:string,1:string} [via, refusal reason]
     */
    private function invoiceLink(array $invoice, int $bound, array $own, int $serviceId, int $userId): array
    {
        $id = (int) ($invoice['id'] ?? 0);
        if ($id === $bound) {
            return ['order', ''];
        }
        if ($userId <= 0 || (int) ($invoice['userid'] ?? 0) !== $userId) {
            return ['', sprintf(
                'Paid invoice #%d is linked to service #%d but belongs to client #%d, not the service owner #%d',
                $id,
                $serviceId,
                (int) ($invoice['userid'] ?? 0),
                $userId
            )];
        }
        $mass = false;
        $hosting = false;
        $items = Capsule::table('tblinvoiceitems')->where('invoiceid', $id)->get();
        foreach ($items as $item) {
            $item = (array) $item;
            $type = (string) ($item['type'] ?? '');
            $relid = (int) ($item['relid'] ?? 0);
            if ($type === 'Invoice' && in_array($relid, $own, true)) {
                $mass = true;
            }
            if (($type === 'Hosting' || $type === 'Setup') && $relid === $serviceId) {
                $hosting = true;
            }
        }
        if ($mass) {
            return ['mass_pay', ''];
        }
        if ($hosting) {
            return [$this->row('tblinvoices', 'id', $bound) === null ? 'merged' : 'reinvoice', ''];
        }
        return ['', sprintf(
            'Paid invoice #%d is not linked to the draft\'s invoice #%d or to service #%d by any invoice item, so it cannot settle this order',
            $id,
            $bound,
            $serviceId
        )];
    }

    /**
     * Why an unsealed service with a Paid invoice is still blocked on the
     * paying-invoice link (empty when there is nothing to report).
     */
    public function unsealedNote(int $serviceId): string
    {
        $service = $this->row('tblhosting', 'id', $serviceId);
        if ($service === null) {
            return '';
        }
        $orderId = (int) ($service['orderid'] ?? 0);
        $row = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->whereIn('state', ['draft', 'invalid'])
            ->orderByDesc('id')
            ->first();
        $bound = $row !== null ? $this->draftInvoiceId((array) $row) : null;
        return $this->payingInvoice($orderId, $serviceId, 0, $bound)['refusal'];
    }

    /**
     * Seals the order's drafts once their invoice is Paid. Never refuses a
     * paid order for being late; refuses (marks `invalid`) only a draft that
     * fails verification. Every refusal and every late or drifted seal is
     * written to the Activity Log.
     *
     * `$invoiceId` is the invoice the hook reports. The invoice that actually
     * settles each draft is resolved by payingInvoice(): the quoted invoice,
     * or one linked to it through WHMCS invoice items (mass pay, merge,
     * re-invoice). A Paid invoice with no such link never seals a draft.
     */
    public function sealOrder(int $orderId, int $invoiceId): int
    {
        $order = $this->row('tblorders', 'id', $orderId);
        if ($order === null) {
            return 0;
        }
        if (in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
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
            $serviceId = (int) ($row['service_id'] ?? 0);
            // A draft that fails verification cannot vouch for its own invoice
            // id, so a tampered row is judged against the order's invoice.
            $fault = $this->integrityFault($row, $orderId);
            $paying = $this->payingInvoice(
                $orderId,
                $serviceId,
                $invoiceId,
                $fault === null ? $this->draftInvoiceId($row) : null
            );
            if ($paying['invoice_id'] <= 0) {
                if ($paying['refusal'] !== '') {
                    $this->activity(sprintf(
                        'Contabo paid-order snapshot NOT sealed: order #%d service #%d snapshot %s: %s. It stays blocked and is listed in Maintenance.',
                        $orderId,
                        $serviceId,
                        $uuid,
                        $paying['refusal']
                    ));
                }
                continue;
            }
            $payingId = $paying['invoice_id'];
            $invoice = $this->row('tblinvoices', 'id', $payingId) ?? [];
            if ($fault !== null) {
                Capsule::table('mod_securiacevps_order_snapshots')
                    ->where('snapshot_uuid', $uuid)
                    ->where('state', 'draft')
                    ->update(['state' => 'invalid', 'updated_at' => $now]);
                $this->activity(sprintf(
                    'Contabo paid-order snapshot REFUSED to seal: order #%d invoice #%d service #%d snapshot %s failed verification (%s). The service stays blocked; an admin can re-quote it from Maintenance.',
                    $orderId,
                    $payingId,
                    $serviceId,
                    $uuid,
                    $fault
                ));
                continue;
            }
            $expires = strtotime((string) ($row['quote_expires_at'] ?? ''));
            $late = $expires === false || $expires < time();
            $drift = $this->priceDrift($row, $invoice, $paying['via'] === 'order');
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
            if ($paying['via'] !== 'order') {
                $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
                $this->activity(sprintf(
                    'WARNING: Contabo snapshot sealed via a linked invoice: order #%d service #%d snapshot %s was quoted on invoice #%d and settled by Paid invoice #%d (%s); paid total %s, quoted total %s. Sealed at the quoted terms.',
                    $orderId,
                    $serviceId,
                    $uuid,
                    $paying['bound_invoice_id'],
                    $payingId,
                    $paying['via'],
                    $this->money($invoice['total'] ?? '0'),
                    $this->money(is_array($payload) ? ($payload['whmcs']['total_due'] ?? '0') : '0')
                ));
            }
            if ($late) {
                $this->activity(sprintf(
                    'WARNING: Contabo late payment sealed: order #%d invoice #%d service #%d snapshot %s was paid after its quote window (%s); sealed at the quoted terms.',
                    $orderId,
                    $payingId,
                    $serviceId,
                    $uuid,
                    (string) ($row['quote_expires_at'] ?? 'no expiry recorded')
                ));
            }
            if ($drift !== []) {
                $this->activity(sprintf(
                    'WARNING: Contabo price drift at seal: order #%d invoice #%d service #%d snapshot %s keeps the quoted amounts (%s). The sealed price is never raised.',
                    $orderId,
                    $payingId,
                    $serviceId,
                    $uuid,
                    implode('; ', $drift)
                ));
            }
        }
        return $sealed;
    }

    /**
     * Seals any verified draft of one service whose invoice (or linked paying
     * invoice) is Paid. Used by the PreModuleCreate gate as a safety net when
     * OrderPaid did not seal (hook failure, ordering gap, payment on a
     * linked invoice). Returns how many drafts were sealed.
     */
    public function sealPaidService(int $serviceId): int
    {
        $service = $this->row('tblhosting', 'id', $serviceId);
        $orderId = $service === null ? 0 : (int) ($service['orderid'] ?? 0);
        $order = $this->row('tblorders', 'id', $orderId);
        if ($order === null) {
            return 0;
        }
        return $this->sealOrder($orderId, (int) ($order['invoiceid'] ?? 0));
    }

    /**
     * Seals the drafts of every order a paid invoice settles: the invoice's
     * own order, the orders of the invoices a Mass Pay invoice covers, and
     * the services named by its Hosting/Setup items (merged, re-invoiced).
     * Each draft is still verified by sealOrder() before it seals.
     */
    public function sealForPaidInvoice(int $invoiceId): int
    {
        if ($invoiceId <= 0) {
            return 0;
        }
        $orderIds = [];
        $covered = [$invoiceId];
        $services = [];
        $items = Capsule::table('tblinvoiceitems')->where('invoiceid', $invoiceId)->get();
        foreach ($items as $item) {
            $item = (array) $item;
            $type = (string) ($item['type'] ?? '');
            $relid = (int) ($item['relid'] ?? 0);
            if ($type === 'Invoice' && $relid > 0) {
                $covered[] = $relid;
            } elseif (($type === 'Hosting' || $type === 'Setup') && $relid > 0) {
                $services[] = $relid;
            }
        }
        foreach (Capsule::table('tblorders')->whereIn('invoiceid', $covered)->get() as $order) {
            $orderIds[] = (int) (((array) $order)['id'] ?? 0);
        }
        foreach (array_unique($services) as $serviceId) {
            $service = $this->row('tblhosting', 'id', (int) $serviceId);
            $orderIds[] = $service === null ? 0 : (int) ($service['orderid'] ?? 0);
        }
        $count = 0;
        foreach (array_values(array_unique(array_filter($orderIds, static fn (int $id): bool => $id > 0))) as $orderId) {
            $count += $this->sealOrder($orderId, $invoiceId);
        }
        return $count;
    }

    /**
     * Marks the drafts of an order whose invoice was cancelled as `invalid`.
     * A cancelled invoice ends the draft's validity; recovery after an
     * un-cancel and payment is the admin re-quote. A draft whose service is
     * still named by another live (non-cancelled) invoice is kept: that is a
     * re-invoice, and paying it seals the draft through its link.
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
            $drafts = Capsule::table('mod_securiacevps_order_snapshots')
                ->where('order_id', $orderId)
                ->where('state', 'draft')
                ->get();
            foreach ($drafts as $draft) {
                $draft = (array) $draft;
                if ($this->hasLiveReplacementInvoice((int) ($draft['service_id'] ?? 0), $invoiceId)) {
                    $this->activity(sprintf(
                        'Contabo paid-order snapshot: invoice #%d was cancelled but service #%d has a replacement invoice; the draft stays open and seals if that invoice is paid.',
                        $invoiceId,
                        (int) ($draft['service_id'] ?? 0)
                    ));
                    continue;
                }
                $count += (int) Capsule::table('mod_securiacevps_order_snapshots')
                    ->where('snapshot_uuid', (string) ($draft['snapshot_uuid'] ?? ''))
                    ->where('state', 'draft')
                    ->update(['state' => 'invalid', 'updated_at' => $now]);
            }
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

    private function hasLiveReplacementInvoice(int $serviceId, int $exceptInvoiceId): bool
    {
        $service = $this->row('tblhosting', 'id', $serviceId);
        $userId = $service === null ? 0 : (int) ($service['userid'] ?? 0);
        foreach (['Hosting', 'Setup'] as $type) {
            $items = Capsule::table('tblinvoiceitems')->where('type', $type)->where('relid', $serviceId)->get();
            foreach ($items as $item) {
                $id = (int) (((array) $item)['invoiceid'] ?? 0);
                if ($id <= 0 || $id === $exceptInvoiceId) {
                    continue;
                }
                $invoice = $this->row('tblinvoices', 'id', $id);
                if ($invoice !== null
                    && (int) ($invoice['userid'] ?? 0) === $userId
                    && strcasecmp((string) ($invoice['status'] ?? ''), 'Cancelled') !== 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param array<string,mixed> $row
     */
    private function draftInvoiceId(array $row): ?int
    {
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload) || !isset($payload['whmcs']) || !is_array($payload['whmcs'])) {
            return null;
        }
        $id = (int) ($payload['whmcs']['invoice_id'] ?? 0);
        return $id > 0 ? $id : null;
    }

    /**
     * Read-only plan for an admin re-quote: the guards, the paying invoice,
     * and where the SKU, region, image and renewal would come from.
     *
     * Sources, best first:
     *  - `recorded_mapping`  the customer's own mapping version is still a
     *                        published/superseded publication that agrees with
     *                        the verified draft row;
     *  - `recorded_snapshot` the verified draft row itself (its mapping
     *                        publication is gone);
     *  - none                nothing verifiable was recorded. The current
     *                        mapping may differ, so an explicit admin override
     *                        that shows the diff is required.
     *
     * @return array<string,mixed>
     */
    public function requotePlan(int $serviceId): array
    {
        $plan = [
            'ok' => false, 'code' => '', 'message' => '', 'snapshot_uuid' => '',
            'ready' => false, 'needs_override' => false, 'source' => 'none',
            'mapping_version' => '', 'diff' => [], 'diff_token' => '', 'recorded' => null,
            'order_id' => 0, 'invoice_id' => 0, 'bound_invoice_id' => 0, 'via' => '', 'reason' => '',
        ];
        $fail = static function (array $plan, string $code, string $message): array {
            $plan['code'] = $code;
            $plan['message'] = $message;
            return $plan;
        };
        $service = $this->row('tblhosting', 'id', $serviceId);
        if ($service === null) {
            return $fail($plan, 'service_not_found', 'Service #' . $serviceId . ' was not found.');
        }
        $productId = (int) ($service['packageid'] ?? 0);
        $product = $this->row('tblproducts', 'id', $productId);
        if ($product === null || !in_array((string) ($product['servertype'] ?? ''), self::NATIVE_MODULES, true)) {
            return $fail($plan, 'not_native_product', 'Service #' . $serviceId . ' is not a native VPS product.');
        }
        $sealed = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->where('state', 'sealed')
            ->orderByDesc('id')
            ->first();
        if ($sealed !== null) {
            $plan['ok'] = true;
            $plan['snapshot_uuid'] = (string) (((array) $sealed)['snapshot_uuid'] ?? '');
            return $fail($plan, 'already_sealed', 'Service #' . $serviceId . ' already has a sealed snapshot; nothing to do.');
        }
        $orderId = (int) ($service['orderid'] ?? 0);
        $order = $this->row('tblorders', 'id', $orderId);
        if ($order === null) {
            return $fail($plan, 'no_order', 'Service #' . $serviceId . ' has no order.');
        }
        $plan['order_id'] = $orderId;
        if (in_array(strtolower((string) ($order['status'] ?? '')), ['fraud', 'cancelled'], true)) {
            return $fail($plan, 'order_blocked', 'Order #' . $orderId . ' is ' . strtolower((string) $order['status']) . '.');
        }

        $previousRows = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->whereIn('state', ['draft', 'invalid', 'superseded'])
            ->orderByDesc('id')
            ->get();
        $trusted = null;
        $newest = null;
        $whyNot = 'no earlier snapshot row records this order\'s quote';
        foreach ($previousRows as $item) {
            $row = (array) $item;
            $newest = $newest ?? $row;
            $fault = $this->integrityFault($row, $orderId);
            if ($fault === null) {
                $trusted = $row;
                break;
            }
            if ($row === $newest) {
                $whyNot = 'the newest earlier snapshot row failed verification (' . $fault . ')';
            }
        }
        $boundHint = $trusted !== null ? $this->draftInvoiceId($trusted) : null;
        $paying = $this->payingInvoice($orderId, $serviceId, 0, $boundHint);
        $plan['invoice_id'] = $paying['invoice_id'];
        $plan['bound_invoice_id'] = $paying['bound_invoice_id'];
        $plan['via'] = $paying['via'];
        if ($paying['invoice_id'] <= 0) {
            if ($paying['refusal'] !== '') {
                $plan['reason'] = $paying['refusal'];
                return $fail($plan, 'paying_invoice_refused', 'Refused: ' . $paying['refusal'] . '. Fix the invoice in WHMCS; it cannot be overridden here.');
            }
            return $fail($plan, 'invoice_not_paid', 'Order #' . $orderId . ' has no Paid invoice (own, merged, mass-pay or re-invoice); a snapshot can only be re-quoted after payment.');
        }

        $recorded = $trusted !== null ? $this->recordedSource($trusted, $whyNot) : null;
        if ($recorded !== null) {
            $plan['ready'] = true;
            $plan['source'] = $recorded['source'];
            $plan['mapping_version'] = (string) $recorded['mapping_version'];
            $plan['recorded'] = $recorded;
            $plan['ok'] = true;
            $plan['code'] = 'ready';
            $plan['message'] = 'Ready to re-quote from the ' . ($recorded['source'] === 'recorded_mapping' ? 'recorded mapping ' . $recorded['mapping_version'] : 'recorded snapshot');
            return $plan;
        }
        $plan['reason'] = $whyNot;

        try {
            $current = $this->currentPublication($productId);
        } catch (\Throwable $e) {
            return $fail($plan, 'draft_failed', 'Cannot re-quote: ' . $whyNot . ', and the current mapping is unusable (' . substr($e->getMessage(), 0, 160) . ').');
        }
        $diff = $this->quoteDiff($newest, $current['payload'], $service);
        $plan['ready'] = true;
        $plan['needs_override'] = true;
        $plan['mapping_version'] = (string) $current['mapping_version'];
        $plan['diff'] = $diff;
        $plan['diff_token'] = hash('sha256', $this->canonicalJson([
            $serviceId, $orderId, $paying['invoice_id'], $paying['bound_invoice_id'], $current['mapping_version'], $diff,
        ]));
        $plan['code'] = 'needs_override';
        $plan['message'] = sprintf(
            'Service #%d cannot be re-quoted on its own recorded quote (%s). The current mapping %s would be used instead: %s. Confirm the override to proceed.',
            $serviceId,
            $whyNot,
            $current['mapping_version'],
            $this->describeDiff($diff)
        );
        return $plan;
    }

    /**
     * Admin recovery for a paid service that has no sealed snapshot (a
     * tampered or invalidated draft, one left over from 1.1.3, or a payment
     * that arrived on a linked invoice). Rebuilds the contract from the
     * order/service records and the customer's OWN recorded mapping version
     * (or recorded snapshot), binds it to the invoice the customer paid, then
     * seals it. Refuses unless a linked invoice is Paid and the order is not
     * Fraud/Cancelled. Older non-sealed rows are kept as `superseded`.
     *
     * When nothing verifiable was recorded the current published mapping is
     * only used through an explicit override: `$override` plus the
     * `$diffToken` returned by requotePlan() for exactly the diff shown to the
     * admin. The override and its diff are written to the Activity Log.
     *
     * @return array{ok:bool, code:string, message:string, snapshot_uuid:string}
     */
    public function requote(int $serviceId, int $adminId = 0, bool $override = false, string $diffToken = ''): array
    {
        $fail = static function (string $code, string $message): array {
            return ['ok' => false, 'code' => $code, 'message' => $message, 'snapshot_uuid' => ''];
        };
        $plan = $this->requotePlan($serviceId);
        if (!$plan['ready']) {
            return [
                'ok' => (bool) $plan['ok'],
                'code' => (string) $plan['code'],
                'message' => (string) $plan['message'],
                'snapshot_uuid' => (string) $plan['snapshot_uuid'],
            ];
        }
        $orderId = (int) $plan['order_id'];
        $recorded = null;
        if ($plan['needs_override']) {
            if (!$override) {
                return $fail('needs_override', (string) $plan['message']);
            }
            if ($diffToken === '' || !hash_equals((string) $plan['diff_token'], $diffToken)) {
                return $fail('diff_changed', 'The order, invoice or current mapping changed since the diff was shown. Review the new diff and confirm again.');
            }
        } else {
            $recorded = $plan['recorded'];
        }
        $boundInvoiceId = (int) $plan['bound_invoice_id'];
        if ($boundInvoiceId <= 0) {
            return $fail('draft_failed', 'Order #' . $orderId . ' has no invoice to bind the snapshot to.');
        }

        $previous = Capsule::table('mod_securiacevps_order_snapshots')
            ->where('service_id', $serviceId)
            ->whereIn('state', ['draft', 'invalid'])
            ->orderByDesc('id')
            ->first();
        $previous = $previous !== null ? (array) $previous : null;
        try {
            $uuid = $this->createDraft($orderId, $boundInvoiceId, $serviceId, $this->configuredQuoteTtl(), $recorded);
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
        $count = $this->sealOrder($orderId, (int) $plan['invoice_id']);
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
        $source = $plan['needs_override'] ? 'ADMIN OVERRIDE onto the current mapping' : (string) $plan['source'];
        $this->activity(sprintf(
            'Contabo snapshot re-quoted by admin #%d: service #%d order #%d quoted on invoice #%d, paid on invoice #%d (%s), sealed as %s on mapping %s from %s (%s). Setup, recurring and total are those billed on the order; SKU, region, image and renewal come from the source named here.',
            $adminId,
            $serviceId,
            $orderId,
            $boundInvoiceId,
            (int) $plan['invoice_id'],
            (string) $plan['via'],
            $uuid,
            $mapping,
            $source,
            $note
        ));
        if ($plan['needs_override']) {
            $this->activity(sprintf(
                'WARNING: Contabo re-quote OVERRIDE by admin #%d for service #%d: %s Reason no recorded quote was used: %s.',
                $adminId,
                $serviceId,
                $this->describeDiff((array) $plan['diff']),
                (string) $plan['reason']
            ));
        }
        return [
            'ok' => true,
            'code' => 'requoted',
            'message' => 'Service #' . $serviceId . ' re-quoted and sealed (' . $uuid . ') from ' . $source . '. Run Create on the service again.',
            'snapshot_uuid' => $uuid,
        ];
    }

    /**
     * Paid native-VPS services that have only non-sealed snapshot rows, for the
     * Maintenance re-quote panel. `status` is `ready` (recorded quote
     * available), `override` (needs an explicit admin override; `diff` and
     * `diff_token` are set) or `refused` (`reason` says why; not recoverable
     * from here).
     *
     * @return list<array<string,mixed>>
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
            $plan = $this->requotePlan((int) $serviceId);
            if (in_array($plan['code'], ['service_not_found', 'not_native_product', 'already_sealed', 'no_order', 'order_blocked', 'invoice_not_paid'], true)) {
                continue;
            }
            $refused = $plan['code'] === 'paying_invoice_refused';
            $out[] = [
                'service_id' => (int) $serviceId,
                'order_id' => (int) $plan['order_id'],
                'invoice_id' => (int) ($plan['invoice_id'] > 0 ? $plan['invoice_id'] : $plan['bound_invoice_id']),
                'states' => implode(', ', array_values(array_unique($states))),
                'via' => (string) $plan['via'],
                'status' => $refused ? 'refused' : ($plan['needs_override'] ? 'override' : ($plan['ready'] ? 'ready' : 'refused')),
                'source' => (string) $plan['source'],
                'reason' => $refused || !$plan['ready'] ? (string) $plan['message'] : (string) $plan['reason'],
                'diff' => $plan['diff'],
                'diff_token' => (string) $plan['diff_token'],
            ];
        }
        return $out;
    }

    /**
     * Where a verified draft row's SKU/region/image/renewal can be rebuilt
     * from, or null when the recorded mapping contradicts the row.
     *
     * @param array<string,mixed> $row a row that already passed integrityFault()
     * @return array{source:string, mapping_version:string, catalog_version:string, payload:array<string,mixed>}|null
     */
    private function recordedSource(array $row, string &$why): ?array
    {
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            return null;
        }
        $provider = (array) $payload['provider'];
        $version = trim((string) ($row['mapping_version'] ?? ''));
        if ($version !== '') {
            $publication = Capsule::table('mod_contabo_mapping_publications')
                ->where('mapping_version', $version)
                ->whereIn('state', ['published', 'superseded'])
                ->first();
            $publicationPayload = $publication !== null
                ? json_decode((string) (((array) $publication)['payload_json'] ?? ''), true)
                : null;
            if (is_array($publicationPayload) && isset($publicationPayload['provider']) && is_array($publicationPayload['provider'])
                && trim((string) ($publicationPayload['provider']['sku_id'] ?? '')) !== '') {
                foreach (['sku_id', 'region_id', 'image_id'] as $key) {
                    if ((string) ($publicationPayload['provider'][$key] ?? '') !== (string) ($provider[$key] ?? '')) {
                        $why = 'the recorded mapping ' . $version . ' disagrees with the draft row on provider.' . $key;
                        return null;
                    }
                }
                return [
                    'source' => 'recorded_mapping',
                    'mapping_version' => $version,
                    'catalog_version' => (string) ($row['catalog_version'] ?? ''),
                    'payload' => $publicationPayload,
                ];
            }
        }
        $configuration = (array) $payload['configuration'];
        $pricing = (array) $payload['pricing'];
        return [
            'source' => 'recorded_snapshot',
            'mapping_version' => $version,
            'catalog_version' => (string) ($row['catalog_version'] ?? ''),
            'payload' => [
                'provider' => $provider,
                'pricing_profile_version' => (string) ($row['pricing_profile_version'] ?? ''),
                'compatibility_version' => (string) ($configuration['compatibility_version'] ?? ''),
                'management_code' => (string) ($configuration['management_code'] ?? 'self_managed'),
                'tax_policy' => (string) ($pricing['tax_policy'] ?? 'whmcs'),
                'renewal_amount' => (string) ($pricing['renewal'] ?? '0.00'),
                'labels' => isset($payload['labels']) && is_array($payload['labels']) ? $payload['labels'] : [],
            ],
        ];
    }

    /**
     * Old (newest earlier row, verified or not) vs new (current mapping).
     *
     * @param array<string,mixed>|null $oldRow
     * @param array<string,mixed> $newPayload
     * @param array<string,mixed> $service
     * @return list<array{field:string, old:string, new:string, changed:bool}>
     */
    private function quoteDiff(?array $oldRow, array $newPayload, array $service): array
    {
        $old = $oldRow !== null ? json_decode((string) ($oldRow['payload_json'] ?? ''), true) : null;
        $old = is_array($old) ? $old : [];
        $pairs = [
            'SKU' => [$old['provider']['sku_id'] ?? null, $newPayload['provider']['sku_id'] ?? ''],
            'Region' => [$old['provider']['region_id'] ?? null, $newPayload['provider']['region_id'] ?? ''],
            'Image' => [$old['provider']['image_id'] ?? null, $newPayload['provider']['image_id'] ?? ''],
            'Renewal' => [$old['pricing']['renewal'] ?? null, $newPayload['renewal_amount'] ?? ($service['amount'] ?? '0.00')],
        ];
        $diff = [];
        foreach ($pairs as $field => [$was, $now]) {
            $oldText = $was === null || (string) $was === '' ? '(not recorded)' : (string) $was;
            $newText = (string) $now;
            $diff[] = [
                'field' => $field,
                'old' => $oldText,
                'new' => $newText,
                'changed' => $oldText !== $newText,
            ];
        }
        return $diff;
    }

    /** @param array<int,array<string,mixed>> $diff */
    private function describeDiff(array $diff): string
    {
        $parts = [];
        foreach ($diff as $d) {
            $parts[] = sprintf(
                '%s %s -> %s%s',
                (string) ($d['field'] ?? ''),
                (string) ($d['old'] ?? ''),
                (string) ($d['new'] ?? ''),
                !empty($d['changed']) ? '' : ' (same)'
            );
        }
        return implode('; ', $parts);
    }

    /**
     * The product's current published mapping.
     *
     * @return array{mapping_version:string, catalog_version:string, payload:array<string,mixed>}
     */
    private function currentPublication(int $productId): array
    {
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
        $payload = json_decode((string) (((array) $publication)['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            throw new \RuntimeException('Published mapping payload is invalid');
        }
        $provider = isset($payload['provider']) && is_array($payload['provider']) ? $payload['provider'] : [];
        foreach (['sku_id', 'region_id', 'image_id'] as $required) {
            if (trim((string) ($provider[$required] ?? '')) === '') {
                throw new \RuntimeException('Published mapping is missing provider.' . $required);
            }
        }
        return [
            'mapping_version' => $mappingVersion,
            'catalog_version' => (string) ($mapping['rust_catalog_version'] ?? ''),
            'payload' => $payload,
        ];
    }

    /**
     * Null when the draft verifies; otherwise the reason it must not be sealed.
     *
     * @param array<string,mixed> $row
     */
    private function integrityFault(array $row, int $orderId): ?string
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
        if ((int) ($whmcs['invoice_id'] ?? 0) <= 0) {
            return 'draft is not bound to an invoice';
        }
        if ((int) ($whmcs['service_id'] ?? 0) !== (int) ($row['service_id'] ?? 0)) {
            return 'draft is bound to a different service';
        }
        // The provider section is not covered by a stored hash (no schema
        // change in 1.1.4), so tie the fields that are mirrored into the
        // hashed configuration back to it and require a SKU to be present.
        $provider = $payload['provider'];
        $configuration = $payload['configuration'];
        if (trim((string) ($provider['sku_id'] ?? '')) === '') {
            return 'provider section has no sku_id';
        }
        if ((string) ($provider['region_id'] ?? '') !== (string) ($configuration['region'] ?? '')
            || (string) ($provider['image_id'] ?? '') !== (string) ($configuration['image_id'] ?? '')) {
            return 'provider section disagrees with the hashed configuration';
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
    private function priceDrift(array $row, array $invoice, bool $compareInvoiceTotal = true): array
    {
        $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
        if (!is_array($payload)) {
            return [];
        }
        $service = $this->row('tblhosting', 'id', (int) ($row['service_id'] ?? 0));
        $pairs = [
            'invoice total' => [$payload['whmcs']['total_due'] ?? null, $compareInvoiceTotal ? ($invoice['total'] ?? null) : null],
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
