<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Watchdog — observe-only invoice auditor.
 *
 * Hooks (registered separately by Agent D in hooks.php):
 *   - `InvoiceCreation`  → onInvoiceCreation($vars['invoiceid'])
 *     Per WHMCS docs invoice items are STILL MUTABLE at this hook. The plan
 *     explicitly forbids using that mutability — we observe only and log
 *     mismatches between the line-item amount and the latest applied
 *     mod_contabo_price_decision for the same service.
 *
 *   - `InvoiceCreated`   → onInvoiceCreated($vars['invoiceid'])
 *     Post-delivery audit. Mismatch here is more serious (the invoice has
 *     already been generated and may be visible to the client) so we log at
 *     higher severity and also alert admin.
 *
 * NON-NEGOTIABLE: this class MUST NEVER mutate tblinvoiceitems, tblinvoices,
 * tblhosting, or any other WHMCS data. It is read-only. If you find yourself
 * writing a `->update()` call inside this file, you are off the plan; back
 * out and re-read Deliverable 3.
 *
 * PHP 7.4 + 8.x polyglot.
 */
final class Watchdog
{
    /** Tolerance for currency comparisons: 1 cent. */
    private const PRICE_TOLERANCE = 0.01;

    /**
     * Pre-final watchdog. Logs (no mutation) any line-item / latest-applied-
     * decision mismatch on the given invoice.
     */
    public function onInvoiceCreation(int $invoiceId): void
    {
        $this->audit($invoiceId, false);
    }

    /**
     * Post-final watchdog. Same observation, higher severity, with admin
     * alert if a mismatch is found.
     */
    public function onInvoiceCreated(int $invoiceId): void
    {
        $this->audit($invoiceId, true);
    }

    /**
     * Walk tblinvoiceitems for the invoice; for each Hosting line, look up the
     * latest applied mod_contabo_price_decision and log if amounts diverge.
     *
     * @param bool $strict If true (InvoiceCreated path) we ALSO call
     *                     sendAdminNotification(); if false (InvoiceCreation
     *                     path) we only call logActivity().
     */
    private function audit(int $invoiceId, bool $strict): void
    {
        $items = $this->fetchHostingItems($invoiceId);

        foreach ($items as $item) {
            $serviceId  = (int) ($item['relid'] ?? 0);
            $itemAmount = (float) ($item['amount'] ?? 0.0);
            if ($serviceId <= 0) {
                continue;
            }

            $decision = $this->latestAppliedDecisionFor($serviceId);
            if ($decision === null) {
                continue; // no engine decision yet → nothing to audit against
            }

            $expected = (float) ($decision['proposed_new_price'] ?? 0.0);
            if ($expected <= 0.0) {
                continue;
            }

            if (abs($expected - $itemAmount) <= self::PRICE_TOLERANCE) {
                continue; // match within tolerance
            }

            $message = sprintf(
                'Contabo Pricing Watchdog [%s]: invoice #%d item for service #%d'
                . ' shows amount %s but latest applied decision #%s expected %s',
                $strict ? 'InvoiceCreated' : 'InvoiceCreation',
                $invoiceId,
                $serviceId,
                number_format($itemAmount, 4, '.', ''),
                isset($decision['id']) ? (string) $decision['id'] : '?',
                number_format($expected, 4, '.', '')
            );

            if (function_exists('logActivity')) {
                \logActivity($message);
            }

            if ($strict && function_exists('sendAdminNotification')) {
                \sendAdminNotification(
                    'Account',
                    'Contabo Pricing: invoice line amount mismatch',
                    $message
                );
            }

            // Hard rule: NEVER mutate. No update/insert/delete calls below.
        }
    }

    /**
     * Fetch Hosting-type line items for the given invoice.
     *
     * @return list<array<string,mixed>>
     */
    private function fetchHostingItems(int $invoiceId): array
    {
        $rows = Capsule::table('tblinvoiceitems')
            ->where('invoiceid', $invoiceId)
            ->where('type', 'Hosting')
            ->get();

        return $this->normaliseRows($rows);
    }

    /**
     * Look up the latest decision with applied=true for the given service.
     *
     * @return array<string,mixed>|null
     */
    private function latestAppliedDecisionFor(int $serviceId): ?array
    {
        $row = Capsule::table('mod_contabo_price_decision')
            ->where('service_id', $serviceId)
            ->where('applied', 1)
            ->orderByDesc('decided_at')
            ->first();

        if ($row === null) {
            return null;
        }
        if (is_array($row)) {
            return $row;
        }
        if (is_object($row)) {
            return (array) $row;
        }
        return null;
    }

    /**
     * Normalise a Capsule collection / array / iterable of stdClass rows to a
     * list of associative arrays.
     *
     * @param mixed $rows
     * @return list<array<string,mixed>>
     */
    private function normaliseRows($rows): array
    {
        $out = [];
        if (is_iterable($rows)) {
            foreach ($rows as $row) {
                if (is_array($row)) {
                    $out[] = $row;
                } elseif (is_object($row)) {
                    $out[] = (array) $row;
                }
            }
        }
        return $out;
    }
}
