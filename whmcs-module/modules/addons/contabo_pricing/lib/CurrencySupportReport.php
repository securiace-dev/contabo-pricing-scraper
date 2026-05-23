<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * 0.5.1 — READ-ONLY multi-currency exposure report (the in-addon equivalent of
 * the production currency audit). Performs ZERO writes.
 *
 * v1 of the addon's configurable-option pricing + revenue path is INR-only
 * (the supported currency is {@see WhmcsConfigOptionsAdapter::INR_CURRENCY_ID}).
 * This report tells an admin whether any non-INR services exist and — crucially —
 * whether any are on contabo_pricing-mapped products, which is what turns the
 * multi-currency guard from a LATENT safeguard into an ACTIVE risk.
 *
 * "Meaningful" service statuses = Active / Suspended / Pending. Cancelled /
 * Terminated / Fraud are excluded from the primary risk verdict.
 *
 * Aggregation is done in PHP (not via DB JOIN/GROUP BY) so the logic is unit-
 * testable against the array-based FakeCapsule and portable across WHMCS schema
 * variants. The raw reads live in small overridable protected methods.
 *
 * IMPORTANT: tblhosting's recurring charge column is `amount` (there is NO
 * `recurringamount` raw column on a live WHMCS install). tblclients.currency is
 * the client's billing currency id.
 *
 * PHP 7.4 + 8.x polyglot: no enums, match, readonly, promotion, named args,
 * union types, str_starts_with.
 */
class CurrencySupportReport
{
    /** @var list<string> */
    private const MEANINGFUL_STATUSES = ['Active', 'Suspended', 'Pending'];

    /**
     * Build the full read-only report.
     *
     * @return array<string,mixed>
     */
    public function build(): array
    {
        $baseCurrencyId = WhmcsConfigOptionsAdapter::INR_CURRENCY_ID;

        // Currencies (mark the base/supported one).
        $currencies = [];
        $baseCode = '';
        foreach ($this->fetchCurrencies() as $c) {
            $c = (array) $c;
            $id = (int) ($c['id'] ?? 0);
            $isBase = ($id === $baseCurrencyId);
            if ($isBase) {
                $baseCode = (string) ($c['code'] ?? '');
            }
            $currencies[] = [
                'id'      => $id,
                'code'    => (string) ($c['code'] ?? ''),
                'prefix'  => (string) ($c['prefix'] ?? ''),
                'suffix'  => (string) ($c['suffix'] ?? ''),
                'rate'    => (float) ($c['rate'] ?? 0.0),
                'is_base' => $isBase,
            ];
        }

        // client id -> currency id
        $clientCurrency = [];
        foreach ($this->fetchClients() as $cl) {
            $cl = (array) $cl;
            $clientCurrency[(int) ($cl['id'] ?? 0)] = (int) ($cl['currency'] ?? 0);
        }

        // Active mapped product ids.
        $mappingPresent = $this->mappingTablePresent();
        $mappedProducts = [];
        $mappedProductIds = [];
        if ($mappingPresent) {
            foreach ($this->fetchActiveMappings() as $m) {
                $m = (array) $m;
                $pid = (int) ($m['product_id'] ?? 0);
                $mappedProducts[] = ['product_id' => $pid, 'profile_id' => (int) ($m['profile_id'] ?? 0)];
                $mappedProductIds[$pid] = true;
            }
        }

        // Aggregate services in PHP.
        $countsIndex = [];            // "currencyId|status" => count
        $nonInrMeaningfulTotal = 0;
        $mappedLiveServicesTotal = 0;
        $nonInrMapped = [];
        $excludedNonInr = 0;          // non-INR services in excluded statuses (Cancelled/Terminated/Fraud)

        foreach ($this->fetchServices() as $svc) {
            $svc = (array) $svc;
            $status = (string) ($svc['domainstatus'] ?? '');
            $userId = (int) ($svc['userid'] ?? 0);
            $currencyId = $clientCurrency[$userId] ?? 0;
            $packageId = (int) ($svc['packageid'] ?? 0);
            $isMeaningful = in_array($status, self::MEANINGFUL_STATUSES, true);
            $isNonBase = ($currencyId !== 0 && $currencyId !== $baseCurrencyId);
            $isMapped = isset($mappedProductIds[$packageId]);

            if (!$isMeaningful) {
                if ($isNonBase) {
                    $excludedNonInr++;
                }
                continue;
            }

            $key = $currencyId . '|' . $status;
            $countsIndex[$key] = ($countsIndex[$key] ?? 0) + 1;

            if ($isNonBase) {
                $nonInrMeaningfulTotal++;
            }
            if ($isMapped) {
                $mappedLiveServicesTotal++;
                if ($isNonBase) {
                    $nonInrMapped[] = [
                        'service_id'    => (int) ($svc['id'] ?? 0),
                        'client_id'     => $userId,
                        'packageid'     => $packageId,
                        'status'        => $status,
                        'billingcycle'  => (string) ($svc['billingcycle'] ?? ''),
                        'amount'        => (float) ($svc['amount'] ?? 0.0), // WHMCS service amount (tblhosting.amount)
                        'currency_code' => $this->codeFor($currencies, $currencyId),
                    ];
                }
            }
        }

        // Flatten the counts index into a stable, sorted list.
        $meaningfulCounts = [];
        foreach ($countsIndex as $key => $count) {
            $parts = explode('|', $key, 2);
            $cid = (int) $parts[0];
            $meaningfulCounts[] = [
                'currency_id' => $cid,
                'code'        => $this->codeFor($currencies, $cid),
                'status'      => $parts[1],
                'count'       => $count,
            ];
        }
        usort($meaningfulCounts, static function (array $a, array $b): int {
            return ($a['currency_id'] <=> $b['currency_id']) ?: strcmp($a['status'], $b['status']);
        });

        return [
            'base_currency_id'          => $baseCurrencyId,
            'base_currency_code'        => $baseCode,
            'currencies'                => $currencies,
            'meaningful_counts'         => $meaningfulCounts,
            'non_inr_meaningful_total'  => $nonInrMeaningfulTotal,
            'excluded_non_inr_total'    => $excludedNonInr, // Cancelled/Terminated/Fraud, separately counted
            'mapping_table_present'     => $mappingPresent,
            'mapped_products'           => $mappedProducts,
            'mapped_live_services_total' => $mappedLiveServicesTotal,
            'non_inr_mapped'            => $nonInrMapped,
            'verdict'                   => $this->classify($nonInrMeaningfulTotal, count($nonInrMapped)),
        ];
    }

    /**
     * Pure verdict classifier (unit-testable without a DB):
     *   - any non-INR service on a mapped product  => active risk
     *   - else any non-INR meaningful service       => latent (unmapped)
     *   - else                                      => clean
     */
    public function classify(int $nonInrMeaningfulTotal, int $nonInrMappedCount): string
    {
        if ($nonInrMappedCount > 0) {
            return 'non_inr_mapped_active_risk';
        }
        if ($nonInrMeaningfulTotal > 0) {
            return 'non_inr_unmapped';
        }
        return 'no_non_inr';
    }

    /**
     * Human-readable one-liner for a verdict (for the admin view / logs).
     */
    public function verdictLabel(string $verdict): string
    {
        if ($verdict === 'non_inr_mapped_active_risk') {
            return 'Non-INR services are mapped to contabo_pricing products — the multi-currency guard is an ACTIVE risk.';
        }
        if ($verdict === 'non_inr_unmapped') {
            return 'Non-INR services exist but none are mapped to contabo_pricing products — latent, not an immediate issue.';
        }
        return 'No non-INR meaningful services found — the multi-currency guard is fully latent.';
    }

    /**
     * @param list<array<string,mixed>> $currencies
     */
    private function codeFor(array $currencies, int $currencyId): string
    {
        foreach ($currencies as $c) {
            if ((int) $c['id'] === $currencyId) {
                return (string) $c['code'];
            }
        }
        return (string) $currencyId;
    }

    // ── READ-ONLY data access (overridable for tests). No writes anywhere. ──

    /** @return iterable<array<string,mixed>|object> */
    protected function fetchCurrencies(): iterable
    {
        return Capsule::table('tblcurrencies')->get();
    }

    /** @return iterable<array<string,mixed>|object> */
    protected function fetchClients(): iterable
    {
        // Project only what we aggregate (FakeCapsule returns full rows, so tests
        // are unaffected; real WHMCS avoids materializing every client column).
        return Capsule::table('tblclients')->get(['id', 'currency']);
    }

    /** @return iterable<array<string,mixed>|object> */
    protected function fetchServices(): iterable
    {
        return Capsule::table('tblhosting')->get(['id', 'userid', 'packageid', 'domain', 'domainstatus', 'billingcycle', 'amount']);
    }

    protected function mappingTablePresent(): bool
    {
        return Capsule::schema()->hasTable('mod_contabo_mapping');
    }

    /** @return iterable<array<string,mixed>|object> */
    protected function fetchActiveMappings(): iterable
    {
        return Capsule::table('mod_contabo_mapping')->where('active', 1)->get();
    }
}
