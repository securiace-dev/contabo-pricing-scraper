<?php
declare(strict_types=1);

namespace ContaboPricing;

use Throwable;
use WHMCS\Database\Capsule;

/**
 * Phase-A backfill: for every active, Contabo-mapped service that does not
 * yet have a service-policy row, insert one with `policy = lifetime` and
 * `locked_price = current tblhosting.amount` (the REAL recurring-charge column;
 * `recurringamount` is an API/model field, not a raw tblhosting column).
 *
 * The CURRENT recurring amount is preserved — NOT the catalog price — so any
 * special deal a client originally received stays intact through the engine's
 * observe → opt-in → enforce rollout.
 *
 * Idempotent: every service that already has a policy row is skipped.
 *
 * This command does not write to tblhosting; it only inserts into
 * mod_contabo_service_policy.
 */
class BackfillCommand
{
    /**
     * Run the backfill across every mapped, active service.
     *
     * @return array{created:int,skipped:int,errors:int} per-service counts
     */
    public function run(): array
    {
        $created = 0;
        $skipped = 0;
        $errors  = 0;

        $defaultCurrency = $this->resolveDefaultCurrency();
        $now = date('Y-m-d H:i:s');

        // Candidate loading runs BEFORE the per-service try/catch, so a schema/
        // query failure here must be handled at the command level — log clearly
        // and exit safely instead of throwing uncaught out of run().
        try {
            $services = $this->loadCandidateServices();
        } catch (Throwable $e) {
            if (function_exists('logActivity')) {
                logActivity('Contabo Pricing BackfillCommand: candidate load failed '
                    . '(schema/query error) — ' . $e->getMessage());
            }
            return ['created' => 0, 'skipped' => 0, 'errors' => 1];
        }

        foreach ($services as $row) {
            $serviceId = (int) $row['service_id'];
            try {
                $existing = Capsule::table('mod_contabo_service_policy')
                    ->where('service_id', $serviceId)
                    ->exists();

                if ($existing) {
                    $skipped++;
                    continue;
                }

                $lockedPrice = (float) $row['service_amount'];
                if ($lockedPrice <= 0) {
                    // Free / one-time / invalid amount — skip; the engine's
                    // edge-case matrix handles these via skip_reasons later.
                    $skipped++;
                    continue;
                }

                Capsule::table('mod_contabo_service_policy')->insert([
                    'service_id'           => $serviceId,
                    'policy'               => 'lifetime',
                    'locked_price'         => $lockedPrice,
                    'locked_currency'      => $defaultCurrency,
                    'allow_auto_decrease'  => 0,
                    'notes'                => 'Phase A backfill: grandfathered at current price',
                    'created_at'           => $now,
                    'updated_at'           => $now,
                ]);

                $created++;
            } catch (Throwable $e) {
                $errors++;
                if (function_exists('logActivity')) {
                    logActivity(
                        'Contabo Pricing BackfillCommand error on service '
                        . $serviceId . ': ' . $e->getMessage()
                    );
                }
            }
        }

        return ['created' => $created, 'skipped' => $skipped, 'errors' => $errors];
    }

    /**
     * Returns rows of { service_id, service_amount } for every mapped service in
     * a billable status. service_amount is read from the REAL tblhosting.`amount`
     * column (NOT `recurringamount`, which is an API/model field, not a raw
     * column). Billable excludes Cancelled/Terminated/Fraud; Suspended is included
     * so suspended-but-billable services still get a policy row.
     *
     * Two reads (active mapped product ids, then services) instead of a JOIN —
     * portable + unit-testable, and avoids the wrong-column SELECT.
     *
     * @return list<array{service_id:int, service_amount:float}>
     */
    protected function loadCandidateServices(): array
    {
        $productIds = $this->activeMappedProductIds();
        if (empty($productIds)) {
            return [];
        }

        $rows = Capsule::table('tblhosting')
            ->whereIn('packageid', $productIds)
            ->whereNotIn('domainstatus', ['Cancelled', 'Terminated', 'Fraud'])
            ->select(['id', 'amount'])
            ->get();

        $out = [];
        foreach ($rows as $r) {
            $r = (array) $r; // real Capsule returns stdClass; normalise
            $out[] = [
                'service_id'     => (int) ($r['id'] ?? 0),
                'service_amount' => (float) ($r['amount'] ?? 0.0),
            ];
        }
        return $out;
    }

    /**
     * Active Contabo-mapped product ids (replaces the prior JOIN).
     *
     * @return list<int>
     */
    private function activeMappedProductIds(): array
    {
        $ids = [];
        foreach (Capsule::table('mod_contabo_mapping')->where('active', 1)->get() as $m) {
            $m = (array) $m;
            $pid = (int) ($m['product_id'] ?? 0);
            if ($pid > 0) {
                $ids[$pid] = $pid;
            }
        }
        return array_values($ids);
    }

    /**
     * WHMCS base currency code from tblconfiguration; falls back to INR.
     */
    private function resolveDefaultCurrency(): string
    {
        try {
            $value = Capsule::table('tblconfiguration')
                ->where('setting', 'DefaultCurrency')
                ->value('value');
            if (is_string($value) && $value !== '') {
                // DefaultCurrency stores a currency-table id, NOT an ISO code,
                // on some WHMCS versions. Resolve to ISO via tblcurrencies.
                if (ctype_digit($value)) {
                    $code = Capsule::table('tblcurrencies')
                        ->where('id', (int) $value)
                        ->value('code');
                    if (is_string($code) && $code !== '') {
                        return strtoupper(substr($code, 0, 3));
                    }
                } else {
                    return strtoupper(substr($value, 0, 3));
                }
            }
        } catch (Throwable $e) {
            // fall through to default
        }
        return 'INR';
    }
}
