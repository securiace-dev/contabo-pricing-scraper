<?php
declare(strict_types=1);

namespace ContaboPricing;

use Throwable;
use WHMCS\Database\Capsule;

/**
 * Phase-A backfill: for every active, Contabo-mapped service that does not
 * yet have a service-policy row, insert one with `policy = lifetime` and
 * `locked_price = current tblhosting.recurringamount`.
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

        $services = $this->loadCandidateServices();

        foreach ($services as $row) {
            $serviceId = (int) $row->service_id;
            try {
                $existing = Capsule::table('mod_contabo_service_policy')
                    ->where('service_id', $serviceId)
                    ->exists();

                if ($existing) {
                    $skipped++;
                    continue;
                }

                $lockedPrice = (float) $row->recurringamount;
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
     * Returns rows of { service_id, recurringamount } for every active service
     * that is mapped to a Contabo profile via mod_contabo_mapping.
     *
     * "Active" excludes Cancelled and Terminated; Suspended is included so
     * suspended-but-billable services still get a policy row.
     *
     * @return list<object>
     */
    private function loadCandidateServices(): array
    {
        $rows = Capsule::table('tblhosting as h')
            ->join('mod_contabo_mapping as m', 'm.product_id', '=', 'h.packageid')
            ->whereNotIn('h.domainstatus', ['Cancelled', 'Terminated', 'Fraud'])
            ->where('m.active', 1)
            ->select('h.id as service_id', 'h.recurringamount')
            ->get();

        // Capsule may return a Collection; normalise to array for PHP 7.4.
        if (is_object($rows) && method_exists($rows, 'all')) {
            return $rows->all();
        }
        return is_array($rows) ? $rows : [];
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
