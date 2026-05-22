<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * STATIC-GREP GATE: writes to tblproductconfig* / tblpricing(configoptions)
 * occur ONLY in this file.
 *
 * Phase A.6.1 — amendment 3 (binding). This class is the SOLE write chokepoint
 * for the five WHMCS configurable-option tables:
 *
 *   - tblproductconfiggroups        (the option group)
 *   - tblproductconfigoptions       (an option within a group)
 *   - tblproductconfigoptionssub    (a selectable sub-value of an option)
 *   - tblproductconfiglinks         (group ↔ product link)
 *   - tblpricing  WHERE type='configoptions'  (per-sub-option pricing)
 *
 * No other class — not the syncer, not the AdminController — may raw-write any
 * of those tables. A static grep for `tblproductconfig` / `'configoptions'`
 * writes should turn up THIS FILE ONLY (plus read-only schema verification).
 *
 * Behaviour contract:
 *   - Every public mutator is IDEMPOTENT: re-running with identical inputs is a
 *     no-op (action='noop'); changed inputs update in place (action='updated');
 *     a fresh row inserts (action='created').
 *   - The constructor's $dryRun flag defaults to TRUE. In dry-run NO database
 *     write happens at all; every mutator returns action='dryrun' alongside the
 *     exact payload it WOULD have written. This is the safe default — the
 *     A.6.1 round ships the adapter but executes zero WHMCS writes.
 *   - Real writes (dryRun=false) run inside Capsule::connection()->transaction()
 *     and emit one mod_contabo_config_option_audit row per write, tagged with a
 *     sync_batch_id (passed to the constructor or auto-generated).
 *   - INR-only v1 guard (amendment 10): upsertConfigOptionPricing only writes
 *     tblpricing rows for currencyId === INR_CURRENCY_ID (1). Any other currency
 *     is recorded as skipped with skip_reason='non_inr_currency_unsupported_v1'
 *     and produces NO tblpricing write — never a silent stale row.
 *
 * PHP 7.4 polyglot: typed properties OK; no enums/match/readonly.
 */
final class WhmcsConfigOptionsAdapter
{
    /** WHMCS base currency in this deployment (verified: 1 = INR). */
    public const INR_CURRENCY_ID = 1;

    /** Audit note used when a non-INR currency price write is refused. */
    public const SKIP_NON_INR = 'non_inr_currency_unsupported_v1';

    /** The five tables this chokepoint owns (used by verifySchema()). */
    private const REQUIRED_TABLES = [
        'tblproductconfiggroups'    => ['id', 'name', 'description'],
        'tblproductconfigoptions'   => ['id', 'gid', 'optionname', 'optiontype', 'qtyminimum', 'qtymaximum', 'order', 'hidden'],
        'tblproductconfigoptionssub' => ['id', 'configid', 'optionname', 'sortorder', 'hidden'],
        'tblproductconfiglinks'     => ['id', 'gid', 'pid'],
        'tblpricing'                => ['id', 'type', 'currency', 'relid', 'monthly', 'quarterly', 'semiannually', 'annually', 'biennially', 'triennially', 'msetupfee', 'qsetupfee', 'ssetupfee', 'asetupfee', 'bsetupfee', 'tsetupfee'],
    ];

    /**
     * Six recurring cycle columns on tblpricing, keyed by the cycle name the
     * caller supplies in $cyclePrices. Mirrors WHMCS's column layout.
     *
     * @var array<string,string>
     */
    private const CYCLE_COLUMNS = [
        'monthly'      => 'monthly',
        'quarterly'    => 'quarterly',
        'semiannually' => 'semiannually',
        'annually'     => 'annually',
        'biennially'   => 'biennially',
        'triennially'  => 'triennially',
    ];

    /**
     * Six setup-fee columns on tblpricing, keyed by the same cycle name.
     *
     * @var array<string,string>
     */
    private const SETUP_COLUMNS = [
        'monthly'      => 'msetupfee',
        'quarterly'    => 'qsetupfee',
        'semiannually' => 'ssetupfee',
        'annually'     => 'asetupfee',
        'biennially'   => 'bsetupfee',
        'triennially'  => 'tsetupfee',
    ];

    private bool $dryRun;

    private string $syncBatchId;

    public function __construct(bool $dryRun = true, ?string $syncBatchId = null)
    {
        $this->dryRun = $dryRun;
        $this->syncBatchId = ($syncBatchId !== null && $syncBatchId !== '')
            ? $syncBatchId
            : ('cfgopt-' . date('Ymd-His') . '-' . substr(bin2hex(random_bytes(4)), 0, 8));
    }

    public function isDryRun(): bool
    {
        return $this->dryRun;
    }

    public function syncBatchId(): string
    {
        return $this->syncBatchId;
    }

    // ----------------------------------------------------------------------
    // tblproductconfiggroups
    // ----------------------------------------------------------------------

    /**
     * Upsert a configurable-option group by name.
     *
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    public function upsertGroup(string $name, string $description = ''): array
    {
        $payload = ['name' => $name, 'description' => $description];

        if ($this->dryRun) {
            return $this->dryRunResult('tblproductconfiggroups', $payload, null);
        }

        return $this->write(static function () use ($name, $description, $payload) {
            $existing = Capsule::table('tblproductconfiggroups')
                ->where('name', $name)
                ->first();

            if ($existing !== null) {
                $id = (int) $existing['id'];
                if ((string) ($existing['description'] ?? '') === $description) {
                    return ['id' => $id, 'action' => 'noop', 'table' => 'tblproductconfiggroups', 'payload' => $payload];
                }
                Capsule::table('tblproductconfiggroups')
                    ->where('id', $id)
                    ->update(['description' => $description]);
                return ['id' => $id, 'action' => 'updated', 'table' => 'tblproductconfiggroups', 'payload' => $payload];
            }

            $id = (int) Capsule::table('tblproductconfiggroups')->insertGetId($payload);
            return ['id' => $id, 'action' => 'created', 'table' => 'tblproductconfiggroups', 'payload' => $payload];
        });
    }

    // ----------------------------------------------------------------------
    // tblproductconfiglinks
    // ----------------------------------------------------------------------

    /**
     * Link a group to a WHMCS product (idempotent).
     *
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    public function linkGroupToProduct(int $groupId, int $productId): array
    {
        $payload = ['gid' => $groupId, 'pid' => $productId];

        if ($this->dryRun) {
            return $this->dryRunResult('tblproductconfiglinks', $payload, null);
        }

        return $this->write(static function () use ($groupId, $productId, $payload) {
            $existing = Capsule::table('tblproductconfiglinks')
                ->where('gid', $groupId)
                ->where('pid', $productId)
                ->first();

            if ($existing !== null) {
                return ['id' => (int) $existing['id'], 'action' => 'noop', 'table' => 'tblproductconfiglinks', 'payload' => $payload];
            }

            $id = (int) Capsule::table('tblproductconfiglinks')->insertGetId($payload);
            return ['id' => $id, 'action' => 'created', 'table' => 'tblproductconfiglinks', 'payload' => $payload];
        });
    }

    // ----------------------------------------------------------------------
    // tblproductconfigoptions
    // ----------------------------------------------------------------------

    /**
     * Upsert an option within a group, keyed by (gid, optionname).
     * optiontype: 0=dropdown 1=radio 2=yes/no 3=qty 4=text.
     *
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    public function upsertOption(
        int $groupId,
        string $optionName,
        int $optionType,
        ?int $qtyMin = null,
        ?int $qtyMax = null,
        int $order = 0
    ): array {
        $payload = [
            'gid'        => $groupId,
            'optionname' => $optionName,
            'optiontype' => $optionType,
            'qtyminimum' => $qtyMin !== null ? $qtyMin : 0,
            'qtymaximum' => $qtyMax !== null ? $qtyMax : 0,
            'order'      => $order,
        ];

        if ($this->dryRun) {
            return $this->dryRunResult('tblproductconfigoptions', $payload, null);
        }

        return $this->write(static function () use ($groupId, $optionName, $payload) {
            $existing = Capsule::table('tblproductconfigoptions')
                ->where('gid', $groupId)
                ->where('optionname', $optionName)
                ->first();

            if ($existing !== null) {
                $id = (int) $existing['id'];
                $diff = self::columnsThatChanged($existing, $payload, ['optiontype', 'qtyminimum', 'qtymaximum', 'order']);
                if ($diff === []) {
                    return ['id' => $id, 'action' => 'noop', 'table' => 'tblproductconfigoptions', 'payload' => $payload];
                }
                Capsule::table('tblproductconfigoptions')->where('id', $id)->update($diff);
                return ['id' => $id, 'action' => 'updated', 'table' => 'tblproductconfigoptions', 'payload' => $payload];
            }

            $id = (int) Capsule::table('tblproductconfigoptions')->insertGetId($payload);
            return ['id' => $id, 'action' => 'created', 'table' => 'tblproductconfigoptions', 'payload' => $payload];
        });
    }

    // ----------------------------------------------------------------------
    // tblproductconfigoptionssub
    // ----------------------------------------------------------------------

    /**
     * Upsert a sub-option (selectable value) of an option, keyed by
     * (configid, optionname).
     *
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    public function upsertSubOption(int $optionId, string $optionName, int $sortOrder, bool $hidden = false): array
    {
        $payload = [
            'configid'   => $optionId,
            'optionname' => $optionName,
            'sortorder'  => $sortOrder,
            'hidden'     => $hidden ? 1 : 0,
        ];

        if ($this->dryRun) {
            return $this->dryRunResult('tblproductconfigoptionssub', $payload, null);
        }

        return $this->write(static function () use ($optionId, $optionName, $payload) {
            $existing = Capsule::table('tblproductconfigoptionssub')
                ->where('configid', $optionId)
                ->where('optionname', $optionName)
                ->first();

            if ($existing !== null) {
                $id = (int) $existing['id'];
                $diff = self::columnsThatChanged($existing, $payload, ['sortorder', 'hidden']);
                if ($diff === []) {
                    return ['id' => $id, 'action' => 'noop', 'table' => 'tblproductconfigoptionssub', 'payload' => $payload];
                }
                Capsule::table('tblproductconfigoptionssub')->where('id', $id)->update($diff);
                return ['id' => $id, 'action' => 'updated', 'table' => 'tblproductconfigoptionssub', 'payload' => $payload];
            }

            $id = (int) Capsule::table('tblproductconfigoptionssub')->insertGetId($payload);
            return ['id' => $id, 'action' => 'created', 'table' => 'tblproductconfigoptionssub', 'payload' => $payload];
        });
    }

    // ----------------------------------------------------------------------
    // tblpricing (type=configoptions)
    // ----------------------------------------------------------------------

    /**
     * Upsert the per-sub-option pricing row in tblpricing (type=configoptions,
     * relid = sub-option id), keyed by (type, currency, relid).
     *
     * INR-ONLY v1 GUARD (amendment 10): if $currencyId !== INR_CURRENCY_ID this
     * method writes NOTHING to tblpricing. It records an audit row with
     * skip_reason='non_inr_currency_unsupported_v1' (real-write mode) and logs,
     * returning action='skipped'. This prevents silent stale non-INR rows.
     *
     * @param array<string,float|int> $cyclePrices keyed by cycle name
     *        (monthly|quarterly|semiannually|annually|biennially|triennially).
     * @param array<string,float|int> $setupFees   same keys; optional.
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>, skip_reason?:string}
     */
    public function upsertConfigOptionPricing(int $subId, int $currencyId, array $cyclePrices, array $setupFees = []): array
    {
        $row = $this->buildPricingRow($subId, $currencyId, $cyclePrices, $setupFees);

        // INR-only guard fires regardless of dry-run vs real-write.
        if ($currencyId !== self::INR_CURRENCY_ID) {
            $this->logSkip($subId, $currencyId);
            if (!$this->dryRun) {
                // Record the deliberate skip in the audit trail (no tblpricing write).
                $this->insertAudit('tblpricing', 'skipped', null, $row, self::SKIP_NON_INR);
            }
            return [
                'id'          => null,
                'action'      => 'skipped',
                'table'       => 'tblpricing',
                'payload'     => $row,
                'skip_reason' => self::SKIP_NON_INR,
            ];
        }

        if ($this->dryRun) {
            return $this->dryRunResult('tblpricing', $row, null);
        }

        return $this->write(function () use ($subId, $currencyId, $row) {
            $existing = Capsule::table('tblpricing')
                ->where('type', 'configoptions')
                ->where('currency', $currencyId)
                ->where('relid', $subId)
                ->first();

            $priceCols = array_merge(array_values(self::CYCLE_COLUMNS), array_values(self::SETUP_COLUMNS));

            if ($existing !== null) {
                $id = (int) $existing['id'];
                $diff = self::columnsThatChanged($existing, $row, $priceCols);
                if ($diff === []) {
                    return ['id' => $id, 'action' => 'noop', 'table' => 'tblpricing', 'payload' => $row];
                }
                Capsule::table('tblpricing')->where('id', $id)->update($diff);
                return ['id' => $id, 'action' => 'updated', 'table' => 'tblpricing', 'payload' => $row];
            }

            $id = (int) Capsule::table('tblpricing')->insertGetId($row);
            return ['id' => $id, 'action' => 'created', 'table' => 'tblpricing', 'payload' => $row];
        });
    }

    // ----------------------------------------------------------------------
    // Schema verification (READ-ONLY)
    // ----------------------------------------------------------------------

    /**
     * Read-only check that the five WHMCS configurable-option tables (and the
     * key columns this adapter touches) exist. Never writes.
     *
     * @return array{ok:bool, missing:list<string>}
     */
    public function verifySchema(): array
    {
        $missing = [];
        $schema = Capsule::schema();

        foreach (self::REQUIRED_TABLES as $table => $columns) {
            if (!$schema->hasTable($table)) {
                $missing[] = $table;
                continue;
            }
            foreach ($columns as $col) {
                if (!$schema->hasColumn($table, $col)) {
                    $missing[] = $table . '.' . $col;
                }
            }
        }

        return ['ok' => $missing === [], 'missing' => $missing];
    }

    // ----------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------

    /**
     * Build the canonical tblpricing(configoptions) row from cycle/setup maps.
     * Unspecified cycles default to 0.0 (WHMCS treats absent as free).
     *
     * @param array<string,float|int> $cyclePrices
     * @param array<string,float|int> $setupFees
     * @return array<string,mixed>
     */
    private function buildPricingRow(int $subId, int $currencyId, array $cyclePrices, array $setupFees): array
    {
        $row = [
            'type'     => 'configoptions',
            'currency' => $currencyId,
            'relid'    => $subId,
        ];
        foreach (self::CYCLE_COLUMNS as $cycle => $col) {
            $row[$col] = isset($cyclePrices[$cycle]) ? (float) $cyclePrices[$cycle] : 0.0;
        }
        foreach (self::SETUP_COLUMNS as $cycle => $col) {
            $row[$col] = isset($setupFees[$cycle]) ? (float) $setupFees[$cycle] : 0.0;
        }
        return $row;
    }

    /**
     * Build a dry-run result envelope.
     *
     * @param array<string,mixed> $payload
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    private function dryRunResult(string $table, array $payload, ?int $id): array
    {
        return ['id' => $id, 'action' => 'dryrun', 'table' => $table, 'payload' => $payload];
    }

    /**
     * Execute a real write inside a transaction, then emit the audit row.
     * The callback returns the result envelope.
     *
     * @param callable():array{id:?int, action:string, table:string, payload:array<string,mixed>} $fn
     * @return array{id:?int, action:string, table:string, payload:array<string,mixed>}
     */
    private function write(callable $fn): array
    {
        $result = Capsule::connection()->transaction($fn);

        // Audit every write attempt (noop included — proves idempotency).
        $this->insertAudit(
            (string) $result['table'],
            (string) $result['action'],
            isset($result['id']) ? (int) $result['id'] : null,
            (array) $result['payload'],
            null
        );

        return $result;
    }

    /**
     * Append one row to mod_contabo_config_option_audit. Best-effort: a missing
     * audit table (e.g. schema v5 not yet migrated) must NOT abort the write,
     * so failures here are swallowed.
     *
     * @param array<string,mixed> $payload
     */
    private function insertAudit(string $table, string $action, ?int $objectId, array $payload, ?string $skipReason): void
    {
        $audit = [
            'sync_batch_id' => $this->syncBatchId,
            'whmcs_table'   => $table,
            'object_id'     => $objectId,
            'action'        => $action,
            'payload_json'  => json_encode($payload),
            'skip_reason'   => $skipReason,
            'created_at'    => date('Y-m-d H:i:s'),
        ];
        try {
            Capsule::table('mod_contabo_config_option_audit')->insert($audit);
        } catch (\Throwable $e) {
            // Audit table may not exist yet in A.6.1; do not break the write.
        }
    }

    private function logSkip(int $subId, int $currencyId): void
    {
        if (function_exists('logActivity')) {
            logActivity(sprintf(
                'Contabo Pricing: skipped config-option pricing for sub #%d currency #%d — %s (only INR synced)',
                $subId,
                $currencyId,
                self::SKIP_NON_INR
            ));
        }
    }

    /**
     * Return the subset of $desired columns whose value differs from $existing.
     * Numeric comparisons are loose so 1 vs '1' and 1.5 vs '1.50' compare equal.
     *
     * @param array<string,mixed> $existing
     * @param array<string,mixed> $desired
     * @param list<string>        $columns
     * @return array<string,mixed>
     */
    private static function columnsThatChanged(array $existing, array $desired, array $columns): array
    {
        $diff = [];
        foreach ($columns as $col) {
            if (!array_key_exists($col, $desired)) {
                continue;
            }
            $want = $desired[$col];
            $have = $existing[$col] ?? null;
            if (is_numeric($want) && is_numeric($have)) {
                if ((float) $want !== (float) $have) {
                    $diff[$col] = $want;
                }
            } elseif ((string) $want !== (string) $have) {
                $diff[$col] = $want;
            }
        }
        return $diff;
    }
}
