<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase A.6.2 — configurable-options syncer (OBSERVE / preview).
 *
 * Given a profile's normalised dimension specs (the {@see DimensionParser}
 * output carried on a profile version's `specsSnapshot`), this produces the
 * full set of WHMCS configurable options that WOULD be created for the
 * profile's product — group → options → sub-options → per-cycle pricing —
 * WITHOUT writing to WHMCS. It is the read-only half of A.6; A.6.3 adds the
 * apply path. All writes are funnelled through {@see WhmcsConfigOptionsAdapter}
 * (constructed in dry-run for observe), the sole tblproductconfig + tblpricing
 * write chokepoint, and every step is recorded in {@see OptionAuditLog}.
 *
 * Pricing — each value's `monthly_eur_delta` is converted to the local landed
 * cost via {@see ConfigOptionPricingContext::landedMultiplier} and then to a
 * per-cycle sell price via {@see MarginCalculator::sellPriceForCycle}, so an
 * option delta is priced exactly like the base plan it sits on.
 *
 * Binding A.5.2 amendments enforced here:
 *   - #1 negative-delta clamp: a cheaper-than-default value (negative EUR
 *     delta) is clamped to 0 for v1, so we never emit negative tblpricing rows
 *     (WHMCS handles them, but the order form hides $0/negative labels and
 *     downgrades become credits — see PHASE_A6_PREFLIGHT_CONFIGOPTIONS.md).
 *   - #10 base-currency only: pricing for a non-base currency is skipped by the
 *     adapter (action 'skipped', never a stale row); the syncer surfaces it.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ConfigurableOptionsSyncer
{
    /** WHMCS billing cycle → months, in tblpricing column order. */
    private const CYCLE_MONTHS = [
        'monthly'      => 1,
        'quarterly'    => 3,
        'semiannually' => 6,
        'annually'     => 12,
        'biennially'   => 24,
        'triennially'  => 36,
    ];

    /** @var WhmcsConfigOptionsAdapter */
    private $adapter;

    /** @var OptionAuditLog */
    private $audit;

    /** @var ConfigOptionLinkRepository|null Required for apply(); unused by observe(). */
    private $links;

    public function __construct(WhmcsConfigOptionsAdapter $adapter, OptionAuditLog $audit, ?ConfigOptionLinkRepository $links = null)
    {
        $this->adapter = $adapter;
        $this->audit   = $audit;
        $this->links   = $links;
    }

    /**
     * Build the preview for one profile.
     *
     * @param list<array<string,mixed>> $specs DimensionParser specs (each with
     *        dimension_key, optiontype, values[], optional default_value_key).
     * @param int|null $productId when set, the group is also previewed as
     *        linked to this WHMCS product.
     * @return array{
     *     profile_id:int, dry_run:bool, currency_id:int, group:array<string,mixed>,
     *     options:list<array<string,mixed>>, skipped:list<array<string,mixed>>,
     *     audit_count:int, totals:array{options:int,values:int,skipped:int}
     * }
     */
    public function observe(
        int $profileId,
        string $groupName,
        array $specs,
        ConfigOptionPricingContext $ctx,
        ?int $productId = null
    ): array {
        $auditCount = 0;
        $skipped    = [];

        $group = $this->adapter->upsertGroup(
            $groupName,
            'Contabo configurable options (profile #' . $profileId . ')'
        );
        $auditCount += $this->audit->observe(
            $profileId,
            null,
            'tblproductconfiggroups',
            $group['payload'],
            'group ' . $group['action']
        ) > 0 ? 1 : 0;

        $groupId = (int) ($group['id'] ?? 0); // null in dry-run; real id in apply mode
        if ($productId !== null) {
            $this->adapter->linkGroupToProduct($groupId, $productId);
        }

        $options = [];
        $valueCount = 0;

        foreach ($specs as $spec) {
            $dimKey     = (string) ($spec['dimension_key'] ?? '');
            $optionType = (int) ($spec['optiontype'] ?? OptionTypeMapper::TYPE_DROPDOWN);
            $values     = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];

            if ($dimKey === '' || $values === []) {
                $skipped[] = ['dimension_key' => $dimKey, 'reason' => 'empty_spec'];
                continue;
            }

            $isQuantity = $optionType === OptionTypeMapper::TYPE_QUANTITY;
            $option = $this->adapter->upsertOption(
                $groupId,
                $dimKey,
                $optionType,
                $isQuantity ? 0 : null,                 // qtyMin
                $isQuantity ? count($values) : null,    // qtyMax (provisional; refined in apply)
                0
            );
            $auditCount += $this->audit->observe(
                $profileId,
                $dimKey,
                'tblproductconfigoptions',
                $option['payload'],
                'option ' . $option['action']
            ) > 0 ? 1 : 0;

            $optionId   = (int) ($option['id'] ?? 0);
            $valuesPrev = [];

            foreach ($values as $i => $value) {
                $label      = (string) ($value['label'] ?? ($value['value_key'] ?? ('value ' . $i)));
                $sortOrder  = (int) ($value['sortorder'] ?? $i);
                $sub = $this->adapter->upsertSubOption($optionId, $label, $sortOrder, false);

                $cyclePrices = $this->priceCycles((float) ($value['monthly_eur_delta'] ?? 0.0), $ctx);
                $setupFees   = $this->priceSetup((float) ($value['setup_eur_delta'] ?? 0.0), $ctx);

                $subId  = (int) ($sub['id'] ?? 0);
                $pricing = $this->adapter->upsertConfigOptionPricing(
                    $subId,
                    $ctx->currencyId,
                    $cyclePrices,
                    $setupFees
                );

                if (($pricing['action'] ?? '') === 'skipped') {
                    $skipped[] = [
                        'dimension_key' => $dimKey,
                        'value'         => $label,
                        'reason'        => (string) ($pricing['skip_reason'] ?? 'pricing_skipped'),
                    ];
                }

                $auditCount += $this->audit->observe(
                    $profileId,
                    $dimKey,
                    'tblproductconfigoptionssub',
                    [
                        'label'        => $label,
                        'value_key'    => (string) ($value['value_key'] ?? ''),
                        'is_default'   => (bool) ($value['is_default'] ?? false),
                        'monthly_eur_delta' => (float) ($value['monthly_eur_delta'] ?? 0.0),
                        'cycle_prices' => $cyclePrices,
                        'setup_fees'   => $setupFees,
                    ],
                    'suboption ' . $sub['action']
                ) > 0 ? 1 : 0;

                $valuesPrev[] = [
                    'label'        => $label,
                    'value_key'    => (string) ($value['value_key'] ?? ''),
                    'is_default'   => (bool) ($value['is_default'] ?? false),
                    'cycle_prices' => $cyclePrices,
                    'setup_fees'   => $setupFees,
                    'sub'          => $sub,
                    'pricing'      => $pricing,
                ];
                $valueCount++;
            }

            $options[] = [
                'dimension_key' => $dimKey,
                'optiontype'    => $optionType,
                'is_quantity'   => $isQuantity,
                'option'        => $option,
                'values'        => $valuesPrev,
            ];
        }

        return [
            'profile_id'  => $profileId,
            'dry_run'     => $this->adapter->isDryRun(),
            'currency_id' => $ctx->currencyId,
            'group'       => $group,
            'options'     => $options,
            'skipped'     => $skipped,
            'audit_count' => $auditCount,
            'totals'      => [
                'options' => count($options),
                'values'  => $valueCount,
                'skipped' => count($skipped),
            ],
        ];
    }

    /**
     * APPLY — write the configurable options to a real WHMCS product.
     *
     * Same traversal as {@see observe()} but with a NON-dry-run adapter: each
     * group/option/sub/pricing is upserted for real, the resulting WHMCS id is
     * recorded in the link tables via {@see ConfigOptionLinkRepository} (which
     * makes re-apply idempotent + ownership-scoped), and every action is written
     * to {@see OptionAuditLog} with the adapter's real action (created→insert,
     * updated→update, noop→skip_no_change).
     *
     * Requires: a link repo (constructor arg), a real (non-dry-run) adapter, and
     * a target $productId. Base-currency-only and the negative-delta clamp are
     * enforced exactly as in observe(). Drift guard (manual-edit protection) runs
     * at BOTH the option level (option link expected_hash over OPTION_DRIFT_COLUMNS)
     * and the value level (value link expected_hash over the sub-option + recurring
     * pricing columns) — a hand-edited live object is flagged
     * (summary.drift_skipped) and skipped, never clobbered.
     *
     * @param list<array<string,mixed>> $specs
     * @return array{
     *   profile_id:int, product_id:int, group:array<string,mixed>,
     *   summary:array{created:int,updated:int,noop:int,skipped:int},
     *   options:int, values:int
     * }
     */
    public function apply(
        int $profileId,
        int $productId,
        string $groupKey,
        string $groupName,
        array $specs,
        ConfigOptionPricingContext $ctx
    ): array {
        if ($this->links === null) {
            throw new \RuntimeException('ConfigurableOptionsSyncer::apply() requires a ConfigOptionLinkRepository.');
        }
        if ($this->adapter->isDryRun()) {
            throw new \RuntimeException('apply() needs a non-dry-run adapter; got a dry-run one.');
        }

        // Phase C: expose_configurable_options gate — master switch at the
        // profile level. When disabled the admin wants catalog price sync only,
        // not customer-facing WHMCS config option groups. Fail-open: if the
        // gate check itself errors (pre-migration, schema race) we apply as
        // before rather than silently skipping a requested apply.
        try {
            if (Capsule::schema()->hasColumn('mod_contabo_profile', 'expose_configurable_options')) {
                $exposeEnabled = Capsule::table('mod_contabo_profile')
                    ->where('id', $profileId)
                    ->value('expose_configurable_options');
                // null ⇒ no such profile row (shouldn't happen); treat as enabled.
                if ($exposeEnabled !== null && !(bool) $exposeEnabled) {
                    return [
                        'profile_id'  => $profileId,
                        'product_id'  => $productId,
                        'group'       => [],
                        'summary'     => ['created' => 0, 'updated' => 0, 'noop' => 0, 'skipped' => 1, 'drift_skipped' => 0],
                        'options'     => 0,
                        'values'      => 0,
                        'skipped'     => true,
                        'skip_reason' => 'expose_gate_disabled',
                    ];
                }
            }
        } catch (\Throwable $e) {
            // fall through and apply normally (fail-open)
        }

        $summary = ['created' => 0, 'updated' => 0, 'noop' => 0, 'skipped' => 0, 'drift_skipped' => 0];

        $group   = $this->adapter->upsertGroup($groupName, 'Contabo configurable options (profile #' . $profileId . ')');
        $groupId = (int) ($group['id'] ?? 0);
        $this->links->upsertGroupLink($profileId, $productId, $groupKey, $groupId > 0 ? $groupId : null);
        if ($groupId > 0) {
            $this->adapter->linkGroupToProduct($groupId, $productId);
        }
        $this->tally($summary, (string) ($group['action'] ?? ''));
        $this->audit->record($profileId, null, 'tblproductconfiggroups', $groupId > 0 ? $groupId : null, $this->auditAction($group['action'] ?? ''), null, $group['payload'] ?? null, 'apply group');

        $optionCount = 0;
        $valueCount  = 0;

        foreach ($specs as $spec) {
            $dimKey     = (string) ($spec['dimension_key'] ?? '');
            $optionType = (int) ($spec['optiontype'] ?? OptionTypeMapper::TYPE_DROPDOWN);
            $values     = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];
            if ($dimKey === '' || $values === []) {
                continue;
            }
            $isQuantity = $optionType === OptionTypeMapper::TYPE_QUANTITY;

            // Exposure curation (amendment 8). Resolution order: an existing
            // option-link's stored flags win (the admin may have curated them via
            // the exposure editor); otherwise the RetailVpsMinimal default. This
            // drives WHMCS option visibility AND is recorded back on the link, so
            // a fresh apply produces a curated catalog (OS only, etc.) rather than
            // flooding the order form with all 34 images + every dimension.
            $existingLink = $this->links->findOptionLink($profileId, $dimKey);

            // Drift guard (§14 / amendment 14): if the addon previously wrote this
            // option (whmcs id + a recorded baseline hash) and the LIVE WHMCS
            // object no longer matches that baseline, an admin hand-edited it out
            // of band — flag it and SKIP (including its sub-values); never clobber.
            if ($existingLink !== null
                && (int) ($existingLink['whmcs_option_id'] ?? 0) > 0
                && !empty($existingLink['expected_hash'])
            ) {
                $liveOpt = $this->adapter->fetchOption((int) $existingLink['whmcs_option_id']);
                if ($liveOpt !== null
                    && !DriftHasher::matches((string) $existingLink['expected_hash'], $liveOpt, WhmcsConfigOptionsAdapter::OPTION_DRIFT_COLUMNS)
                ) {
                    $summary['drift_skipped']++;
                    $this->audit->record(
                        $profileId, $dimKey, 'tblproductconfigoptions',
                        (int) $existingLink['whmcs_option_id'], 'drift_skip',
                        ['expected_hash' => (string) $existingLink['expected_hash']], $liveOpt,
                        'live option hand-edited since last apply — skipped to avoid clobbering the admin change'
                    );
                    continue;
                }
            }

            // Exposure curation (amendment 8). Resolution order: an existing
            // option-link's stored flags win (the admin may have curated them via
            // the exposure editor); otherwise the RetailVpsMinimal default. This
            // drives WHMCS option visibility AND is recorded back on the link, so
            // a fresh apply produces a curated catalog (OS only, etc.) rather than
            // flooding the order form with all 34 images + every dimension.
            if ($existingLink !== null && array_key_exists('hidden', $existingLink)) {
                $optHidden = (bool) $existingLink['hidden'];
                $optExpose = array_key_exists('expose_to_customer', $existingLink)
                    ? (bool) $existingLink['expose_to_customer']
                    : !$optHidden;
            } else {
                $d = ExposureResolver::decideForDimension($dimKey);
                $optHidden = (bool) $d['hidden'];
                $optExpose = (bool) $d['expose_to_customer'];
            }

            $option   = $this->adapter->upsertOption($groupId, $dimKey, $optionType, $isQuantity ? 0 : null, $isQuantity ? count($values) : null, 0, $optHidden);
            $optionId = (int) ($option['id'] ?? 0);
            // Record the NEW drift baseline = hash of the option as just written
            // (read it back so the baseline matches what a future re-apply sees).
            $newHash = null;
            if ($optionId > 0) {
                $liveAfter = $this->adapter->fetchOption($optionId);
                if ($liveAfter !== null) {
                    $newHash = DriftHasher::hashFields($liveAfter, WhmcsConfigOptionsAdapter::OPTION_DRIFT_COLUMNS);
                }
            }
            $link     = $this->links->upsertOptionLink(
                $profileId, $dimKey, $optionType, $optionId > 0 ? $optionId : null,
                ['expose_to_customer' => $optExpose, 'hidden' => $optHidden],
                $newHash
            );
            $optionLinkId = (int) ($link['id'] ?? 0);
            $this->tally($summary, (string) ($option['action'] ?? ''));
            $this->audit->record($profileId, $dimKey, 'tblproductconfigoptions', $optionId > 0 ? $optionId : null, $this->auditAction($option['action'] ?? ''), null, $option['payload'] ?? null, 'apply option');
            $optionCount++;

            foreach ($values as $i => $value) {
                $label     = (string) ($value['label'] ?? ($value['value_key'] ?? ('value ' . $i)));
                $valueKey  = (string) ($value['value_key'] ?? $label);
                $sortOrder = (int) ($value['sortorder'] ?? $i);
                $isDefault = (bool) ($value['is_default'] ?? false);
                $eurDelta  = (float) ($value['monthly_eur_delta'] ?? 0.0);

                // Image is ONE dropdown whose sub-values span categories; the
                // Retail preset hides Panels/Apps/Blockchain sub-values but shows
                // OS. Other dimensions' visibility is controlled at the option
                // level above, so their sub-values stay visible.
                $subHidden = false;
                if ($dimKey === 'Image') {
                    $subHidden = (bool) ExposureResolver::decideForImageCategory((string) ($value['category'] ?? ''))['hidden'];
                }

                // Value-level drift guard (extends the option-level guard to the
                // sub-option + pricing). If the addon previously wrote this value
                // (sub id + recorded baseline) and the LIVE sub-option/pricing no
                // longer matches, an admin hand-edited it — flag + SKIP this value,
                // never clobber.
                $existingValueLink = $this->links->findValueLink($optionLinkId, $valueKey);
                if ($existingValueLink !== null
                    && (int) ($existingValueLink['whmcs_sub_id'] ?? 0) > 0
                    && !empty($existingValueLink['expected_hash'])
                ) {
                    $liveSub     = $this->adapter->fetchSub((int) $existingValueLink['whmcs_sub_id']);
                    $livePricing = $this->adapter->fetchPricing((int) $existingValueLink['whmcs_sub_id'], $ctx->currencyId);
                    if ($liveSub !== null) {
                        $liveFields = $this->valueDriftFields($liveSub, $livePricing);
                        if (!DriftHasher::matches((string) $existingValueLink['expected_hash'], $liveFields, array_keys($liveFields))) {
                            $summary['drift_skipped']++;
                            $this->audit->record(
                                $profileId, $dimKey, 'tblproductconfigoptionssub',
                                (int) $existingValueLink['whmcs_sub_id'], 'drift_skip',
                                ['expected_hash' => (string) $existingValueLink['expected_hash']], $liveFields,
                                'live sub-option/pricing hand-edited since last apply — value skipped to avoid clobbering'
                            );
                            continue;
                        }
                    }
                }

                $sub   = $this->adapter->upsertSubOption($optionId, $label, $sortOrder, $subHidden);
                $subId = (int) ($sub['id'] ?? 0);

                $pricing = $this->adapter->upsertConfigOptionPricing(
                    $subId,
                    $ctx->currencyId,
                    $this->priceCycles($eurDelta, $ctx),
                    $this->priceSetup((float) ($value['setup_eur_delta'] ?? 0.0), $ctx)
                );

                // Record the NEW value drift baseline = hash of the sub + pricing as
                // just written (read back so it matches what a future re-apply sees).
                $valueHash = null;
                if ($subId > 0) {
                    $liveSubAfter     = $this->adapter->fetchSub($subId);
                    $livePricingAfter = $this->adapter->fetchPricing($subId, $ctx->currencyId);
                    if ($liveSubAfter !== null) {
                        $fieldsAfter = $this->valueDriftFields($liveSubAfter, $livePricingAfter);
                        $valueHash = DriftHasher::hashFields($fieldsAfter, array_keys($fieldsAfter));
                    }
                }
                $this->links->upsertValueLink($optionLinkId, $valueKey, $label, $subId > 0 ? $subId : null, $isDefault, $eurDelta, $valueHash);

                $this->tally($summary, (string) ($pricing['action'] ?? ''));
                $this->tally($summary, (string) ($sub['action'] ?? ''));
                $this->audit->record($profileId, $dimKey, 'tblproductconfigoptionssub', $subId > 0 ? $subId : null, $this->auditAction($sub['action'] ?? ''), null, ['label' => $label, 'value_key' => $valueKey], 'apply value');
                $valueCount++;
            }
        }

        return [
            'profile_id' => $profileId,
            'product_id' => $productId,
            'group'      => $group,
            'summary'    => $summary,
            'options'    => $optionCount,
            'values'     => $valueCount,
        ];
    }

    /**
     * READ-ONLY pre-apply diff: what {@see apply()} WOULD do to THIS live product,
     * per dimension, WITHOUT writing anything. Reuses apply's drift guard +
     * exposure resolution so the preview matches the real run. Per-dimension action:
     *   create     — no addon-owned option recorded yet (or the recorded one is gone)
     *   drift_skip — live option hand-edited since last apply → apply will SKIP it
     *   update     — addon-owned option exists but a field (name/type/visibility) differs
     *   noop       — addon-owned option exists and already matches what apply would write
     *
     * Option-level (matching apply's drift granularity); each row carries the
     * value count. Sub-option/pricing-level diff is a follow-up (item 4 territory).
     *
     * @param list<array<string,mixed>> $specs
     * @return array{
     *   profile_id:int, product_id:int, group_exists:bool,
     *   rows:list<array<string,mixed>>,
     *   summary:array{create:int,update:int,noop:int,drift_skip:int}
     * }
     */
    public function diff(int $profileId, int $productId, string $groupKey, array $specs): array
    {
        if ($this->links === null) {
            throw new \RuntimeException('ConfigurableOptionsSyncer::diff() requires a ConfigOptionLinkRepository.');
        }

        $summary = ['create' => 0, 'update' => 0, 'noop' => 0, 'drift_skip' => 0];
        $rows = [];
        $groupExists = $this->links->findGroupLink($profileId, $productId, $groupKey) !== null;

        foreach ($specs as $spec) {
            $dimKey     = (string) ($spec['dimension_key'] ?? '');
            $optionType = (int) ($spec['optiontype'] ?? OptionTypeMapper::TYPE_DROPDOWN);
            $values     = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];
            if ($dimKey === '' || $values === []) {
                continue;
            }

            // Visibility apply() WOULD write: existing link flags win, else preset.
            $existingLink = $this->links->findOptionLink($profileId, $dimKey);
            if ($existingLink !== null && array_key_exists('hidden', $existingLink)) {
                $optHidden = (bool) $existingLink['hidden'];
            } else {
                $optHidden = (bool) ExposureResolver::decideForDimension($dimKey)['hidden'];
            }

            $liveOptionId = $existingLink !== null ? (int) ($existingLink['whmcs_option_id'] ?? 0) : 0;
            $action = 'create';
            $detail = 'new option — apply will create it';

            if ($liveOptionId > 0) {
                $liveOpt = $this->adapter->fetchOption($liveOptionId);
                if ($liveOpt === null) {
                    $action = 'create';
                    $detail = 'recorded WHMCS option #' . $liveOptionId . ' is gone — apply will re-create it';
                } elseif (!empty($existingLink['expected_hash'])
                    && !DriftHasher::matches((string) $existingLink['expected_hash'], $liveOpt, WhmcsConfigOptionsAdapter::OPTION_DRIFT_COLUMNS)
                ) {
                    $action = 'drift_skip';
                    $detail = 'live option hand-edited since last apply — apply will SKIP it (your edit is preserved)';
                } else {
                    $changed = ((string) ($liveOpt['optionname'] ?? '') !== $dimKey)
                        || ((int) ($liveOpt['optiontype'] ?? -1) !== $optionType)
                        || ((int) ($liveOpt['hidden'] ?? -1) !== ($optHidden ? 1 : 0));
                    $action = $changed ? 'update' : 'noop';
                    $detail = $changed ? 'option exists; apply will update it' : 'option exists and already matches — no change';
                }
            }

            $summary[$action]++;
            $rows[] = [
                'dimension_key'   => $dimKey,
                'optiontype'      => $optionType,
                'values'          => count($values),
                'action'          => $action,
                'detail'          => $detail,
                'whmcs_option_id' => $liveOptionId,
                'will_be_hidden'  => $optHidden,
            ];
        }

        return [
            'profile_id'   => $profileId,
            'product_id'   => $productId,
            'group_exists' => $groupExists,
            'rows'         => $rows,
            'summary'      => $summary,
        ];
    }

    /**
     * Combined drift fields for one value = the sub-option columns + the recurring
     * pricing columns the addon controls, flattened with stable prefixed keys and
     * hashed into the value link's expected_hash. So a hand-edited sub-option
     * label/sort/visibility OR a hand-edited cycle price is detected on re-apply.
     *
     * @param array<string,mixed>|null $sub
     * @param array<string,mixed>|null $pricing
     * @return array<string,string>
     */
    private function valueDriftFields(?array $sub, ?array $pricing): array
    {
        $sub = $sub ?? [];
        $pricing = $pricing ?? [];
        $fields = [];
        foreach (WhmcsConfigOptionsAdapter::SUB_DRIFT_COLUMNS as $c) {
            $fields['sub_' . $c] = (string) ($sub[$c] ?? '');
        }
        foreach (WhmcsConfigOptionsAdapter::PRICING_DRIFT_COLUMNS as $c) {
            $fields['price_' . $c] = (string) ($pricing[$c] ?? '');
        }
        return $fields;
    }

    /** @param array{created:int,updated:int,noop:int,skipped:int} $summary */
    private function tally(array &$summary, string $action): void
    {
        if ($action === 'created') {
            $summary['created']++;
        } elseif ($action === 'updated') {
            $summary['updated']++;
        } elseif ($action === 'skipped') {
            $summary['skipped']++;
        } elseif ($action === 'noop') {
            $summary['noop']++;
        }
    }

    /** Map an adapter action to an OptionAuditLog action constant. */
    private function auditAction(string $adapterAction): string
    {
        if ($adapterAction === 'created') {
            return OptionAuditLog::ACTION_INSERT;
        }
        if ($adapterAction === 'updated') {
            return OptionAuditLog::ACTION_UPDATE;
        }
        if ($adapterAction === 'skipped') {
            return OptionAuditLog::ACTION_SKIP_DISABLED;
        }
        return OptionAuditLog::ACTION_SKIP_NO_CHANGE; // noop / dryrun
    }

    /**
     * Per-cycle sell price for one option value's monthly EUR delta.
     *
     * The EUR delta is clamped to >= 0 (amendment #1), converted to local
     * landed monthly via the context multiplier, then run through the same
     * MarginCalculator path as the base plan. A zero (or clamped) delta prices
     * to 0.00 in every cycle — exactly what a default / no-cost value should be.
     *
     * @return array<string,float> keyed by cycle name
     */
    private function priceCycles(float $monthlyEurDelta, ConfigOptionPricingContext $ctx): array
    {
        $eur = max(0.0, $monthlyEurDelta);
        $landedMonthly = $eur * $ctx->landedMultiplier;

        $prices = [];
        foreach (self::CYCLE_MONTHS as $cycle => $months) {
            // A zero (or negative-clamped) marginal cost is FREE — never apply
            // the markup to it. This matters for cost_plus_amount, where a flat
            // amount would otherwise be charged on a default/no-cost value; the
            // flat amount belongs to the base product, not to each option value.
            if ($landedMonthly <= 0.0) {
                $prices[$cycle] = 0.0;
                continue;
            }
            $sell = MarginCalculator::sellPriceForCycle(
                $landedMonthly,
                $ctx->markupStrategy,
                $ctx->markupValue,
                null,
                $months
            );
            $prices[$cycle] = Rounding::apply($sell, $ctx->roundingMode);
        }

        return $prices;
    }

    /**
     * One-time setup fee per cycle. Setup deltas are rare on Contabo options;
     * when present we pass the landed cost through (clamped >= 0, rounded) with
     * no recurring markup, and replicate it across the cycle columns WHMCS
     * stores. Returns an empty array when there is no setup delta, so the
     * adapter writes no setup columns.
     *
     * @return array<string,float>
     */
    private function priceSetup(float $setupEurDelta, ConfigOptionPricingContext $ctx): array
    {
        $eur = max(0.0, $setupEurDelta);
        if ($eur <= 0.0) {
            return [];
        }

        $setupLocal = Rounding::apply($eur * $ctx->landedMultiplier, $ctx->roundingMode);

        $fees = [];
        foreach (array_keys(self::CYCLE_MONTHS) as $cycle) {
            $fees[$cycle] = $setupLocal;
        }

        return $fees;
    }
}
