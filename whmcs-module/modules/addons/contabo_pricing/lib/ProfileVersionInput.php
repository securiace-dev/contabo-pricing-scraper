<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Value object passed to ProfileManager::appendVersion to record an immutable
 * pricing snapshot for a profile.
 *
 * PHP 7.4-compatible: typed properties + traditional constructor (no readonly,
 * no property promotion, no named args).
 */
final class ProfileVersionInput
{
    /** @var float    */ public $baseMonthlyEur;
    /** @var float    */ public $configuredMonthlyEur;
    /** @var float    */ public $setupFeeEur;
    /** @var array    */ public $optionsSnapshot;
    /** @var array    */ public $specsSnapshot;
    /** @var float|null */ public $fxRate;
    /** @var string|null */ public $fxSource;
    /** @var float    */ public $fxMarkupPct;
    /** @var float    */ public $gstPct;
    /** @var string   */ public $currencyIso;
    /** @var float    */ public $finalMonthly;
    /** @var float    */ public $finalSetup;
    /** @var string   */ public $snapshotGeneratedAt;

    /**
     * Per-period EUR SOURCE vector keyed by month count, e.g.
     * {1: 4.50, 3: 4.50, 6: 4.05, 12: 3.60, 24: 3.60, 36: 3.60}. Scraped periods
     * (1/3/6/12) carry their real effective_monthly; absent cycles (24/36) are
     * pre-expanded to the longest available period's rate at build time. Empty
     * for legacy versions — SyncEngine::computeCyclePrice falls back to the
     * single finalMonthly basis in that case.
     *
     * @var array<int,float>
     */
    public $periodPricesEur;

    /**
     * @param array<string, mixed> $optionsSnapshot
     * @param array<string, mixed> $specsSnapshot
     * @param array<int,float>     $periodPricesEur
     */
    public function __construct(
        float  $baseMonthlyEur,
        float  $configuredMonthlyEur,
        float  $setupFeeEur,
        array  $optionsSnapshot,
        array  $specsSnapshot,
        ?float $fxRate,
        ?string $fxSource,
        float  $fxMarkupPct,
        float  $gstPct,
        string $currencyIso,
        float  $finalMonthly,
        float  $finalSetup,
        string $snapshotGeneratedAt,
        array  $periodPricesEur = []
    ) {
        $this->baseMonthlyEur       = $baseMonthlyEur;
        $this->configuredMonthlyEur = $configuredMonthlyEur;
        $this->setupFeeEur          = $setupFeeEur;
        $this->optionsSnapshot      = $optionsSnapshot;
        $this->specsSnapshot        = $specsSnapshot;
        $this->fxRate               = $fxRate;
        $this->fxSource             = $fxSource;
        $this->fxMarkupPct          = $fxMarkupPct;
        $this->gstPct               = $gstPct;
        $this->currencyIso          = $currencyIso;
        $this->finalMonthly         = $finalMonthly;
        $this->finalSetup           = $finalSetup;
        $this->snapshotGeneratedAt  = $snapshotGeneratedAt;
        $this->periodPricesEur      = self::normalizePeriodPrices($periodPricesEur);
    }

    /**
     * Convert a monthly EUR amount to the local-currency monthly using the exact
     * GST-then-FX pipeline the version snapshot was computed with. The single
     * source of truth for EUR→local so computed() (headline period) and
     * SyncEngine::computeCyclePrice (per-cycle source vector) never diverge.
     *
     * @param float      $eurMonthly  Monthly amount in EUR.
     * @param float|null $fxRate      EUR→local rate (null / 'EUR' currency = passthrough).
     * @param float      $fxMarkupPct FX markup as PERCENT (e.g. 2.0).
     * @param float      $gstRatio    GST as a RATIO (e.g. 0.18), NOT a percent.
     * @param string     $currencyIso Target currency ISO.
     */
    public static function toLocalMonthly(
        float $eurMonthly,
        ?float $fxRate,
        float $fxMarkupPct,
        float $gstRatio,
        string $currencyIso
    ): float {
        // GST-PLACEMENT (Phase D §2.4): GST is currently folded into the COST
        // basis here, exactly as the legacy computed() did — no behaviour change.
        // Under a strict source/customer split GST is arguably a customer-price
        // concern, not a cost concern. If/when we move it, drop the line below and
        // apply GST in the customer (mapping markup) step instead:
        //
        //   $afterGst = $eurMonthly;                 // cost basis WITHOUT gst
        //   ... and gross-up at sell time:           $sell = $sellExGst * (1 + $gstRatio);
        //
        // Kept on the cost basis for now per owner decision.
        $afterGst = $eurMonthly * (1.0 + $gstRatio);
        if ($currencyIso === 'EUR' || $fxRate === null) {
            return round($afterGst, 4);
        }
        $effectiveFx = $fxRate * (1.0 + $fxMarkupPct / 100.0);
        return round($afterGst * $effectiveFx, 2);
    }

    /**
     * Coerce a period-price map to {int months => float eurMonthly}, dropping
     * non-positive month keys. Accepts string keys (JSON-decoded) transparently.
     *
     * @param array<int|string,mixed> $raw
     * @return array<int,float>
     */
    private static function normalizePeriodPrices(array $raw): array
    {
        $out = [];
        foreach ($raw as $months => $eur) {
            $m = (int) $months;
            if ($m > 0) {
                $out[$m] = (float) $eur;
            }
        }
        return $out;
    }

    /**
     * Compute final-monthly/final-setup deterministically from EUR + FX + GST.
     * Mirrors the API /quote endpoint exactly to keep the addon's "preview"
     * column consistent with what an admin sees in the report UI.
     *
     * @param array<string, mixed> $optionsSnapshot
     * @param array<string, mixed> $specsSnapshot
     */
    public static function computed(
        float  $baseMonthlyEur,
        float  $configuredMonthlyEur,
        float  $setupFeeEur,
        array  $optionsSnapshot,
        array  $specsSnapshot,
        ?float $fxRate,
        ?string $fxSource,
        float  $fxMarkupPct,
        bool   $applyGst18,
        string $currencyIso,
        string $snapshotGeneratedAt,
        array  $periodPricesEur = []
    ): self {
        $gstPct = $applyGst18 ? 0.18 : 0.0;
        $finalMonthly = self::toLocalMonthly($configuredMonthlyEur, $fxRate, $fxMarkupPct, $gstPct, $currencyIso);
        $finalSetup   = self::toLocalMonthly($setupFeeEur, $fxRate, $fxMarkupPct, $gstPct, $currencyIso);

        return new self(
            $baseMonthlyEur,
            $configuredMonthlyEur,
            $setupFeeEur,
            $optionsSnapshot,
            $specsSnapshot,
            $fxRate,
            $fxSource,
            $fxMarkupPct,
            $gstPct * 100.0, // store as percentage in DB for readability
            $currencyIso,
            $finalMonthly,
            $finalSetup,
            $snapshotGeneratedAt,
            $periodPricesEur
        );
    }

    /**
     * Have any pricing-relevant fields changed compared to a previously-stored
     * version row? Used by SyncEngine to decide whether to append a new version.
     *
     * @param array<string, mixed>|null $previous
     */
    public function differsFrom(?array $previous): bool
    {
        if ($previous === null) {
            return true;
        }
        // Compare to 2 decimal places — sub-cent FX wiggle shouldn't create new versions
        // 7.4-compatible arrow function (introduced in 7.4)
        $cmp = static fn ($a, $b): bool => abs(((float) $a) - ((float) $b)) > 0.005;

        return $cmp($previous['base_monthly_eur'] ?? 0, $this->baseMonthlyEur)
            || $cmp($previous['configured_monthly_eur'] ?? 0, $this->configuredMonthlyEur)
            || $cmp($previous['setup_fee_eur'] ?? 0, $this->setupFeeEur)
            || $cmp($previous['final_monthly'] ?? 0, $this->finalMonthly)
            || $cmp($previous['final_setup'] ?? 0, $this->finalSetup)
            || (($previous['currency_iso'] ?? '') !== $this->currencyIso)
            || $this->periodPricesDiffer($previous['period_prices_json'] ?? null);
    }

    /**
     * Whether the per-period EUR SOURCE vector differs from a stored
     * period_prices_json blob (> half a cent on any cycle, or a changed key set).
     * A version with an empty vector never reports a diff on this axis (keeps
     * legacy rows from churning).
     *
     * @param mixed $previousJson stored period_prices_json (string|null)
     */
    private function periodPricesDiffer($previousJson): bool
    {
        if ($this->periodPricesEur === []) {
            return false;
        }
        $prev = [];
        if (is_string($previousJson) && $previousJson !== '') {
            $decoded = json_decode($previousJson, true);
            if (is_array($decoded)) {
                foreach ($decoded as $m => $v) {
                    $prev[(int) $m] = (float) $v;
                }
            }
        }
        if (array_keys($prev) != array_keys($this->periodPricesEur)) {
            return true;
        }
        foreach ($this->periodPricesEur as $months => $eur) {
            if (abs(((float) ($prev[$months] ?? 0)) - $eur) > 0.005) {
                return true;
            }
        }
        return false;
    }
}
