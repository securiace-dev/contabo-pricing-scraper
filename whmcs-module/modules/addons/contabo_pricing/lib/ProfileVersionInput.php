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
     * @param array<string, mixed> $optionsSnapshot
     * @param array<string, mixed> $specsSnapshot
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
        string $snapshotGeneratedAt
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
        string $snapshotGeneratedAt
    ): self {
        $gstPct = $applyGst18 ? 0.18 : 0.0;
        $afterGst = $configuredMonthlyEur * (1.0 + $gstPct);
        $afterGstSetup = $setupFeeEur * (1.0 + $gstPct);

        if ($currencyIso === 'EUR' || $fxRate === null) {
            $finalMonthly = round($afterGst, 4);
            $finalSetup   = round($afterGstSetup, 4);
        } else {
            $effectiveFx = $fxRate * (1.0 + $fxMarkupPct / 100.0);
            $finalMonthly = round($afterGst * $effectiveFx, 2);
            $finalSetup   = round($afterGstSetup * $effectiveFx, 2);
        }

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
            $snapshotGeneratedAt
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
            || (($previous['currency_iso'] ?? '') !== $this->currencyIso);
    }
}
