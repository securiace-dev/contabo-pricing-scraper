<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Read contract for plan/catalog data consumed by SyncEngine and the admin UI.
 * Implemented locally (LocalPlanSource) from the addon's own catalog tables.
 *
 * PHP 7.4 polyglot: docblock types only for arrays.
 */
interface PlanSource
{
    /** @return array<string,mixed> {scraper_version, schema_version, snapshot_meta{generated_at, plan_count, scraper_version}} */
    public function meta(): array;

    /** @return list<array<string,mixed>> */
    public function plans(?string $family = null): array;

    /**
     * @return array<string,mixed>
     * @throws \RuntimeException when the plan is unknown
     */
    public function plan(string $slug): array;

    /** @return array<string,mixed> */
    public function configurator(string $slug): array;

    /**
     * @return array<string,mixed> raw FX body plus eurInr, rate, mid, source, fetched_at, age_minutes
     * @throws \RuntimeException when rates are unavailable
     */
    public function fx(): array;

    /**
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     * @throws \InvalidArgumentException on unknown cycle/period
     */
    public function quote(array $body): array;
}
