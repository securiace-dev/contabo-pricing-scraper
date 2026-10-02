<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PlanSource;

/** Inert PlanSource for tests that construct SyncEngine; override what you need. */
class StubPlanSource implements PlanSource
{
    public function meta(): array { return []; }
    public function plans(?string $family = null): array { return []; }
    public function plan(string $slug): array { throw new \RuntimeException('stub: no plan ' . $slug); }
    public function configurator(string $slug): array { return ['options' => []]; }
    public function fx(): array { return []; }
    public function quote(array $body): array { return []; }
}
