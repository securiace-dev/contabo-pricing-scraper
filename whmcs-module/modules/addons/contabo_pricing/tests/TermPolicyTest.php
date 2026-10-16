<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\TermPolicy;
use ContaboPricing\TermTranslator;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class TermPolicyTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testDefaultsWhenTableEmpty(): void
    {
        $this->assertSame(TermTranslator::POLICY_NEAREST_TERM, TermPolicy::policy());
        $this->assertSame(TermTranslator::POLICY_PROPORTIONAL, TermPolicy::addonPolicy());
        $this->assertSame([], TermPolicy::adjustPct());
        $this->assertSame('{}', TermPolicy::adjustPctJson());
    }

    public function testReadsPersistedPolicyAndClampsAdjust(): void
    {
        Capsule::table('mod_contabo_settings')->insert([
            'key' => 'term.policy',
            'value' => 'proportional',
            'updated_at' => '2026-10-16 00:00:00',
        ]);
        Capsule::table('mod_contabo_settings')->insert([
            'key' => 'term.addon_policy',
            'value' => 'nearest-term',
            'updated_at' => '2026-10-16 00:00:00',
        ]);
        Capsule::table('mod_contabo_settings')->insert([
            'key' => 'term.adjust_pct_json',
            'value' => '{"24":2,"36":99,"nope":5}',
            'updated_at' => '2026-10-16 00:00:00',
        ]);

        $this->assertSame(TermTranslator::POLICY_PROPORTIONAL, TermPolicy::policy());
        $this->assertSame(TermTranslator::POLICY_NEAREST_TERM, TermPolicy::addonPolicy());
        $this->assertSame([24 => 2.0, 36 => 50.0], TermPolicy::adjustPct());
    }

    public function testBadJsonAndUnknownPolicyFallBackToDefaults(): void
    {
        Capsule::table('mod_contabo_settings')->insert([
            'key' => 'term.policy',
            'value' => 'bogus',
            'updated_at' => '2026-10-16 00:00:00',
        ]);
        Capsule::table('mod_contabo_settings')->insert([
            'key' => 'term.adjust_pct_json',
            'value' => 'not-json',
            'updated_at' => '2026-10-16 00:00:00',
        ]);
        $this->assertSame(TermTranslator::POLICY_NEAREST_TERM, TermPolicy::policy());
        $this->assertSame([], TermPolicy::adjustPct());
    }

    public function testSaveRejectsListShapedJsonThenPersistsObject(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        TermPolicy::save('nearest-term', 'proportional', '[1,2]');
    }

    public function testSavePersistsNormalizedValues(): void
    {
        TermPolicy::save(' Proportional ', 'nearest-term', '{"36":5,"24":-1}');
        $this->assertSame(TermTranslator::POLICY_PROPORTIONAL, TermPolicy::policy());
        $this->assertSame(TermTranslator::POLICY_NEAREST_TERM, TermPolicy::addonPolicy());
        $this->assertSame([36 => 5.0, 24 => 0.0], TermPolicy::adjustPct());
    }
}
