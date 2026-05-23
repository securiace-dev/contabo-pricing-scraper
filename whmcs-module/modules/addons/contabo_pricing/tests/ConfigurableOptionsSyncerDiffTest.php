<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\DriftHasher;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 0.5.1 item 3 — ConfigurableOptionsSyncer::diff() (read-only pre-apply diff).
 *
 * Proves the diff classifies each dimension exactly as apply would treat it
 * (create / update / noop / drift_skip) WITHOUT writing anything.
 */
final class ConfigurableOptionsSyncerDiffTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function syncer(ConfigOptionLinkRepository $links): ConfigurableOptionsSyncer
    {
        $audit = new class('diff-test') extends OptionAuditLog {
            protected function storeRow(array $row): int { return 0; }
        };
        // Dry-run adapter: diff() only reads (fetchOption).
        return new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(true), $audit, $links);
    }

    /** @return array<string,mixed> */
    private function spec(string $dim, int $type): array
    {
        return [
            'dimension_key' => $dim,
            'optiontype'    => $type,
            'values'        => [['value_key' => $dim . ':0', 'label' => $dim . ' 0', 'monthly_eur_delta' => 0.0, 'is_default' => true, 'sortorder' => 0]],
        ];
    }

    private function seedLiveOption(int $id, string $name, int $type): array
    {
        Capsule::table('tblproductconfigoptions')->insert([
            'id' => $id, 'gid' => 10, 'optionname' => $name, 'optiontype' => $type,
            'qtyminimum' => 0, 'qtymaximum' => 0, 'hidden' => 0,
        ]);
        return (array) Capsule::table('tblproductconfigoptions')->where('id', $id)->first();
    }

    public function testDiffClassifiesEachDimension(): void
    {
        $links = new ConfigOptionLinkRepository();
        $pid = 7;
        $cols = WhmcsConfigOptionsAdapter::OPTION_DRIFT_COLUMNS;

        // noop — link + matching live option + matching baseline hash.
        $region = $this->seedLiveOption(20, 'Region', OptionTypeMapper::TYPE_DROPDOWN);
        $links->upsertOptionLink($pid, 'Region', OptionTypeMapper::TYPE_DROPDOWN, 20, ['hidden' => false, 'expose_to_customer' => true], DriftHasher::hashFields($region, $cols));

        // drift_skip — baseline recorded, then the live option is hand-edited.
        $image = $this->seedLiveOption(21, 'Image', OptionTypeMapper::TYPE_DROPDOWN);
        $links->upsertOptionLink($pid, 'Image', OptionTypeMapper::TYPE_DROPDOWN, 21, ['hidden' => false, 'expose_to_customer' => true], DriftHasher::hashFields($image, $cols));
        Capsule::table('tblproductconfigoptions')->where('id', 21)->update(['optionname' => 'X HAND-EDITED']);

        // update — baseline matches live, but the spec's optiontype now differs.
        $storage = $this->seedLiveOption(22, 'Storage', OptionTypeMapper::TYPE_DROPDOWN);
        $links->upsertOptionLink($pid, 'Storage', OptionTypeMapper::TYPE_DROPDOWN, 22, ['hidden' => false], DriftHasher::hashFields($storage, $cols));

        // create-missing — link points to a WHMCS option that no longer exists.
        $links->upsertOptionLink($pid, 'Networking:IPv4', OptionTypeMapper::TYPE_QUANTITY, 999, ['hidden' => false]);

        $specs = [
            $this->spec('Region', OptionTypeMapper::TYPE_DROPDOWN),
            $this->spec('Image', OptionTypeMapper::TYPE_DROPDOWN),
            $this->spec('Storage', OptionTypeMapper::TYPE_QUANTITY), // differs from live DROPDOWN → update
            $this->spec('Backup', OptionTypeMapper::TYPE_DROPDOWN),  // no link → create
            $this->spec('Networking:IPv4', OptionTypeMapper::TYPE_QUANTITY), // link → missing option → create
        ];

        $diff = $this->syncer($links)->diff($pid, 500, 'contabo-test', $specs);

        $byDim = [];
        foreach ($diff['rows'] as $r) {
            $byDim[$r['dimension_key']] = $r['action'];
        }
        $this->assertSame('noop', $byDim['Region']);
        $this->assertSame('drift_skip', $byDim['Image']);
        $this->assertSame('update', $byDim['Storage']);
        $this->assertSame('create', $byDim['Backup']);
        $this->assertSame('create', $byDim['Networking:IPv4']);

        $this->assertSame(['create' => 2, 'update' => 1, 'noop' => 1, 'drift_skip' => 1], $diff['summary']);
    }

    public function testDiffWritesNothing(): void
    {
        $links = new ConfigOptionLinkRepository();
        $this->seedLiveOption(20, 'Region', OptionTypeMapper::TYPE_DROPDOWN);
        $links->upsertOptionLink(7, 'Region', OptionTypeMapper::TYPE_DROPDOWN, 20, ['hidden' => false]);

        $before = Capsule::table('tblproductconfigoptions')->count();
        $this->syncer($links)->diff(7, 500, 'contabo-test', [$this->spec('Region', OptionTypeMapper::TYPE_DROPDOWN), $this->spec('NewDim', OptionTypeMapper::TYPE_DROPDOWN)]);
        $after = Capsule::table('tblproductconfigoptions')->count();

        $this->assertSame($before, $after, 'diff() must not create/delete any option rows');
        // The live option is untouched.
        $live = (array) Capsule::table('tblproductconfigoptions')->where('id', 20)->first();
        $this->assertSame('Region', (string) $live['optionname']);
    }
}
