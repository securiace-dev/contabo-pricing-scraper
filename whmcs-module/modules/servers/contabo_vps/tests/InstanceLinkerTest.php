<?php
declare(strict_types=1);

use ContaboVps\ContaboApiClient;
use ContaboVps\ContaboAuth;
use ContaboVps\ContaboProvisioningException;
use ContaboVps\InstanceLinker;
use ContaboVps\Tests\FakeHttpExecutor;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class InstanceLinkerTest extends TestCase
{
    private InstanceLinker $linker;

    protected function setUp(): void
    {
        Capsule::reset();
        $GLOBALS['__activity_log'] = [];
        $this->linker = new InstanceLinker();
    }

    // ── ensureCustomField ────────────────────────────────────────────────────

    public function testCreatesTheCustomFieldWhenMissing(): void
    {
        Capsule::$tables['tblcustomfields'] = [];

        $fieldId = $this->linker->ensureCustomField(7);

        $this->assertGreaterThan(0, $fieldId);
        $this->assertCount(1, Capsule::$inserts);
        $insert = Capsule::$inserts[0];
        $this->assertSame('tblcustomfields', $insert['table']);
        $this->assertSame('product', $insert['values']['type']);
        $this->assertSame(7, $insert['values']['relid']);
        $this->assertSame('contabo_instance_id|Contabo Instance ID', $insert['values']['fieldname']);
        $this->assertSame('on', $insert['values']['adminonly']);
    }

    public function testFindsBareFieldName(): void
    {
        Capsule::$tables['tblcustomfields'] = [
            ['id' => 31, 'type' => 'product', 'relid' => 7, 'fieldname' => 'contabo_instance_id'],
        ];
        $this->assertSame(31, $this->linker->ensureCustomField(7));
        $this->assertCount(0, Capsule::$inserts);
    }

    public function testFindsPipeFriendlyFieldName(): void
    {
        Capsule::$tables['tblcustomfields'] = [
            ['id' => 32, 'type' => 'product', 'relid' => 7, 'fieldname' => 'contabo_instance_id|Instance'],
        ];
        $this->assertSame(32, $this->linker->ensureCustomField(7));
    }

    public function testDoesNotMatchUnrelatedFields(): void
    {
        Capsule::$tables['tblcustomfields'] = [
            ['id' => 33, 'type' => 'product', 'relid' => 7, 'fieldname' => 'contabo_instance_id_backup'],
        ];
        $fieldId = $this->linker->ensureCustomField(7);
        $this->assertNotSame(33, $fieldId);
        $this->assertCount(1, Capsule::$inserts, 'a fresh field must be created');
    }

    // ── readInstanceId ───────────────────────────────────────────────────────

    public function testReadsFromCustomfieldsParamBareKey(): void
    {
        $id = $this->linker->readInstanceId(['customfields' => ['contabo_instance_id' => ' 1234 ']]);
        $this->assertSame('1234', $id);
    }

    public function testReadsFromCustomfieldsParamPipeKey(): void
    {
        $id = $this->linker->readInstanceId(['customfields' => ['contabo_instance_id|Contabo Instance ID' => '99']]);
        $this->assertSame('99', $id);
    }

    public function testFallsBackToDatabase(): void
    {
        Capsule::$tables['tblcustomfields'] = [
            ['id' => 5, 'type' => 'product', 'relid' => 7, 'fieldname' => 'contabo_instance_id|Contabo Instance ID'],
        ];
        Capsule::$tables['tblcustomfieldsvalues'] = [
            ['fieldid' => 5, 'relid' => 300, 'value' => 'inst-300'],
        ];
        $id = $this->linker->readInstanceId(['serviceid' => 300, 'pid' => 7, 'customfields' => []]);
        $this->assertSame('inst-300', $id);
    }

    public function testReturnsEmptyWhenUnlinked(): void
    {
        $this->assertSame('', $this->linker->readInstanceId(['serviceid' => 300, 'pid' => 7]));
    }

    // ── storeInstanceId ──────────────────────────────────────────────────────

    public function testStoresFreshLink(): void
    {
        Capsule::$tables['tblcustomfieldsvalues'] = [];
        $this->linker->storeInstanceId(300, 5, 'inst-1');
        $this->assertSame('inst-1', Capsule::$tables['tblcustomfieldsvalues'][0]['value']);
    }

    public function testSameIdWriteIsNoOp(): void
    {
        Capsule::$tables['tblcustomfieldsvalues'] = [
            ['fieldid' => 5, 'relid' => 300, 'value' => 'inst-1'],
        ];
        $this->linker->storeInstanceId(300, 5, 'inst-1');
        $this->assertCount(0, Capsule::$calls, 'no update should be issued');
    }

    public function testRefusesToSilentlyRelinkADifferentInstance(): void
    {
        Capsule::$tables['tblcustomfieldsvalues'] = [
            ['fieldid' => 5, 'relid' => 300, 'value' => 'inst-1'],
        ];
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('refusing to silently relink');
        $this->linker->storeInstanceId(300, 5, 'inst-2');
    }

    public function testForceRelinkIsAllowed(): void
    {
        Capsule::$tables['tblcustomfieldsvalues'] = [
            ['fieldid' => 5, 'relid' => 300, 'value' => 'inst-1'],
        ];
        $this->linker->storeInstanceId(300, 5, 'inst-2', true);
        $this->assertSame('inst-2', Capsule::$tables['tblcustomfieldsvalues'][0]['value']);
    }

    // ── tag / displayName ────────────────────────────────────────────────────

    public function testTagMatchingIsExactPerService(): void
    {
        $this->assertTrue(InstanceLinker::displayNameMatchesTag('whmcs-12', 12));
        $this->assertTrue(InstanceLinker::displayNameMatchesTag('whmcs-12 my.host', 12));
        $this->assertFalse(InstanceLinker::displayNameMatchesTag('whmcs-123 my.host', 12), 'whmcs-12 must not match whmcs-123');
        $this->assertFalse(InstanceLinker::displayNameMatchesTag('renamed by admin', 12));
        $this->assertFalse(InstanceLinker::displayNameMatchesTag('', 12));
    }

    // ── verifyOwnership / findByTag ──────────────────────────────────────────

    private function apiClient(FakeHttpExecutor $http): ContaboApiClient
    {
        $http->stubToken();
        $auth = new ContaboAuth('cid', 'cs', 'u@example.com', 'pw', $http);
        return new ContaboApiClient($auth, $http, static function (int $s): void {});
    }

    public function testVerifyOwnershipReportsTagMatch(): void
    {
        $http = new FakeHttpExecutor();
        $http->queue('GET /v1/compute/instances/555', 200, ['data' => [
            ['instanceId' => 555, 'displayName' => 'whmcs-300 my.host', 'status' => 'running'],
        ]]);
        $owned = $this->linker->verifyOwnership($this->apiClient($http), '555', 300);
        $this->assertTrue($owned['exists']);
        $this->assertTrue($owned['tagMatches']);
    }

    public function testVerifyOwnershipDetectsRenamedInstance(): void
    {
        $http = new FakeHttpExecutor();
        $http->queue('GET /v1/compute/instances/555', 200, ['data' => [
            ['instanceId' => 555, 'displayName' => 'production-db', 'status' => 'running'],
        ]]);
        $owned = $this->linker->verifyOwnership($this->apiClient($http), '555', 300);
        $this->assertTrue($owned['exists']);
        $this->assertFalse($owned['tagMatches']);
    }

    public function testVerifyOwnershipHandles404(): void
    {
        $http = new FakeHttpExecutor();
        $http->queue('GET /v1/compute/instances/555', 404, ['message' => 'not found']);
        $owned = $this->linker->verifyOwnership($this->apiClient($http), '555', 300);
        $this->assertFalse($owned['exists']);
    }

    public function testFindByTagReturnsSingleMatch(): void
    {
        $http = new FakeHttpExecutor();
        $http->queue('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 1, 'displayName' => 'whmcs-2999 other'],
            ['instanceId' => 2, 'displayName' => 'whmcs-300 mine'],
            ['instanceId' => 3, 'displayName' => 'unrelated'],
        ]]);
        $found = $this->linker->findByTag($this->apiClient($http), 300);
        $this->assertNotNull($found);
        $this->assertSame(2, $found['instanceId']);
    }

    public function testFindByTagReturnsNullWhenAbsent(): void
    {
        $http = new FakeHttpExecutor();
        $http->stub('GET /v1/compute/instances?', 200, ['data' => []]);
        $this->assertNull($this->linker->findByTag($this->apiClient($http), 300));
    }

    public function testFindByTagUsesServerSideSearchFilter(): void
    {
        $http = new FakeHttpExecutor();
        $http->stub('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 2, 'displayName' => 'whmcs-300 mine'],
        ]]);
        $found = $this->linker->findByTag($this->apiClient($http), 300);
        $this->assertNotNull($found);
        $this->assertSame(2, $found['instanceId']);
        // The very first list call must carry the tag as a server-side search
        // filter, so recovery never depends on scanning the whole account.
        $listCalls = $http->callsMatching('GET https://api.contabo.com/v1/compute/instances?');
        $this->assertStringContainsString('search=whmcs-300', $listCalls[0]['url']);
    }

    public function testFindByTagDedupesRepeatedRows(): void
    {
        // Search + fallback could each surface the same instance — must not
        // count as an ambiguous duplicate.
        $http = new FakeHttpExecutor();
        $http->stub('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 7, 'displayName' => 'whmcs-300 a'],
            ['instanceId' => 7, 'displayName' => 'whmcs-300 a'],
        ]]);
        $found = $this->linker->findByTag($this->apiClient($http), 300);
        $this->assertNotNull($found);
        $this->assertSame(7, $found['instanceId']);
    }

    public function testFindByTagThrowsOnAmbiguity(): void
    {
        $http = new FakeHttpExecutor();
        $http->queue('GET /v1/compute/instances?', 200, ['data' => [
            ['instanceId' => 1, 'displayName' => 'whmcs-300 a'],
            ['instanceId' => 2, 'displayName' => 'whmcs-300 b'],
        ]]);
        $this->expectException(ContaboProvisioningException::class);
        $this->expectExceptionMessage('refusing to guess');
        $this->linker->findByTag($this->apiClient($http), 300);
    }
}
