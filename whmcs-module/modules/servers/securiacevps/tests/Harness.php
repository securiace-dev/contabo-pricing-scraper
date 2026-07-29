<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use SecuriAceVps\ConfigOptionResolver;
use SecuriAceVps\ContaboApiClient;
use SecuriAceVps\ContaboAuth;
use SecuriAceVps\ContaboInstanceMapper;
use SecuriAceVps\ImageResolver;
use SecuriAceVps\InstanceLinker;
use SecuriAceVps\InstanceService;
use SecuriAceVps\Runtime;
use SecuriAceVps\SecretManager;
use WHMCS\Database\Capsule;

/**
 * Shared wiring for flow tests: a full InstanceService over a scripted
 * FakeHttpExecutor, installed as the Runtime factory so the REAL entry-file
 * functions (securiacevps_CreateAccount, …) are what gets exercised.
 */
final class Harness
{
    public FakeHttpExecutor $http;

    public function __construct()
    {
        $this->http = new FakeHttpExecutor();
        $this->http->stubToken();

        $http = $this->http;
        Runtime::swap(static function (array $params) use ($http): InstanceService {
            $auth   = new ContaboAuth('cid', 'cs', 'u@example.com', 'pw', $http);
            $client = new ContaboApiClient($auth, $http, static function (int $s): void {});
            return new InstanceService(
                $client,
                new InstanceLinker(),
                new SecretManager($client),
                new ConfigOptionResolver(),
                new ImageResolver($client),
                new ContaboInstanceMapper()
            );
        });
        Runtime::swapLifecycle(static function () {
            return new LegacyLifecycleAdapter();
        });
    }

    public static function reset(): void
    {
        Runtime::swap(null);
        Runtime::swapLifecycle(null);
        Capsule::reset();
        $GLOBALS['__activity_log'] = [];
        $GLOBALS['__module_log']   = [];
    }

    /** @return array<string,mixed> standard provisioning params for service #300 on product #7 */
    public static function params(array $overrides = []): array
    {
        return array_merge([
            'serviceid'     => 300,
            'pid'           => 7,
            'domain'        => 'vps.example.com',
            'billingcycle'  => 'Monthly',
            'password'      => 'WhmcsGenerated1',
            'customfields'  => [],
            'configoption1' => 'afecbb85-e2fc-46f0-9684-b46b1faf00bb',
            'configoption2' => 'EU',
            'configoption3' => '',
            'configoption4' => 'V45',
            'configoption5' => '',
            'configoption6' => '',
        ], $overrides);
    }

    /** Seed the WHMCS tables the flows touch. */
    public static function seedWhmcs(): void
    {
        Capsule::$tables['tblcustomfields'] = [
            ['id' => 5, 'type' => 'product', 'relid' => 7, 'fieldname' => 'contabo_instance_id|Contabo Instance ID'],
        ];
        Capsule::$tables['tblcustomfieldsvalues'] = [];
        Capsule::$tables['tblhosting'] = [
            ['id' => 300, 'packageid' => 7, 'dedicatedip' => '', 'assignedips' => '', 'password' => ''],
        ];
    }

    public function linkService(string $instanceId = '9001'): void
    {
        Capsule::$tables['tblcustomfieldsvalues'][] = ['fieldid' => 5, 'relid' => 300, 'value' => $instanceId];
    }

    /** Stub GET instance 9001 as a healthy, correctly tagged instance. */
    public function stubTaggedInstance(string $instanceId = '9001', string $status = 'running'): void
    {
        $this->http->stub('GET /v1/compute/instances/' . $instanceId, 200, ['data' => [[
            'instanceId'  => (int) $instanceId,
            'displayName' => 'whmcs-300 vps.example.com',
            'status'      => $status,
            'region'      => 'EU',
            'imageId'     => 'afecbb85-e2fc-46f0-9684-b46b1faf00bb',
            'createdDate' => '2026-07-01T00:00:00Z',
            'ipConfig'    => ['v4' => [['ip' => '203.0.113.10'], ['ip' => '203.0.113.11']]],
        ]]]);
    }
}

/**
 * Keeps the pre-existing low-level flow tests focused on InstanceService.
 * NativeFoundationTest and NativeLifecycleTest exercise the durable
 * orchestrator itself.
 */
final class LegacyLifecycleAdapter
{
    /** @param array<string,mixed> $params */
    public function create(array $params): string
    {
        return Runtime::instanceService($params)->create($params);
    }

    /** @param array<string,mixed> $params */
    public function suspend(array $params): string
    {
        return Runtime::instanceService($params)->powerAction($params, 'stop');
    }

    /** @param array<string,mixed> $params */
    public function unsuspend(array $params): string
    {
        return Runtime::instanceService($params)->powerAction($params, 'start');
    }

    /** @param array<string,mixed> $params */
    public function terminate(array $params): string
    {
        return Runtime::instanceService($params)->terminate($params);
    }

    /** @param array<string,mixed> $params */
    public function power(array $params, string $action): string
    {
        return Runtime::instanceService($params)->powerAction($params, $action);
    }
}
