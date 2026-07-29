<?php
declare(strict_types=1);

namespace SecuriAceVps\Tests;

use PHPUnit\Framework\TestCase;
use SecuriAceVps\CommunicationService;
use WHMCS\Database\Capsule;

final class CommunicationServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Harness::reset();
        Capsule::$columns['mod_securiacevps_communications'] = ['id'];
        Capsule::$tables['mod_securiacevps_communications'] = [];
        Capsule::$columns['mod_securiacevps_operations'] = ['id'];
        Capsule::$tables['mod_securiacevps_operations'] = [];
    }

    protected function tearDown(): void
    {
        Harness::reset();
    }

    public function testReadyMessageIsQueuedOnceWithoutSecretPayload(): void
    {
        $operation = $this->operation('create', 'succeeded');
        $service = new CommunicationService();

        $service->queueForOperation($operation);
        $service->queueForOperation($operation);

        $this->assertCount(1, Capsule::$tables['mod_securiacevps_communications']);
        $row = Capsule::$tables['mod_securiacevps_communications'][0];
        $this->assertSame('SecuriAce VPS Ready', $row['template_name']);
        $this->assertSame('pending', $row['state']);
        $this->assertArrayNotHasKey('payload_json', $row);
        $encoded = json_encode($row);
        $this->assertStringNotContainsString('password', strtolower((string) $encoded));
        $this->assertStringNotContainsString('credential', strtolower((string) $encoded));
    }

    public function testOnlyEligibleOperationTransitionsCreateMessages(): void
    {
        $service = new CommunicationService();
        $service->queueForOperation($this->operation('power_restart', 'succeeded'));
        $service->queueForOperation($this->operation('create', 'provider_pending', 1));
        $service->queueForOperation($this->operation('create', 'provider_pending', 2));
        $service->queueForOperation($this->operation('create', 'manual_review', 2, 'op-review'));
        $service->queueForOperation($this->operation('reset_password', 'succeeded', 1, 'op-reset'));
        $service->queueForOperation($this->operation('reinstall', 'succeeded', 1, 'op-reinstall'));

        $this->assertSame(
            [
                'provisioning_delayed',
                'provisioning_review',
                'password_reset_complete',
                'reinstall_complete',
            ],
            array_column(Capsule::$tables['mod_securiacevps_communications'], 'message_type')
        );
    }

    public function testQueueSendsThroughWhmcsProductEmailContract(): void
    {
        $operation = $this->operation('create', 'succeeded');
        Capsule::$tables['mod_securiacevps_operations'][] = $operation;
        $GLOBALS['__local_api_handler'] = static function (): array {
            return ['result' => 'success'];
        };
        $service = new CommunicationService();
        $service->queueForOperation($operation);

        $service->processQueue();

        $this->assertSame('sent', Capsule::$tables['mod_securiacevps_communications'][0]['state']);
        $this->assertSame(1, Capsule::$tables['mod_securiacevps_communications'][0]['attempt_count']);
        $this->assertCount(1, $GLOBALS['__local_api_calls']);
        $call = $GLOBALS['__local_api_calls'][0];
        $this->assertSame('SendEmail', $call['command']);
        $this->assertSame('product', $call['parameters']['customtype']);
        $this->assertSame(300, $call['parameters']['id']);
        $vars = unserialize(base64_decode((string) $call['parameters']['customvars']), ['allowed_classes' => false]);
        $this->assertSame(['operation_reference' => 'corr-300'], $vars);
    }

    public function testDeliveryFailureRetriesThenRequiresOperatorAttention(): void
    {
        $operation = $this->operation('create', 'succeeded');
        Capsule::$tables['mod_securiacevps_operations'][] = $operation;
        $GLOBALS['__local_api_handler'] = static function (): array {
            return ['result' => 'error', 'message' => 'mail transport detail must not persist'];
        };
        $service = new CommunicationService();
        $service->queueForOperation($operation);

        for ($attempt = 1; $attempt <= 3; $attempt++) {
            Capsule::$tables['mod_securiacevps_communications'][0]['next_attempt_at'] =
                date('Y-m-d H:i:s', time() - 1);
            $service->processQueue();
        }

        $row = Capsule::$tables['mod_securiacevps_communications'][0];
        $this->assertSame('failed', $row['state']);
        $this->assertSame(3, $row['attempt_count']);
        $this->assertSame('email_delivery_failed', $row['safe_error_code']);
        $this->assertNull($row['next_attempt_at']);
        $this->assertStringNotContainsString('transport detail', json_encode($row));
    }

    /**
     * @return array<string,mixed>
     */
    private function operation(
        string $type,
        string $state,
        int $attempt = 1,
        string $uuid = 'op-ready'
    ): array {
        return [
            'id' => count(Capsule::$tables['mod_securiacevps_operations']) + 1,
            'operation_uuid' => $uuid,
            'service_id' => 300,
            'operation_type' => $type,
            'state' => $state,
            'attempt_count' => $attempt,
            'correlation_id' => 'corr-300',
        ];
    }
}
