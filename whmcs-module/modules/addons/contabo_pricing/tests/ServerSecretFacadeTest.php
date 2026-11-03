<?php
declare(strict_types=1);

namespace {
    if (!class_exists('DI')) {
        /** Stands in for WHMCS's DI facade: make() is only reachable through __callStatic, as in WHMCS. */
        final class DI
        {
            /** @var mixed */
            public static $config = null;

            /** @param array<int,mixed> $args @return mixed */
            public static function __callStatic(string $name, array $args)
            {
                if ($name !== 'make' || self::$config === null) {
                    throw new \RuntimeException('container not available');
                }
                return self::$config;
            }
        }
    }
}

namespace ContaboPricing\Tests {

    use ContaboPricing\PaidOrderSnapshotService;
    use PHPUnit\Framework\TestCase;
    use WHMCS\Database\Capsule;

    /** On a real WHMCS the key is not in $GLOBALS in an addon request; it comes from DI::make('config'). */
    final class ServerSecretFacadeTest extends TestCase
    {
        use SnapshotFixture;

        protected function setUp(): void
        {
            $this->bootFixture();
            Capsule::$tables['tblinvoiceitems'] = [];
            unset($GLOBALS['cc_encryption_hash']);
            \DI::$config = null;
            $service = new PaidOrderSnapshotService();
            $service->createDraft(10, 20, 30);
            Capsule::table('mod_securiacevps_order_snapshots')->update(['state' => 'invalid']);
            Capsule::table('tblinvoices')->where('id', 20)->update(['status' => 'Paid']);
            Capsule::table('tblorders')->where('id', 10)->update(['status' => 'Active']);
            Capsule::table('tblhosting')->where('id', 30)->update(['firstpaymentamount' => '1.00', 'amount' => '1.00']);
        }

        protected function tearDown(): void
        {
            \DI::$config = null;
        }

        public function testTheKeyIsReadThroughTheDiFacadeAsArrayAccess(): void
        {
            \DI::$config = new \ArrayObject(['cc_encryption_hash' => 'whmcs-config-key']);
            $plan = (new PaidOrderSnapshotService())->requotePlan(30);
            $this->assertSame('needs_override', $plan['code']);
            $this->assertNotSame('', $plan['diff_token']);
        }

        public function testTheKeyIsReadFromAConfigObjectWithAProperty(): void
        {
            \DI::$config = (object) ['cc_encryption_hash' => 'whmcs-config-key'];
            $this->assertNotSame('', (new PaidOrderSnapshotService())->requotePlan(30)['diff_token']);
        }

        public function testTwoDifferentKeysGiveDifferentTokens(): void
        {
            \DI::$config = new \ArrayObject(['cc_encryption_hash' => 'key-one']);
            $a = (new PaidOrderSnapshotService())->requotePlan(30)['diff_token'];
            \DI::$config = new \ArrayObject(['cc_encryption_hash' => 'key-two']);
            $b = (new PaidOrderSnapshotService())->requotePlan(30)['diff_token'];
            $this->assertNotSame($a, $b);
        }

        public function testTheDiagnosticNamesTheSourceAndNeverReturnsTheSecret(): void
        {
            $service = new PaidOrderSnapshotService();
            $this->assertSame(['available' => false, 'source' => 'none', 'can_sign' => false], $service->confirmationKeyStatus());

            \DI::$config = new \ArrayObject(['cc_encryption_hash' => 'whmcs-config-key']);
            $status = $service->confirmationKeyStatus();
            $this->assertSame(['available' => true, 'source' => 'WHMCS application config (DI config, array access)', 'can_sign' => true], $status);

            \DI::$config = (object) ['cc_encryption_hash' => 'whmcs-config-key'];
            $this->assertSame('WHMCS application config (DI config, property)', $service->confirmationKeyStatus()['source']);

            $GLOBALS['cc_encryption_hash'] = 'a-global-key';
            $status = $service->confirmationKeyStatus();
            $this->assertSame('php global $cc_encryption_hash', $status['source']);
            $this->assertTrue($status['can_sign']);
            foreach (['whmcs-config-key', 'a-global-key'] as $secret) {
                $this->assertStringNotContainsString($secret, json_encode($status));
            }
        }

        public function testTheDiagnosticSaysNoForAnEmptyKey(): void
        {
            \DI::$config = new \ArrayObject(['cc_encryption_hash' => '']);
            $status = (new PaidOrderSnapshotService())->confirmationKeyStatus();
            $this->assertFalse($status['available']);
            $this->assertFalse($status['can_sign']);
        }

        public function testNoKeyAnywhereFailsClosed(): void
        {
            $plan = (new PaidOrderSnapshotService())->requotePlan(30);
            $this->assertSame('token_unavailable', $plan['code']);
            \DI::$config = new \ArrayObject(['cc_encryption_hash' => '']);
            $this->assertSame('token_unavailable', (new PaidOrderSnapshotService())->requotePlan(30)['code']);
        }
    }
}
