<?php
declare(strict_types=1);

use SecuriAceVps\ContaboApiClient;
use SecuriAceVps\ContaboAuth;
use SecuriAceVps\SecretManager;
use SecuriAceVps\Tests\FakeHttpExecutor;
use PHPUnit\Framework\TestCase;

final class SecretManagerTest extends TestCase
{
    private FakeHttpExecutor $http;
    private SecretManager $secrets;

    protected function setUp(): void
    {
        $GLOBALS['__activity_log'] = [];
        $this->http = new FakeHttpExecutor();
        $this->http->stubToken();
        $auth = new ContaboAuth('cid', 'cs', 'u@example.com', 'pw', $this->http);
        $client = new ContaboApiClient($auth, $this->http, static function (int $s): void {});
        $this->secrets = new SecretManager($client);
    }

    public function testCreatesSecretWhenMissing(): void
    {
        $this->http->queue('GET /v1/secrets', 200, ['data' => []]);
        $this->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 555]]]);

        $id = $this->secrets->ensureRootPasswordSecret(42, 'S3curePass');
        $this->assertSame(555, $id);

        $create = $this->http->callsMatching('POST https://api.contabo.com/v1/secrets');
        $this->assertCount(1, $create);
        $body = json_decode((string) $create[0]['body'], true);
        $this->assertSame('whmcs-svc-42-root', $body['name']);
        $this->assertSame('password', $body['type']);
        $this->assertSame('S3curePass', $body['value']);
    }

    public function testPatchesExistingSecretInPlace(): void
    {
        $this->http->queue('GET /v1/secrets', 200, ['data' => [
            ['secretId' => 900, 'name' => 'whmcs-svc-42-root', 'type' => 'password'],
        ]]);

        $id = $this->secrets->ensureRootPasswordSecret(42, 'NewPass1');
        $this->assertSame(900, $id);
        $this->assertCount(1, $this->http->callsMatching('PATCH https://api.contabo.com/v1/secrets/900'));
        $this->assertCount(0, $this->http->callsMatching('POST https://api.contabo.com/v1/secrets'));
    }

    public function testNameFilterIsPrefixSafe(): void
    {
        // The API name param filters loosely — a similarly named secret for a
        // DIFFERENT service must not be patched.
        $this->http->queue('GET /v1/secrets', 200, ['data' => [
            ['secretId' => 901, 'name' => 'whmcs-svc-421-root', 'type' => 'password'],
        ]]);
        $this->http->queue('POST /v1/secrets', 201, ['data' => [['secretId' => 902]]]);

        $id = $this->secrets->ensureRootPasswordSecret(42, 'NewPass1');
        $this->assertSame(902, $id, 'must create a new secret, not adopt svc-421\'s');
    }

    public function testEmptyPasswordRefused(): void
    {
        $this->expectException(\SecuriAceVps\ContaboProvisioningException::class);
        $this->secrets->ensureRootPasswordSecret(42, '');
    }

    public function testCleanupDeletesTheServiceSecret(): void
    {
        $this->http->queue('GET /v1/secrets', 200, ['data' => [
            ['secretId' => 77, 'name' => 'whmcs-svc-42-root', 'type' => 'password'],
        ]]);
        $this->secrets->cleanupServiceSecrets(42);
        $this->assertCount(1, $this->http->callsMatching('DELETE https://api.contabo.com/v1/secrets/77'));
    }

    public function testCleanupSwallowsFailures(): void
    {
        $this->http->queue('GET /v1/secrets', 500, ['message' => 'vault down']);
        $this->http->queue('GET /v1/secrets', 500, ['message' => 'vault down']);
        $this->http->queue('GET /v1/secrets', 500, ['message' => 'vault down']);
        $this->http->queue('GET /v1/secrets', 500, ['message' => 'vault down']);

        $this->secrets->cleanupServiceSecrets(42); // must not throw
        $this->assertNotEmpty($GLOBALS['__activity_log']);
        $this->assertStringContainsString('could not clean up', $GLOBALS['__activity_log'][0]);
    }

    public function testGeneratedPasswordsAreStrong(): void
    {
        for ($i = 0; $i < 20; $i++) {
            $pw = SecretManager::generatePassword();
            $this->assertGreaterThanOrEqual(20, strlen($pw));
            $this->assertMatchesRegularExpression('/[A-Z]/', $pw);
            $this->assertMatchesRegularExpression('/[a-z]/', $pw);
            $this->assertMatchesRegularExpression('/[0-9]/', $pw);
            $this->assertMatchesRegularExpression('/^[A-Za-z0-9]+$/', $pw);
        }
        $this->assertNotSame(SecretManager::generatePassword(), SecretManager::generatePassword());
    }
}
