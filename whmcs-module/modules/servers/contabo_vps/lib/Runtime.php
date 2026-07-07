<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * Tiny factory wiring the module's object graph from WHMCS $params. The
 * entry-file functions call Runtime::instanceService($params); tests swap the
 * factory to inject a scripted HttpExecutor without touching curl.
 */
final class Runtime
{
    /** @var callable|null test seam: fn(array $params): InstanceService */
    private static $factory = null;

    /** @param callable|null $factory fn(array $params): InstanceService */
    public static function swap(?callable $factory): void
    {
        self::$factory = $factory;
    }

    /** @param array<string,mixed> $params */
    public static function instanceService(array $params): InstanceService
    {
        if (self::$factory !== null) {
            return call_user_func(self::$factory, $params);
        }
        return self::instanceServiceWithClient(new ContaboApiClient(self::auth($params)));
    }

    /**
     * Build the object graph around an already-constructed client. Lets the
     * cron sweep reuse ONE authenticated client (one token) across every
     * service on the same server, instead of re-authenticating per service.
     */
    public static function instanceServiceWithClient(ContaboApiClient $client): InstanceService
    {
        return new InstanceService(
            $client,
            new InstanceLinker(),
            new SecretManager($client),
            new ConfigOptionResolver(),
            new ImageResolver($client),
            new ContaboInstanceMapper()
        );
    }

    /**
     * Build the OAuth2 credential holder from WHMCS server settings:
     * Username = client_id, Password = client_secret,
     * Access Hash = "apiUser:apiPassword".
     *
     * @param array<string,mixed> $params
     */
    public static function auth(array $params): ContaboAuth
    {
        $clientId     = (string) ($params['serverusername'] ?? '');
        $clientSecret = (string) ($params['serverpassword'] ?? '');
        $accessHash   = trim((string) ($params['serveraccesshash'] ?? ''));

        if ($clientId === '' || $clientSecret === '' || $accessHash === '') {
            throw new ContaboProvisioningException('Server credentials not configured — set Username (client id), Password (client secret) and Access Hash ("apiUser:apiPassword") on the WHMCS server');
        }

        $parts = array_pad(explode(':', $accessHash, 2), 2, '');
        $apiUser     = trim((string) $parts[0]);
        $apiPassword = (string) $parts[1];
        if ($apiUser === '' || $apiPassword === '') {
            throw new ContaboProvisioningException('Access Hash must be "apiUser:apiPassword" (Contabo API user email + password)');
        }

        return new ContaboAuth($clientId, $clientSecret, $apiUser, $apiPassword);
    }
}
