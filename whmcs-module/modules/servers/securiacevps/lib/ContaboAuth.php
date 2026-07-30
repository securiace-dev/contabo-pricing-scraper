<?php
declare(strict_types=1);

namespace SecuriAceVps;

/**
 * OAuth2 password-grant token provider for the Contabo API (Keycloak).
 *
 * Tokens are short-lived (≈5 min). This holds the token in-memory for the
 * lifetime of the request and proactively refreshes it 60s before expiry, so a
 * single WHMCS provisioning action (which may make several API calls) never
 * trips an expiry mid-flight. The API client also force-refreshes on a 401.
 *
 * Credentials are passed in by the caller (decoded from WHMCS-encrypted server
 * settings) and are never logged or persisted by this class. Transport goes
 * through the injectable HttpExecutor seam so tests never hit the network.
 */
class ContaboAuth
{
    private const TOKEN_URL = 'https://auth.contabo.com/auth/realms/contabo/protocol/openid-connect/token';
    private const REFRESH_BEFORE_EXPIRY_SEC = 60;
    private const TIMEOUT_SEC = 15;

    /** @var string */ private $clientId;
    /** @var string */ private $clientSecret;
    /** @var string */ private $apiUser;
    /** @var string */ private $apiPassword;
    /** @var HttpExecutor */ private $executor;
    /** @var string|null */ private $accessToken = null;
    /** @var int */ private $expiresAt = 0;

    public function __construct(
        string $clientId,
        string $clientSecret,
        string $apiUser,
        string $apiPassword,
        ?HttpExecutor $executor = null
    ) {
        $this->clientId     = $clientId;
        $this->clientSecret = $clientSecret;
        $this->apiUser      = $apiUser;
        $this->apiPassword  = $apiPassword;
        $this->executor     = $executor !== null ? $executor : new CurlHttpExecutor();
    }

    public function getToken(): string
    {
        if ($this->accessToken !== null && time() < ($this->expiresAt - self::REFRESH_BEFORE_EXPIRY_SEC)) {
            return $this->accessToken;
        }
        $this->fetchToken();
        return (string) $this->accessToken;
    }

    public function forceRefresh(): void
    {
        $this->accessToken = null;
        $this->expiresAt   = 0;
        $this->fetchToken();
    }

    private function fetchToken(): void
    {
        $body = http_build_query([
            'grant_type'    => 'password',
            'client_id'     => $this->clientId,
            'client_secret' => $this->clientSecret,
            'username'      => $this->apiUser,
            'password'      => $this->apiPassword,
        ]);

        list($code, $raw, $errno, $err) = $this->executor->execute(
            'POST',
            self::TOKEN_URL,
            ['Content-Type: application/x-www-form-urlencoded'],
            $body,
            self::TIMEOUT_SEC
        );

        if ($errno !== 0 || ($raw === '' && $code === 0)) {
            throw new ContaboProvisioningException('Token fetch transport error: ' . ($err !== '' ? $err : 'no response'));
        }

        $data = json_decode($raw, true);
        if (!is_array($data) || empty($data['access_token'])) {
            // Deliberately terse: never echo the response body (it could carry
            // credential hints) — the HTTP status is enough to diagnose.
            throw new ContaboProvisioningException('Authentication with Contabo failed (HTTP ' . $code . ') — check client id/secret and API user credentials');
        }

        $this->accessToken = (string) $data['access_token'];
        $this->expiresAt   = time() + (int) ($data['expires_in'] ?? 300);
    }
}
