<?php
declare(strict_types=1);

namespace ContaboVps;

/**
 * OAuth2 password-grant token provider for the Contabo API (Keycloak).
 *
 * Tokens are short-lived (≈5 min). This holds the token in-memory for the
 * lifetime of the request and proactively refreshes it 60s before expiry, so a
 * single WHMCS provisioning action (which may make several API calls) never
 * trips an expiry mid-flight. The API client also force-refreshes on a 401.
 *
 * Credentials are passed in by the caller (decoded from WHMCS-encrypted server
 * settings) and are never logged or persisted by this class.
 */
class ContaboAuth
{
    private const TOKEN_URL = 'https://auth.contabo.com/auth/realms/contabo/protocol/openid-connect/token';
    private const REFRESH_BEFORE_EXPIRY_SEC = 60;

    /** @var string */ private $clientId;
    /** @var string */ private $clientSecret;
    /** @var string */ private $apiUser;
    /** @var string */ private $apiPassword;
    /** @var string|null */ private $accessToken = null;
    /** @var int */ private $expiresAt = 0;

    public function __construct(string $clientId, string $clientSecret, string $apiUser, string $apiPassword)
    {
        $this->clientId     = $clientId;
        $this->clientSecret = $clientSecret;
        $this->apiUser      = $apiUser;
        $this->apiPassword  = $apiPassword;
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

        $ch = curl_init(self::TOKEN_URL);
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $body,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_TIMEOUT        => 15,
            CURLOPT_HTTPHEADER     => ['Content-Type: application/x-www-form-urlencoded'],
        ]);

        $raw  = curl_exec($ch);
        $code = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err  = curl_error($ch);
        curl_close($ch);

        if ($raw === false || $err !== '') {
            throw new ContaboProvisioningException('Token fetch curl error: ' . $err);
        }

        $data = json_decode((string) $raw, true);
        if (!is_array($data) || empty($data['access_token'])) {
            throw new ContaboProvisioningException('Token fetch failed (HTTP ' . $code . ')');
        }

        $this->accessToken = (string) $data['access_token'];
        $this->expiresAt   = time() + (int) ($data['expires_in'] ?? 300);
    }
}
