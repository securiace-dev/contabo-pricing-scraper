# Production deployment

`contabo-pricing` ships as a single Rust binary (with Node bundled for fallback)
inside a Docker image. This directory now includes the production-aligned
Dokploy sidecar shape as well as the standalone container recipes.

> **Current production uses an internal Docker sidecar, not a native `systemd`
> unit.** The live `my.securiace.com` deployment runs `contabo-pricing` as a
> private container on the Dokploy WHMCS network, with host-owned data at
> `/var/lib/contabo-pricing/output`, bearer auth from
> `/etc/contabo-pricing/auth_token`, and optional proxy credentials from
> `/etc/contabo-pricing/proxy.env`. See the root
> [README → Production Architecture & Operational Reality](../README.md#production-architecture--operational-reality-dev--ops-deep-dive)
> for the as-deployed truth and the dual version streams.
>
> **Cloudflare upstream constraint — mitigated.** The scraper is `403`-blocked
> from datacenter IPs. This is now resolved by routing fetches through
> **`SCRAPER_PROXY`** (a residential/gateway proxy): set on prod via a
> `chmod 600` host-owned env file, and in CI via the `SCRAPER_PROXY` secret in
> the **`Build`** environment (used by both `scrape.yml` and `parity.yml`).
> With the proxy, plain `reqwest` mode succeeds and refreshes pull fresh data.

## Production scraper deploy (Dokploy WHMCS sidecar)

This is the canonical, lowest-risk process for the live `my.securiace.com`
topology: a private `contabo-pricing` container attached to the same Docker
network as the Dokploy-hosted WHMCS app.

The steady-state invariants are:

- service DNS inside WHMCS stays `http://contabo-pricing:8080/api/v1`;
- snapshot data stays operator-owned on the host at
  `/var/lib/contabo-pricing/output`;
- bearer auth stays operator-owned on the host at
  `/etc/contabo-pricing/auth_token`;
- proxy credentials stay operator-owned on the host at
  `/etc/contabo-pricing/proxy.env`;
- no public ingress or host-port publish exists for the API.

Tracked files for this shape:

- `docker-compose.dokploy-sidecar.yml`
- `dokploy-sidecar.env.example`
- `proxy.env.example`

```bash
# 1. Prepare the host-owned runtime files once.
install -d -m 755 /var/lib/contabo-pricing/output /etc/contabo-pricing
chmod 640 /etc/contabo-pricing/auth_token
chmod 600 /etc/contabo-pricing/proxy.env   # if SCRAPER_PROXY is used

# 2. Copy the tracked examples locally, then fill in the real network/path values.
cd deploy
cp dokploy-sidecar.env.example .env
# edit .env -> set WHMCS_DOCKER_NETWORK and pin CONTABO_PRICING_IMAGE to the release digest

# 3. Validate the sidecar definition before applying it.
docker compose --env-file .env -f docker-compose.dokploy-sidecar.yml config -q

# 4. Start or update the internal-only sidecar.
docker compose --env-file .env -f docker-compose.dokploy-sidecar.yml up -d

# 5. Verify from the WHMCS network context.
docker exec whmcs-production-jvjwfo-app-1 \
  curl -fsS http://contabo-pricing:8080/api/v1/health
docker exec whmcs-production-jvjwfo-app-1 \
  curl -s http://contabo-pricing:8080/api/v1/meta | jq '.scraper_version, .snapshot_meta.generated_at, .snapshot_meta.plan_count'
```

**Rules (do not break these):**

- The proxy credential lives **only** in `/etc/contabo-pricing/proxy.env`
  (`chmod 600`) on the host, or the `SCRAPER_PROXY` GitHub *environment*
  secret. Never commit a real proxy credential.
- Treat `docker-compose.dokploy-sidecar.yml` as the production-owned shape for
  Dokploy-hosted WHMCS. Do not rely on a manually created one-off container.
- Pin `CONTABO_PRICING_IMAGE` to an immutable release reference such as
  `ghcr.io/securiace-dev/contabo-pricing-scraper@sha256:...`. Do not use
  `:latest` in this production path.
- Keep the sidecar private: no public ingress, no host-port publishing, and no
  addon target of `localhost` or `127.0.0.1` from inside the WHMCS container.
- Keep the host-owned bind mounts stable: `/var/lib/contabo-pricing/output`,
  `/etc/contabo-pricing/auth_token`, and `/etc/contabo-pricing/proxy.env`.
- The live addon should resolve the API through Docker DNS at
  `http://contabo-pricing:8080/api/v1`, either via saved `api_base_url` or
  `CONTABO_PRICING_API_BASE_URL`.
- `SCRAPER_PROXY` may be schemeless on >= the normalize fix, but always supply
  the `http://` scheme for compatibility with older binaries such as `v2.3.2`.

## With Dokploy-hosted WHMCS sidecar

Use the dedicated sidecar file when WHMCS already runs under Dokploy and only
needs a private companion API container:

```bash
cd deploy
cp dokploy-sidecar.env.example .env
docker compose --env-file .env -f docker-compose.dokploy-sidecar.yml config -q
docker compose --env-file .env -f docker-compose.dokploy-sidecar.yml up -d
```

Required non-secret settings:

| Var | Example | Notes |
|---|---|---|
| `WHMCS_DOCKER_NETWORK` | `whmcs-production-jvjwfo_default` | External Docker network shared by the Dokploy WHMCS app |
| `CONTABO_PRICING_IMAGE` | `ghcr.io/securiace-dev/contabo-pricing-scraper@sha256:...` | Required immutable release image reference for the sidecar |
| `CONTABO_DATA_HOST_DIR` | `/var/lib/contabo-pricing/output` | Durable snapshot store on the host |
| `CONTABO_AUTH_TOKEN_HOST_FILE` | `/etc/contabo-pricing/auth_token` | Host-owned bearer token file, bind-mounted read-only |
| `CONTABO_PRICING_API_BASE_URL` | `http://contabo-pricing:8080/api/v1` | Value WHMCS should use from inside the app container |

## Quick start (no reverse proxy)

```bash
echo "$(openssl rand -hex 32)" > deploy/auth_token.txt
cd deploy
docker compose up -d
curl -fsS http://localhost:8080/api/v1/health
```

The base compose runs the API on host port 8080 with a generated bearer token
mounted as a Docker secret. The token is required for `POST /api/v1/refresh`;
read endpoints are open.

## With automatic HTTPS via Caddy

```bash
echo "$(openssl rand -hex 32)" > deploy/auth_token.txt
API_DOMAIN=pricing.example.com \
  docker compose -f docker-compose.yml -f docker-compose.caddy.yml up -d
```

Caddy v2 listens on `:80` / `:443`, fetches a Let's Encrypt cert for
`${API_DOMAIN}`, and proxies to the API container.

## With Traefik

Assumes you already run Traefik with the `traefik` network and a `le` cert
resolver:

```bash
API_DOMAIN=pricing.example.com TRAEFIK_NETWORK=traefik \
  docker compose -f docker-compose.yml -f docker-compose.traefik.yml up -d
```

If your Traefik setup uses different names, edit
`docker-compose.traefik.yml` accordingly.

## With Coolify

In the Coolify UI, create a new "Docker Compose" application and paste the
contents of `docker-compose.yml` + `docker-compose.coolify.yml`. Set
environment variables:

| Var | Example | Notes |
|---|---|---|
| `COOLIFY_FQDN` | `pricing.example.com` | Used to route Coolify's built-in proxy |
| `CONTABO_AUTH_TOKEN` | `...` | Optional: inline instead of secret file |
| `CONTABO_REFRESH_CRON` | _(no effect - see note)_ | Accepted but not wired to a scheduler; use external automation instead |

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `CONTABO_BIND` | `0.0.0.0:8080` | bind address |
| `CONTABO_DATA_DIR` | `/app/data/output` | snapshot directory (watched for hot-reload) |
| `CONTABO_AUTH_TOKEN_FILE` | `/run/secrets/contabo_auth_token` | bearer token for write endpoints |
| `CONTABO_AUTH_TOKEN` | unset | alternative to the file form |
| `CONTABO_REFRESH_CRON` | unset | Accepted but not currently wired to an in-app scheduler; refresh must be driven externally |
| `CONTABO_CORS_ORIGIN` | unset | repeatable CORS allow-origin |
| `CONTABO_SCRAPER_CMD` | unset | override scraper invocation (for example `node /app/scripts/contabo_scraper.js`) |
| `RUST_LOG` | `info` | tracing-subscriber filter |

## Verifying a deployment

For public or reverse-proxied deployments:

```bash
curl -fsS https://pricing.example.com/api/v1/health
curl -s https://pricing.example.com/api/v1/meta | jq '.scraper_version, .snapshot_meta.generated_at'
```

For the Dokploy sidecar shape, verify from inside the WHMCS container or from a
container attached to the same Docker network:

```bash
docker exec whmcs-production-jvjwfo-app-1 \
  curl -fsS http://contabo-pricing:8080/api/v1/health
docker exec whmcs-production-jvjwfo-app-1 \
  curl -s http://contabo-pricing:8080/api/v1/meta | jq '.scraper_version, .snapshot_meta.generated_at, .snapshot_meta.plan_count'
```

## Choosing a deployment shape

| Shape | Best for | TLS | Extras |
|---|---|---|---|
| Dokploy sidecar | Dokploy-hosted WHMCS needing a private companion API | inherited from WHMCS ingress | host-owned data, token, proxy, Docker-DNS reachability |
| (none / base) | dev, behind external LB | no | host-port `8080` |
| Caddy | small production, single host | yes - Let's Encrypt | HTTP/3, security headers |
| Traefik | multi-app reverse proxy already in place | yes - your existing resolver | label-based service discovery |
| Coolify | self-hosted PaaS with UI | yes - Coolify-managed | one-click redeploys, env management UI |

Pick the one that matches your operations model. The Dokploy sidecar file is
the production-aligned choice when WHMCS already owns ingress and only needs a
private scraper/API runtime on its Docker network.
