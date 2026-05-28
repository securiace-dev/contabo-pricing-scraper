# Production deployment

`contabo-pricing` ships as a single Rust binary (with Node bundled for fallback) inside a Docker image. This directory has compose recipes for the three common production patterns.

> **⚠️ Current production does NOT use these Docker recipes.** The live deployment runs
> the binary as a **native `systemd` service** (`contabo-pricing.service`, bound to
> `127.0.0.1:8080`, data in `/var/lib/contabo-pricing/output`) — see
> **[Production scraper deploy](#production-scraper-deploy-native--release-binary--proxy)**
> below for the exact, repeatable process. These compose files are a supported
> *alternative* topology. See the root
> [README → Production Architecture & Operational Reality](../README.md#production-architecture--operational-reality-dev--ops-deep-dive)
> for the as-deployed truth and the dual version streams.
>
> **Cloudflare upstream constraint — mitigated.** The scraper is `403`-blocked from
> datacenter IPs. This is now resolved by routing fetches through **`SCRAPER_PROXY`**
> (a residential/gateway proxy): set on prod via a `chmod 600` systemd drop-in, and in
> CI via the `SCRAPER_PROXY` secret in the **`Build`** environment (used by both
> `scrape.yml` and `parity.yml`). With the proxy, plain `reqwest` mode succeeds and
> refreshes pull fresh data — CloakBrowser is only a legacy fallback.

## Production scraper deploy (native + release binary + proxy)

This is the canonical, lowest-risk process for the live API host
(`contabo-pricing.service`, user `contabo`, bound `127.0.0.1:8080`). It ships a
**signed public GitHub release binary** — no build, no GHCR auth, no architecture
change — and configures the proxy. **Rollback is restoring the `.bak` binary.**

```bash
# 1. Download + checksum-verify the latest release binary (assets are PUBLIC)
cd /tmp
BASE=https://github.com/securiace-dev/contabo-pricing-scraper/releases/download/vX.Y.Z
curl -fsSLO "$BASE/contabo-scraper-linux-x86_64"
curl -fsSLO "$BASE/SHA256SUMS.txt"
grep contabo-scraper-linux-x86_64 SHA256SUMS.txt | sha256sum -c -

# 2. Proxy credential — server-side env file ONLY. chmod 600. NEVER commit it.
install -d -m 755 /etc/contabo-pricing
umask 077
printf 'SCRAPER_PROXY=http://USER:PASS@HOST:PORT\n' > /etc/contabo-pricing/proxy.env
chmod 600 /etc/contabo-pricing/proxy.env

# 3. systemd drop-in wires the proxy env into the service
install -d -m 755 /etc/systemd/system/contabo-pricing.service.d
printf '[Service]\nEnvironmentFile=/etc/contabo-pricing/proxy.env\n' \
  > /etc/systemd/system/contabo-pricing.service.d/proxy.conf

# 4. Back up + swap the binary, then reload + restart
cp -a /usr/local/bin/contabo-scraper /usr/local/bin/contabo-scraper.bak-$(date +%F)
install -m 755 /tmp/contabo-scraper-linux-x86_64 /usr/local/bin/contabo-scraper
systemctl daemon-reload && systemctl restart contabo-pricing

# 5. Verify + refresh through the proxy (snapshot generated_at should jump to now)
curl -fsS http://127.0.0.1:8080/api/v1/health
TOKEN=$(cat /etc/contabo-pricing/auth_token)
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/refresh
curl -s http://127.0.0.1:8080/api/v1/meta | jq '.scraper_version, .snapshot_meta.generated_at, .snapshot_meta.plan_count'
```

**Rules (do not break these):**
- The proxy credential lives **only** in `/etc/contabo-pricing/proxy.env` (`chmod 600`)
  on the host, or the `SCRAPER_PROXY` GitHub *environment* secret — **never** in git,
  a tracked file, or a committed compose/env. `deploy/proxy.env` is gitignored.
- Use the **public release binary** (`vX.Y.Z` assets), not the GHCR image, unless you
  have made the GHCR package public or are logged in with a `read:packages` PAT.
- Keep the bind at `127.0.0.1:8080` and the data dir at `/var/lib/contabo-pricing/output`
  (the WHMCS module calls the loopback API).
- `SCRAPER_PROXY` may be schemeless on ≥ the normalize fix, but **always supply the
  `http://` scheme** for compatibility with older binaries (e.g. ≤ v2.3.2).

## Quick start (no reverse proxy)

```bash
echo "$(openssl rand -hex 32)" > deploy/auth_token.txt
cd deploy
docker compose up -d
curl -fsS http://localhost:8080/api/v1/health
```

The base compose runs the API on host port 8080 with a generated bearer token mounted as a docker secret. The token is required for `POST /api/v1/refresh`; read endpoints are open.

## With automatic HTTPS via Caddy

```bash
echo "$(openssl rand -hex 32)" > deploy/auth_token.txt
API_DOMAIN=pricing.example.com \
  docker compose -f docker-compose.yml -f docker-compose.caddy.yml up -d
```

Caddy v2 listens on :80 / :443, fetches a Let's Encrypt cert for `${API_DOMAIN}`, and proxies to the API container. HSTS + security headers preconfigured in `caddy/Caddyfile.example`.

## With Traefik

Assumes you already run Traefik with the `traefik` network and a `le` cert resolver:

```bash
API_DOMAIN=pricing.example.com TRAEFIK_NETWORK=traefik \
  docker compose -f docker-compose.yml -f docker-compose.traefik.yml up -d
```

If your Traefik setup uses different names, edit `docker-compose.traefik.yml` accordingly.

## With Coolify

In the Coolify UI, create a new "Docker Compose" application and paste the contents of `docker-compose.yml` + `docker-compose.coolify.yml`. Set environment variables:

| Var | Example | Notes |
|---|---|---|
| `COOLIFY_FQDN` | `pricing.example.com` | Used to route Coolify's built-in proxy |
| `CONTABO_AUTH_TOKEN` | `…` | Optional: inline instead of secret file |
| `CONTABO_REFRESH_CRON` | _(no effect — see note)_ | ⚠️ Parsed but **not wired** to a scheduler; use an external cron / `systemd` timer instead |

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `CONTABO_BIND` | `0.0.0.0:8080` | bind address |
| `CONTABO_DATA_DIR` | `/app/data/output` | snapshot directory (watched for hot-reload) |
| `CONTABO_AUTH_TOKEN_FILE` | `/run/secrets/contabo_auth_token` | bearer token for write endpoints |
| `CONTABO_AUTH_TOKEN` | unset | alternative to the file form |
| `CONTABO_REFRESH_CRON` | unset | ⚠️ accepted but **not currently wired** to an in-app scheduler — periodic refresh must be driven externally (cron / `systemd` timer hitting `POST /api/v1/refresh`) |
| `CONTABO_CORS_ORIGIN` | unset | repeatable CORS allow-origin |
| `CONTABO_SCRAPER_CMD` | unset | override scraper invocation (e.g. `node /app/scripts/contabo_scraper.js`) |
| `RUST_LOG` | `info` | tracing-subscriber filter |

## Verifying a deployment

```bash
# 1. Liveness
curl -fsS https://pricing.example.com/api/v1/health

# 2. Meta — confirm version + snapshot freshness
curl -s https://pricing.example.com/api/v1/meta | jq '.scraper_version, .snapshot_meta.generated_at'

# 3. Trigger a refresh
TOKEN=$(cat deploy/auth_token.txt)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://pricing.example.com/api/v1/refresh

# 4. Watch the job complete (poll every few seconds)
JOB=$(curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  https://pricing.example.com/api/v1/refresh | jq -r .job_id)
curl -s https://pricing.example.com/api/v1/jobs/$JOB | jq

# 5. The report UI (browse from a browser)
open https://pricing.example.com/
```

## Choosing an overlay

| Overlay | Best for | TLS | Extras |
|---|---|---|---|
| (none / base) | dev, behind external LB | no | — |
| Caddy | small production, single host | yes — Let's Encrypt | HTTP/3, security headers |
| Traefik | multi-app reverse proxy already in place | yes — your existing resolver | label-based service discovery |
| Coolify | self-hosted PaaS with UI | yes — Coolify-managed | one-click redeploys, env mgmt UI |

Pick the one that matches your operations model. They're not mutually exclusive — you can ship `docker-compose.yml` + the matching overlay to each environment.
