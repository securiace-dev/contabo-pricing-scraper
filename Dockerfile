# syntax=docker/dockerfile:1.7
#
# Multi-stage build for contabo-pricing.
#
# Final image contains:
#   - the Rust binary (scrape + serve subcommands)
#   - Node 20 + the Node scraper (fallback / parity testing)
#   - CloakBrowser npm wrapper + pre-downloaded patched Chromium (~200MB)
#   - the enrich and HTML generator scripts
#   - tini as PID 1 for clean signal handling
#
# Build:   docker build -t contabo-pricing:dev .
# Run:     docker run --rm -p 8080:8080 contabo-pricing:dev
#
# CloakBrowser fetch modes (FETCH_MODE env / --fetch-mode flag):
#   reqwest  — default; fast header-level evasion
#   auto     — reqwest first, CloakBrowser fallback for blocked URLs
#   cloak    — CloakBrowser only (strongest evasion)

# ── Stage 1: cargo build ─────────────────────────────────────────────────────
FROM rust:1-bookworm AS rust-builder
WORKDIR /src

# Cache deps independently of source for fast rebuilds
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/api && echo "fn main() {}" > src/main.rs \
    && touch src/api/mod.rs && cargo build --release && rm -rf src

COPY src ./src
COPY report.html ./report.html
# `touch` busts cargo's mtime fingerprint so the real source recompiles instead
# of reusing the dummy `fn main(){}` artifact from the dep-cache layer above.
# Without this the final image ships a ~330KB stub that exits immediately.
RUN touch src/main.rs src/api/mod.rs && cargo build --release --locked

# ── Stage 2: node scraper bundle + CloakBrowser ──────────────────────────────
FROM node:20-bookworm-slim AS node-builder
WORKDIR /app
COPY package.json ./
# Install all deps including cloakbrowser + playwright-core.
# package-lock.json is not committed (cloakbrowser binary is proprietary/large),
# so we always use npm install here.
RUN npm install --omit=dev

# Pre-download the CloakBrowser Chromium binary (~200MB) so containers start
# instantly without a runtime download. CloakBrowser stores its binary in
# ~/.cloakbrowser/ and auto-downloads on first launch() — no separate install()
# function exists in the JS SDK, so we trigger download via a real launch call.
RUN node --input-type=module - <<'EOF' || true
import { launch } from '/app/node_modules/cloakbrowser/dist/index.js';
const b = await launch({ headless: true });
await b.close();
EOF

COPY scripts ./scripts
COPY .github/scripts ./.github/scripts

# ── Stage 3: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tini curl \
      # Fonts required for CloakBrowser Kasada/Akamai fingerprint compatibility
      fonts-liberation fonts-noto-core \
 && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create unprivileged user
RUN groupadd --system --gid 1000 contabo \
 && useradd  --system --uid 1000 --gid contabo --create-home --home-dir /home/contabo --shell /usr/sbin/nologin contabo

WORKDIR /app
COPY --from=rust-builder /src/target/release/contabo-scraper /usr/local/bin/contabo-scraper
COPY --from=node-builder /app /app
# Copy the pre-downloaded CloakBrowser Chromium binary from the builder stage.
# The binary lives in ~/.cloakbrowser/ (not ~/.cache/ms-playwright).
COPY --from=node-builder /root/.cloakbrowser /home/contabo/.cloakbrowser
COPY report.html /app/report.html

# Output dir is the only mutable path; mount a volume here in production
RUN mkdir -p /app/data/output && chown -R contabo:contabo /app \
 && chown -R contabo:contabo /home/contabo/.cloakbrowser 2>/dev/null || true

USER contabo
EXPOSE 8080

ENV CONTABO_BIND=0.0.0.0:8080 \
    CONTABO_DATA_DIR=/app/data/output \
    FETCH_MODE=reqwest \
    CLOAK_SCRIPT=/app/scripts/cloak-fetch.mjs \
    RUST_LOG=info

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -fsS http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/contabo-scraper"]
CMD ["serve"]
