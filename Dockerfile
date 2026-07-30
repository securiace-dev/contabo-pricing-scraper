# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
#
# Multi-stage build for contabo-pricing.
#
# Final image contains:
#   - the Rust binary (scrape + serve subcommands)
#   - Node 20 + the Node scraper (fallback / parity testing)
#   - CloakBrowser npm wrapper (the licensed browser downloads at first use)
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
FROM rust:1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS rust-builder
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
FROM node:20-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0 AS node-builder
WORKDIR /app
COPY package.json package-lock.json ./
# Install all deps including cloakbrowser + playwright-core.
# Lifecycle scripts are disabled; the separately licensed browser is acquired
# by the unprivileged runtime user only when CloakBrowser mode is first used.
RUN npm ci --ignore-scripts --omit=dev

COPY scripts ./scripts
COPY .github/scripts ./.github/scripts

# ── Stage 3: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tini curl libatomic1 \
      # Chromium runtime libraries required by the optional CloakBrowser mode.
      libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
      libcairo2 libcups2 libdbus-1-3 libdrm2 libegl1 libgbm1 \
      libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 \
      libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxdamage1 \
      libxext6 libxfixes3 libxrandr2 libxshmfence1 \
      # Fonts required for CloakBrowser fingerprint compatibility.
      fonts-liberation fonts-noto-core \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create unprivileged user
RUN groupadd --system --gid 1000 contabo \
 && useradd  --system --uid 1000 --gid contabo --create-home --home-dir /home/contabo --shell /usr/sbin/nologin contabo

WORKDIR /app
COPY --from=rust-builder /src/target/release/contabo-scraper /usr/local/bin/contabo-scraper
COPY --from=node-builder /usr/local/bin/node /usr/local/bin/node
COPY --from=node-builder /app /app
COPY report.html /app/report.html

# Output and browser-cache directories are the only mutable paths. Mount the
# browser cache when CloakBrowser startup downloads must persist across runs.
RUN mkdir -p /app/data/output && chown -R contabo:contabo /app \
 && mkdir -p /home/contabo/.cloakbrowser \
 && chown -R contabo:contabo /home/contabo/.cloakbrowser

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
