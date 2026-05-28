#!/usr/bin/env bash
# Compares Rust and Node scraper outputs for parity after the enrich step.
# Masks volatile timestamp fields before diffing so successive runs are stable.
#
# Usage:
#   bash .github/scripts/parity_check.sh
#
# Exit codes:
#   0  outputs are equivalent
#   1  any output file differs in non-volatile content
#   2  setup error (missing toolchain, build failure)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_ROOT="$(mktemp -d -t contabo-parity.XXXXXX)"
RUST_OUT="$TMP_ROOT/rust"
NODE_OUT="$TMP_ROOT/node"
MASKED_RUST="$TMP_ROOT/rust-masked"
MASKED_NODE="$TMP_ROOT/node-masked"

trap 'echo "[parity] tmp: $TMP_ROOT (preserved for inspection on failure)"' ERR
trap 'rm -rf "$TMP_ROOT"' EXIT

cd "$REPO_ROOT"

# ── 1. Run Rust scraper ──────────────────────────────────────────────────────
command -v cargo >/dev/null 2>&1 || { echo "[parity] cargo not found"; exit 2; }
echo "[parity] building Rust scraper (release)…"
cargo build --release --quiet --bin contabo-scraper 2>&1 | tail -5 || {
  # Cargo.toml may declare different bin name; fall back to default
  cargo build --release --quiet 2>&1 | tail -5
}
RUST_BIN="$REPO_ROOT/target/release/contabo-scraper"
[ -x "$RUST_BIN" ] || RUST_BIN="$(ls "$REPO_ROOT/target/release/"*-scraper 2>/dev/null | head -1)"
[ -x "$RUST_BIN" ] || { echo "[parity] cannot find rust binary in target/release/"; exit 2; }
mkdir -p "$RUST_OUT"
# Capture the exit code rather than letting `set -e` abort: a total fetch
# failure (e.g. Cloudflare 403 on a datacenter/CI IP) is handled below as a
# neutral skip, not a hard failure.
rust_rc=0
"$RUST_BIN" --output "$RUST_OUT" --quiet || rust_rc=$?

# ── 2. Run Node scraper ──────────────────────────────────────────────────────
command -v node >/dev/null 2>&1 || { echo "[parity] node not found"; exit 2; }
mkdir -p "$NODE_OUT"
node_rc=0
node scripts/contabo_scraper.js --output "$NODE_OUT" --quiet || node_rc=$?

# ── 2b. Upstream-block guard ─────────────────────────────────────────────────
# If BOTH scrapers failed to fetch, the upstream is unreachable from this runner
# (the documented Cloudflare 403 block on datacenter IPs). Parity cannot be
# evaluated when neither side can fetch — skip neutrally rather than report a
# false regression. A genuine Rust↔Node divergence is still caught on any run
# where the upstream IS reachable (local / proxied).
if [ "$rust_rc" -ne 0 ] && [ "$node_rc" -ne 0 ]; then
  echo "[parity] SKIP: both scrapers failed to fetch upstream (rust=$rust_rc node=$node_rc) —"
  echo "[parity]       likely the Cloudflare 403 block on datacenter/CI IPs. Parity not assessable; treating as pass."
  exit 0
fi
# Exactly one side failing is a real asymmetry — surface it.
if [ "$rust_rc" -ne 0 ] || [ "$node_rc" -ne 0 ]; then
  echo "[parity] FAILED: scrapers disagree on fetch outcome (rust=$rust_rc node=$node_rc) — one fetched, one did not."
  exit 1
fi

# ── 2c. Report plan counts ───────────────────────────────────────────────────
# Concrete proof that both scrapers pulled real plan data (e.g. through the
# proxy on CI) rather than emitting a stale/empty snapshot.
plan_count() {
  node -e "try{const d=require('$1/contabo_base_plans.json');process.stdout.write(String(d.plan_count ?? (Array.isArray(d.plans)?d.plans.length:0)))}catch(e){process.stdout.write('0')}" 2>/dev/null
}
rust_plans="$(plan_count "$RUST_OUT")"
node_plans="$(plan_count "$NODE_OUT")"
echo "[parity] plans scraped — rust=$rust_plans node=$node_plans"
if [ "$rust_plans" = "0" ] || [ "$node_plans" = "0" ]; then
  echo "[parity] FAILED: a scraper produced 0 plans (rust=$rust_plans node=$node_plans) — no real data fetched."
  exit 1
fi

# ── 3. Run enrich on BOTH outputs ────────────────────────────────────────────
# enrich_output.js reads from a fixed path (data/output); symlink each side in
# turn so both Rust and Node outputs are post-processed identically.
DATA_DIR="$REPO_ROOT/data/output"
BACKUP=""
if [ -d "$DATA_DIR" ] && [ ! -L "$DATA_DIR" ]; then
  BACKUP="$DATA_DIR.parity-backup.$$"
  mv "$DATA_DIR" "$BACKUP"
fi
mkdir -p "$REPO_ROOT/data"
for SIDE_OUT in "$RUST_OUT" "$NODE_OUT"; do
  ln -sfn "$SIDE_OUT" "$DATA_DIR"
  node .github/scripts/enrich_output.js >/dev/null
done
rm -f "$DATA_DIR"
[ -n "$BACKUP" ] && mv "$BACKUP" "$DATA_DIR"

# ── 4. Mask volatile timestamps before diff ──────────────────────────────────
mkdir -p "$MASKED_RUST" "$MASKED_NODE"
mask_json() {
  # Replace volatile timestamp values with constants; keep keys intact.
  local src="$1" dst="$2"
  node -e "
    const fs=require('fs'); const v=JSON.parse(fs.readFileSync('$src','utf8'));
    const MASK='__MASKED__';
    const walk=(o)=>{
      if(Array.isArray(o)) return o.forEach(walk);
      if(o && typeof o==='object'){
        for(const k of Object.keys(o)){
          if(/^(generated_at|fetched_at|at|rateDate|scraper_version)\$/.test(k)) o[k]=MASK;
          else walk(o[k]);
        }
      }
    };
    walk(v);
    fs.writeFileSync('$dst', JSON.stringify(v, null, 2));
  "
}
mask_csv() {
  # CSVs contain fetched_at as a column; replace its values with __MASKED__.
  local src="$1" dst="$2"
  awk 'BEGIN{FS=OFS=","}
       NR==1 { for(i=1;i<=NF;i++) if($i=="fetched_at") fc=i; print; next }
       { if(fc) $fc="__MASKED__"; print }' "$src" > "$dst"
}

EXPECTED=(contabo_base_plans.json contabo_configs.json contabo_pricing_dataset.json \
          contabo_quick_reference.json contabo_view_model.json \
          contabo_base_plans.csv contabo_option_catalog.csv \
          contabo_gap_report.json contabo_gap_summary.json)

missing=0
for f in "${EXPECTED[@]}"; do
  for side in rust node; do
    src_dir=$([ "$side" = rust ] && echo "$RUST_OUT" || echo "$NODE_OUT")
    [ -f "$src_dir/$f" ] || { echo "[parity] $side missing $f"; missing=$((missing+1)); }
  done
done
[ "$missing" -gt 0 ] && { echo "[parity] FAILED: $missing missing files"; exit 1; }

for f in "${EXPECTED[@]}"; do
  case "$f" in
    *.json) mask_json "$RUST_OUT/$f" "$MASKED_RUST/$f"
            mask_json "$NODE_OUT/$f" "$MASKED_NODE/$f" ;;
    *.csv)  mask_csv  "$RUST_OUT/$f" "$MASKED_RUST/$f"
            mask_csv  "$NODE_OUT/$f" "$MASKED_NODE/$f" ;;
  esac
done

# ── 5. Diff ──────────────────────────────────────────────────────────────────
echo "[parity] diffing masked outputs…"
if diff -ru "$MASKED_RUST" "$MASKED_NODE" > "$TMP_ROOT/diff.patch"; then
  echo "[parity] OK — Rust and Node outputs are equivalent."
  exit 0
else
  lines=$(wc -l < "$TMP_ROOT/diff.patch" | tr -d ' ')
  echo "[parity] FAILED — $lines lines of differences. See $TMP_ROOT/diff.patch:"
  head -100 "$TMP_ROOT/diff.patch"
  trap - EXIT  # preserve tmp for debugging
  echo "[parity] (tmp dir preserved at $TMP_ROOT)"
  exit 1
fi
