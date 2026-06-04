#!/usr/bin/env bash
# Teammate onboarding for the shared mempalace + graphify setup.
#
# The repo ships the DATA (graphify-out/graph.json, GRAPH_REPORT.md, memory/,
# mempalace.yaml, entities.json) and the per-project tool config (AGENTS.md,
# GEMINI.md, .cursor/rules, .codex, .gemini, .mcp.json). What each clone still
# needs is the local wiring this script installs:
#   1. the graphify git post-commit hook (auto-rebuild on commit)
#   2. a sanity check that graphify + mempalace are installed
#   3. printed one-liners to register the tools you personally use
#
# Safe to re-run. Does not touch your global tool configs automatically — the
# per-tool registration commands are printed so you choose what to wire.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

say() { printf '\033[1m%s\033[0m\n' "$*"; }

# --- 1. detect a python that can import graphify ---
PY=""
for cand in "$(command -v graphify >/dev/null 2>&1 && head -1 "$(command -v graphify)" | sed 's/^#![[:space:]]*//')" \
            python3 /opt/homebrew/opt/python@3.14/bin/python3.14; do
  [ -n "$cand" ] || continue
  case "$cand" in *[!a-zA-Z0-9/_.\ -]*) continue;; esac
  if "$cand" -c 'import graphify' >/dev/null 2>&1; then PY="$cand"; break; fi
done

if [ -z "$PY" ]; then
  say "graphify not importable. Install it first:"
  echo "    uv tool install graphifyy   # or: pipx install graphifyy"
else
  say "graphify OK ($PY)"
fi

if ! command -v mempalace-mcp >/dev/null 2>&1; then
  say "mempalace not on PATH. Install it first:"
  echo "    uv tool install mempalace   # provides mempalace + mempalace-mcp"
else
  say "mempalace OK ($(command -v mempalace-mcp))"
fi

# --- 2. install the git post-commit hook (portable) ---
HOOK=".git/hooks/post-commit"
cat > "$HOOK" <<'EOF'
#!/bin/sh
# graphify auto-rebuild after each commit (fresh AST + cached semantic; no LLM).
PY=""
for c in "$(command -v graphify >/dev/null 2>&1 && head -1 "$(command -v graphify)" | sed 's/^#![[:space:]]*//')" python3; do
  case "$c" in *[!a-zA-Z0-9/_.-]*) continue;; esac
  [ -n "$c" ] && "$c" -c 'import graphify' >/dev/null 2>&1 && { PY="$c"; break; }
done
[ -n "$PY" ] || exit 0
[ -f graphify-out/graph.json ] || exit 0
"$PY" scripts/graphify-rebuild.py >/dev/null 2>&1 || true
exit 0
EOF
chmod +x "$HOOK"
say "git post-commit hook installed -> $HOOK"

# --- 3. print per-tool registration one-liners ---
cat <<'EOF'

────────────────────────────────────────────────────────────────────
Register the tools YOU use (each is optional, run what applies):

graphify skill (knowledge-graph /graphify command):
  graphify claude install          # Claude Code
  graphify cursor install          # Cursor (.cursor/rules — already in repo)
  graphify codex install           # Codex / any AGENTS.md reader (already in repo)
  graphify gemini install          # Gemini CLI
  graphify install --platform opencode

mempalace MCP server (memory):
  The project .mcp.json already declares it for MCP clients that read it.
  For global use, add an MCP server named "mempalace" with command
  "mempalace-mcp" to your tool (Cursor ~/.cursor/mcp.json, VS Code user
  mcp.json, Zed context_servers, Continue config.yaml, Codex config.toml,
  Gemini settings.json, OpenCode opencode.json).

mempalace auto-save hooks (Claude Code, global ~/.claude/settings.json):
  SessionStart / Stop / PreCompact ->
    mempalace hook run --hook <session-start|stop|precompact> --harness claude-code

Build/refresh the graph anytime:
  /graphify .            (in your assistant — full rebuild incl. concept layer)
────────────────────────────────────────────────────────────────────
EOF
say "Done. The shared graph is already in graphify-out/graph.json — open graph.html or query it."
