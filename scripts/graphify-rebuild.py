#!/usr/bin/env python3
"""Rebuild the graphify knowledge graph WITHOUT calling an LLM.

Unlike graphify's native `_rebuild_code` (which is AST-only and drops the
document/concept layer), this merges a fresh AST extraction of the code with
the *cached* semantic extraction of docs/papers. The semantic doc nodes were
produced once by `/graphify` and live in graphify-out/cache, so re-using them
is free. Result: code structure stays current on every commit/turn while the
conceptual nodes (Profile/Mapping, fallback rule, etc.) survive.

Community labels degrade to generic "Community N" on these rebuilds; run
`/graphify` or `/graphify --update` to refresh the human-readable labels and
the doc layer itself when docs change.

No-ops (exit 0) if graphify isn't importable or no graph exists yet, so it is
safe to wire into a global Stop hook that fires in every project.
"""
import sys
from pathlib import Path

ROOT = Path(".")
OUT = ROOT / "graphify-out"

# Only act where a graph already exists — keeps a global hook inert elsewhere.
if not (OUT / "graph.json").exists():
    sys.exit(0)

try:
    from graphify.detect import detect
    from graphify.extract import collect_files, extract
    from graphify.cache import check_semantic_cache
    from graphify.build import build_from_json
    from graphify.cluster import cluster, score_all
    from graphify.analyze import god_nodes, surprising_connections, suggest_questions
    from graphify.report import generate
    from graphify.export import to_json, to_html
except Exception:
    # graphify not installed for this interpreter — nothing to do.
    sys.exit(0)

det = detect(ROOT)

# Fresh AST over current code files.
code_files = []
for f in det.get("files", {}).get("code", []):
    p = Path(f)
    code_files.extend(collect_files(p) if p.is_dir() else [p])
ast = extract(code_files) if code_files else {"nodes": [], "edges": [], "input_tokens": 0, "output_tokens": 0}

# Cached semantic extraction (includes the doc/concept nodes from the last /graphify run).
all_files = [f for files in det.get("files", {}).values() for f in files]
cached_nodes, cached_edges, cached_hyper, _uncached = check_semantic_cache(all_files)

# Merge: AST nodes win on id collisions (code), cached semantic adds the doc layer.
seen = {n["id"] for n in ast["nodes"]}
nodes = list(ast["nodes"])
for n in cached_nodes:
    if n["id"] not in seen:
        nodes.append(n)
        seen.add(n["id"])
merged = {
    "nodes": nodes,
    "edges": ast["edges"] + cached_edges,
    "hyperedges": cached_hyper,
    "input_tokens": 0,
    "output_tokens": 0,
}

G = build_from_json(merged)
if G.number_of_nodes() == 0:
    sys.exit(0)

communities = cluster(G)
cohesion = score_all(G, communities)
labels = {cid: "Community " + str(cid) for cid in communities}
gods = god_nodes(G)
surprises = surprising_connections(G, communities)
questions = suggest_questions(G, communities, labels)

report = generate(G, communities, cohesion, labels, gods, surprises, det,
                  {"input": 0, "output": 0}, ".", suggested_questions=questions)
(OUT / "GRAPH_REPORT.md").write_text(report)
to_json(G, communities, str(OUT / "graph.json"))
if G.number_of_nodes() <= 5000:
    to_html(G, communities, str(OUT / "graph.html"), community_labels=labels)

print(f"graphify-rebuild: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges, "
      f"{len(communities)} communities (fresh AST + cached semantic)")
