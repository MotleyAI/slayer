"""Export the feature matrix (matrix.yaml) with each row's probes (probes.yaml) to matrix.json.

matrix.json feeds the comparison page on motley.ai (motley-website: /semantic-layer-comparison). Cell text is
rendered from inline markdown to HTML here, so the site only lays it out. Run after editing matrix.yaml or
probes.yaml; --check only reports whether matrix.json is stale.
"""

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
MATRIX = HERE / "matrix.yaml"
PROBES = HERE / "probes.yaml"
OUT = HERE / "matrix.json"
PROBES_URL = "https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml"

TOOLS = [("slayer", "SLayer"), ("malloy", "Malloy"), ("cube", "Cube Core"), ("metricflow", "MetricFlow")]
ENGINES = {"slayer": "SLayer", "malloy": "Malloy", "cube": "Cube", "metricflow": "MetricFlow"}
RANK = {"yes": 3, "partial": 2, "no": 1, "unknown": 0}
ORIGINS = {"SLayer", "Malloy", "Cube", "MetricFlow", "all"}
SAFE_URL = re.compile(r"(https?://|/|#)")


def _link(m: re.Match) -> str:
    url = html.unescape(m.group(2))
    if not SAFE_URL.match(url):
        return m.group(0)
    return f'<a href="{html.escape(url)}">{m.group(1)}</a>'


def inline(text: str) -> str:
    """Inline markdown (`code`, **bold**, *italic*, [text](url)) to HTML; only http(s) and site-relative links."""
    out = []
    for part in re.split(r"(`[^`]*`)", text):
        if part.startswith("`") and part.endswith("`") and len(part) > 1:
            out.append(f"<code>{html.escape(part[1:-1], quote=False)}</code>")
            continue
        p = html.escape(part, quote=False)
        p = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", p)
        p = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", p)
        out.append(p)
    joined = "".join(out)
    return re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", _link, joined)


def load_probes(row_ids: Set[str]) -> Dict[str, List[Dict[str, str]]]:
    """Each matrix row's probes as {engine, id, url}, linking the probe's line in probes.yaml."""
    text = PROBES.read_text()
    lines = {m.group(1): text[:m.start()].count("\n") + 1 for m in re.finditer(r"^ {2}- id: (\S+)$", text, re.M)}
    by_row: Dict[str, List[Dict[str, str]]] = {}
    for p in yaml.safe_load(text)["probes"]:
        if p["row"] not in row_ids:
            sys.exit(f"probe {p['id']}: row {p['row']} is not in matrix.yaml")
        for key, engine in ENGINES.items():
            if key in p:
                by_row.setdefault(p["row"], []).append(
                    {"engine": engine, "id": p["id"], "url": f"{PROBES_URL}#L{lines[p['id']]}"})
    return by_row


def cell(c: Dict[str, Any]) -> Dict[str, str]:
    out = {"verdict": c["verdict"]}
    for key in ("how", "text", "caveat"):
        if c.get(key):
            out[key] = inline(c[key])
    return out


def validate(matrix: Dict[str, Any]) -> None:
    groups = {g["name"] for g in matrix["groups"]}
    ids = [r["id"] for r in matrix["rows"]]
    if len(ids) != len(set(ids)):
        sys.exit("duplicate row ids in matrix.yaml")
    for r in matrix["rows"]:
        if r["group"] not in groups or r["origin"] not in ORIGINS:
            sys.exit(f"{r['id']}: unknown group or origin")
        for key, _ in TOOLS:
            if r[key]["verdict"] not in RANK:
                sys.exit(f"{r['id']}.{key}: verdict must be one of {list(RANK)}")


def export() -> str:
    matrix = yaml.safe_load(MATRIX.read_text())
    validate(matrix)
    by_row = load_probes({r["id"] for r in matrix["rows"]})
    rows = []
    for r in matrix["rows"]:
        best = max(RANK[r[key]["verdict"]] for key, _ in TOOLS)
        rows.append({
            "id": r["id"],
            "group": r["group"],
            "name": inline(r["name"]),
            "intent": inline(r["intent"]),
            "origin": r["origin"],
            "cells": {key: cell(r[key]) for key, _ in TOOLS},
            "leaders": [key for key, _ in TOOLS if RANK[r[key]["verdict"]] == best],
            "comment": inline(r["comment"]),
            "probes": by_row.get(r["id"], []),
        })
    data = {
        "source": "https://github.com/MotleyAI/slayer/tree/main/examples/comparisons",
        "tools": [{"key": key, "label": label} for key, label in TOOLS],
        "groups": matrix["groups"],
        "rows": rows,
    }
    return json.dumps(data, indent=1, ensure_ascii=False) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="exit 1 if matrix.json is out of date, without writing")
    args = ap.parse_args()
    updated = export()
    if OUT.exists() and OUT.read_text() == updated:
        return 0
    if args.check:
        print(f"{OUT.relative_to(ROOT)} is out of date; run python {Path(__file__).relative_to(ROOT)}")
        return 1
    OUT.write_text(updated)
    print(f"updated {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
