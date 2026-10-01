"""Regenerate the derived parts of docs/comparisons/semantic_layers.md from its row sections and probes.yaml.

Rewrites the at-a-glance tables and scorecard, each row's "**Probes:**" line, and the probe link definitions
(GitHub line anchors into probes.yaml). Run after editing a row's verdicts or the probes; --check only reports.
"""

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DOC = ROOT / "docs" / "comparisons" / "semantic_layers.md"
PROBES = HERE / "probes.yaml"
PROBES_URL = "https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml"

TOOLS = ["SLayer", "Malloy", "Cube Core", "MetricFlow"]
ENGINES = [("slayer", "SLayer"), ("malloy", "Malloy"), ("cube", "Cube"), ("metricflow", "MetricFlow")]
VERDICTS = ["✅", "🟡", "❌", "❔"]
RANK = {"✅": 3, "🟡": 2, "❌": 1, "❔": 0}
GLANCE = ("<!-- glance:start -->", "<!-- glance:end -->")
LINKS = ("<!-- probe-links:start -->", "<!-- probe-links:end -->")
HEADING = re.compile(r"^### ([QC]\d+) (.+)$")
BULLET = re.compile(r"^- \*\*(" + "|".join(TOOLS) + r")\*\* (" + "|".join(VERDICTS) + ")")


def slug(text: str) -> str:
    """Python-Markdown's default heading id."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def load_probes() -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, int]]:
    """Probe ids per row as (engine label, id), and each id's line in probes.yaml."""
    text = PROBES.read_text()
    lines = {m.group(1): text[:m.start()].count("\n") + 1 for m in re.finditer(r"^  - id: (\S+)$", text, re.M)}
    by_row: Dict[str, List[Tuple[str, str]]] = {}
    for p in yaml.safe_load(text)["probes"]:
        for key, label in ENGINES:
            if key in p:
                by_row.setdefault(p["row"], []).append((label, p["id"]))
    return by_row, lines


def parse_rows(doc: str) -> List[Tuple[str, str, str, Dict[str, str]]]:
    """(group, row id, name, verdict per tool) for every row section, in order."""
    rows, group, current = [], "", None
    for line in doc.splitlines():
        if line.startswith("## "):
            group = line[3:].strip()
        m = HEADING.match(line)
        if m:
            current = (group, m.group(1), m.group(2).strip(), {})
            rows.append(current)
            continue
        b = BULLET.match(line)
        if b and current:
            current[3][b.group(1)] = b.group(2)
    for _, rid, _, verdicts in rows:
        missing = [t for t in TOOLS if t not in verdicts]
        if missing:
            sys.exit(f"{rid}: no verdict bullet for {missing}")
    return rows


def glance_block(rows) -> str:
    out = [GLANCE[0], "", "| | ✅ Yes | 🟡 Partial | ❌ No | ❔ Unknown | Leads a row |", "|---|---|---|---|---|---|"]
    for tool in TOOLS:
        counts = [sum(1 for r in rows if r[3][tool] == v) for v in VERDICTS]
        leads = sum(1 for r in rows if RANK[r[3][tool]] == max(RANK[v] for v in r[3].values()))
        out.append(f"| **{tool}** | " + " | ".join(str(c) for c in counts) + f" | {leads} |")
    out.append("")
    out.append("*Leads a row* counts the rows where the tool has the best verdict, ties included.")
    for group in dict.fromkeys(r[0] for r in rows):
        out += ["", f"**{group}**", "", "| | Capability | " + " | ".join(TOOLS) + " |", "|---|---|" + "---|" * len(TOOLS)]
        for g, rid, name, v in rows:
            if g == group:
                out.append(f"| {rid} | [{name}](#{slug(f'{rid} {name}')}) | " + " | ".join(v[t] for t in TOOLS) + " |")
    out += ["", GLANCE[1]]
    return "\n".join(out)


def probes_line(rid: str, by_row) -> str:
    entries = by_row.get(rid, [])
    if not entries:
        return "**Probes:** none; graded from documentation."
    parts = []
    for _, label in ENGINES:
        ids = [i for lab, i in entries if lab == label]
        if ids:
            parts.append(f"{label} " + " ".join(f"[{i}][]" for i in ids))
    return "**Probes:** " + " · ".join(parts)


def regenerate(doc: str) -> str:
    by_row, lines = load_probes()
    rows = parse_rows(doc)
    doc = replace_block(doc, GLANCE, glance_block(rows))
    out, rid = [], None
    for line in doc.splitlines():
        m = HEADING.match(line)
        if m:
            rid = m.group(1)
        if line.startswith("**Probes:**") and rid:
            line = probes_line(rid, by_row)
        out.append(line)
    doc = "\n".join(out) + "\n"
    used = sorted({m.group(1) for m in re.finditer(r"\[([^\]]+)\]\[\]", doc)} & set(lines), key=lambda i: lines[i])
    unknown = sorted({m.group(1) for m in re.finditer(r"\[([^\]\[]+)\]\[\]", doc)} - set(lines))
    if unknown:
        sys.exit(f"probe links with no such probe id: {unknown}")
    defs = "\n".join([LINKS[0]] + [f"[{i}]: {PROBES_URL}#L{lines[i]}" for i in used] + [LINKS[1]])
    return replace_block(doc, LINKS, defs)


def replace_block(doc: str, markers: Tuple[str, str], block: str) -> str:
    start, end = doc.find(markers[0]), doc.find(markers[1])
    if start < 0 or end < start:
        sys.exit(f"missing {markers[0]} ... {markers[1]} in {DOC}")
    return doc[:start] + block + doc[end + len(markers[1]):]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="exit 1 if the page is out of date, without writing")
    args = ap.parse_args()
    current = DOC.read_text()
    updated = regenerate(current)
    if updated == current:
        return 0
    if args.check:
        print(f"{DOC.relative_to(ROOT)} is out of date; run python {Path(__file__).relative_to(ROOT)}")
        return 1
    DOC.write_text(updated)
    print(f"updated {DOC.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
