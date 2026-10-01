"""Render the feature matrix of docs/comparisons/semantic_layers.md from matrix.yaml and probes.yaml.

The page's prose is hand-written markdown; this rewrites only the block between the matrix markers (scorecard,
legend and table, with links to each row's probes) and the probe link definitions used by the prose. Styles live
in docs/stylesheets/comparisons.css. Run after editing matrix.yaml or probes.yaml; --check only reports.
"""

import argparse
import html
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DOC = ROOT / "docs" / "comparisons" / "semantic_layers.md"
MATRIX = HERE / "matrix.yaml"
PROBES = HERE / "probes.yaml"
PROBES_URL = "https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml"

TOOLS = [("slayer", "SLayer", "s"), ("malloy", "Malloy", "m"), ("cube", "Cube Core", "c"), ("metricflow", "MetricFlow", "f")]
VERDICTS = [("yes", "Yes"), ("partial", "Partial"), ("no", "No"), ("unknown", "Unknown")]
RANK = {"yes": 3, "partial": 2, "no": 1, "unknown": 0}
# Shown only where the stylesheet is absent (e.g. GitHub's file view).
EMOJI = {"yes": "✅", "partial": "🟡", "no": "❌", "unknown": "❔"}
ORIGIN = {"SLayer": "s", "Malloy": "m", "Cube": "c", "MetricFlow": "f", "all": "a"}
MATRIX_MARKERS = ("<!-- matrix:start -->", "<!-- matrix:end -->")
LINK_MARKERS = ("<!-- probe-links:start -->", "<!-- probe-links:end -->")


def inline(text: str) -> str:
    """Inline markdown (`code`, **bold**, *italic*, [text](url)) to HTML."""
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
    return re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", lambda m: f'<a href="{html.escape(m.group(2))}">{m.group(1)}</a>', joined)


def load_probes() -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, int]]:
    """Probe ids per matrix row as (engine label, id), and each id's line in probes.yaml."""
    text = PROBES.read_text()
    lines = {m.group(1): text[:m.start()].count("\n") + 1 for m in re.finditer(r"^  - id: (\S+)$", text, re.M)}
    by_row: Dict[str, List[Tuple[str, str]]] = {}
    labels = {"slayer": "SLayer", "malloy": "Malloy", "cube": "Cube", "metricflow": "MetricFlow"}
    for p in yaml.safe_load(text)["probes"]:
        for key, label in labels.items():
            if key in p:
                by_row.setdefault(p["row"], []).append((label, p["id"]))
    return by_row, lines


def pill(verdict: str) -> str:
    return f'<span class="pill {verdict}"><span class="slm-e">{EMOJI[verdict]} </span>{dict(VERDICTS)[verdict]}</span>'


def tool_cell(cell: Dict[str, Any]) -> str:
    parts = [pill(cell["verdict"])]
    if cell.get("how"):
        parts.append(f'<div class="slm-how">{inline(cell["how"])}</div>')
    if cell.get("text"):
        parts.append(f"<p>{inline(cell['text'])}</p>")
    if cell.get("caveat"):
        parts.append(f'<p class="slm-cav">{inline(cell["caveat"])}</p>')
    return f'<td><div class="slm-cell">{"".join(parts)}</div></td>'


def leaders(row: Dict[str, Any]) -> List[str]:
    best = max(RANK[row[key]["verdict"]] for key, _, _ in TOOLS)
    return [key for key, _, _ in TOOLS if RANK[row[key]["verdict"]] == best]


def comment_cell(row: Dict[str, Any], probes: List[Tuple[str, str]], lines: Dict[str, int]) -> str:
    lead = leaders(row)
    if len(lead) == len(TOOLS):
        pills = '<span class="pill lead-all">All four</span>'
    else:
        pills = "".join(f'<span class="pill lead-{short}">{name}</span>' for key, name, short in TOOLS if key in lead)
    parts = [f'<div class="slm-leads-row">{pills}</div>', f"<p>{inline(row['comment'])}</p>"]
    if probes:
        groups = []
        for label in dict.fromkeys(label for label, _ in probes):
            links = "".join(f'<a href="{PROBES_URL}#L{lines[i]}">{i}</a>' for lab, i in probes if lab == label)
            groups.append(f"<div><b>{label}</b> {links}</div>")
        n = len({i for _, i in probes})
        parts.append(f'<details class="slm-probes"><summary>{n} probe{"s" if n > 1 else ""}</summary>{"".join(groups)}</details>')
    return f'<td><div class="slm-cell">{"".join(parts)}</div></td>'


def scorecard(rows: List[Dict[str, Any]]) -> str:
    out = ['<div class="slm-score">']
    for key, name, short in TOOLS:
        counts = {v: sum(1 for r in rows if r[key]["verdict"] == v) for v, _ in VERDICTS}
        bar = "".join(f'<span class="{v}" style="flex:{c}">{c}</span>' for v, c in counts.items() if c)
        leads = sum(1 for r in rows if key in leaders(r))
        out.append(f'<div class="slm-who" style="color:var(--slm-{short})">{name}</div><div class="slm-bar">{bar}</div>'
                   f'<div class="slm-leads">leads {leads}</div>')
    out.append("</div>")
    legend = ['<div class="slm-legend">',
              f"<div>{pill('yes')}native, in one query or expression</div>",
              f"<div>{pill('partial')}extra steps or stages, a derived source or custom SQL, or material limits</div>",
              f"<div>{pill('no')}not expressible, or no protection</div>",
              f"<div>{pill('unknown')}not documented</div>",
              "<div>Bars count verdicts (yes / partial / no); <i>leads</i> counts rows where the tool has the best "
              "verdict, ties included.</div>",
              "</div>"]
    return '<div class="slm-top">' + "".join(out) + "".join(legend) + "</div>"


def matrix_block(matrix: Dict[str, Any], by_row, lines) -> str:
    rows = matrix["rows"]
    head = ('<thead><tr><th>Capability<small>the intent, and whose feature list it came from</small></th>'
            '<th class="s">SLayer<small>verdict · how · caveats</small></th>'
            '<th class="m">Malloy<small>verdict · how · caveats</small></th>'
            '<th class="c">Cube Core<small>query-only, no model changes</small></th>'
            '<th class="f">MetricFlow<small>open source, query-only</small></th>'
            "<th>Leader &amp; comment<small>best verdict in the row, and what the difference means</small></th></tr></thead>")
    body = []
    for group in matrix["groups"]:
        body.append(f'<tr class="slm-group"><td colspan="6"><b>{inline(group["name"])}</b>'
                    f'<span>{inline(group["note"])}</span></td></tr>')
        for r in (r for r in rows if r["group"] == group["name"]):
            origin = "From all lists" if r["origin"] == "all" else f"From {r['origin']}'s list"
            cap = (f'<td><div class="slm-name"><span class="slm-id">{r["id"]}</span>{inline(r["name"])}</div>'
                   f'<div class="slm-intent">{inline(r["intent"])}</div>'
                   f'<span class="slm-origin {ORIGIN[r["origin"]]}">{origin}</span></td>')
            cells = "".join(tool_cell(r[key]) for key, _, _ in TOOLS)
            body.append(f'<tr id="{r["id"].lower()}">{cap}{cells}{comment_cell(r, by_row.get(r["id"], []), lines)}</tr>')
    cols = '<colgroup><col class="c-cap">' + '<col class="c-tool">' * len(TOOLS) + '<col class="c-e"></colgroup>'
    table = f'<div class="slm-wrap"><table class="slm-table">{cols}{head}<tbody>{"".join(body)}</tbody></table></div>'
    block = f'<div class="slm">{scorecard(rows)}{table}</div>'
    return f"{MATRIX_MARKERS[0]}\n{block}\n{MATRIX_MARKERS[1]}"


def replace_block(doc: str, markers: Tuple[str, str], block: str) -> str:
    start, end = doc.find(markers[0]), doc.find(markers[1])
    if start < 0 or end < start:
        sys.exit(f"missing {markers[0]} ... {markers[1]} in {DOC}")
    return doc[:start] + block + doc[end + len(markers[1]):]


def validate(matrix: Dict[str, Any]) -> None:
    groups = {g["name"] for g in matrix["groups"]}
    ids = [r["id"] for r in matrix["rows"]]
    if len(ids) != len(set(ids)):
        sys.exit("duplicate row ids in matrix.yaml")
    for r in matrix["rows"]:
        if r["group"] not in groups or r["origin"] not in ORIGIN:
            sys.exit(f"{r['id']}: unknown group or origin")
        for key, _, _ in TOOLS:
            if r[key]["verdict"] not in RANK:
                sys.exit(f"{r['id']}.{key}: verdict must be one of {list(RANK)}")


def regenerate(doc: str) -> str:
    matrix = yaml.safe_load(MATRIX.read_text())
    validate(matrix)
    by_row, lines = load_probes()
    doc = replace_block(doc, MATRIX_MARKERS, matrix_block(matrix, by_row, lines))
    refs = {m.group(1) for m in re.finditer(r"\[([^\]\[]+)\]\[\]", doc)}
    unknown = sorted(refs - set(lines))
    if unknown:
        sys.exit(f"probe links with no such probe id: {unknown}")
    defs = [f"[{i}]: {PROBES_URL}#L{lines[i]}" for i in sorted(refs, key=lines.__getitem__)]
    return replace_block(doc, LINK_MARKERS, "\n".join([LINK_MARKERS[0], *defs, LINK_MARKERS[1]]))


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
