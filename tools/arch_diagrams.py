"""Generate mermaid diagrams in arc42 docs from the LikeC4 model (see system.arc42.md §5).

The single parser of the constrained `.c4` authoring convention: `arch_check` imports
this module. `poetry run python tools/arch_diagrams.py` rewrites the mapped docs in place.
"""

import re
import sys
from pathlib import Path

import yaml
from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parent.parent
FIX_CMD = "poetry run python tools/arch_diagrams.py"


class Element(BaseModel):
    id: str
    kind: str
    title: str
    parent: str | None = None
    virtual: bool = False


class Relation(BaseModel):
    src: str
    dst: str
    legacy: bool = False


class ModelParse(BaseModel):
    elements: list[Element]
    relations: list[Relation]
    findings: list[str]


class Edge(BaseModel):
    src: str
    dst: str
    legacy: bool = False


class View(BaseModel):
    id: str
    title: str
    node_ids: list[str]
    edges: list[Edge]


class ViewsParse(BaseModel):
    views: list[View]
    findings: list[str]


_ELEMENT_RE = re.compile(r"^(\w+)\s*=\s*(\w+)\s+'([^']*)'\s*(.*)$")
_RELATION_RE = re.compile(r"^(\w+)\s*->\s*(\w+)(\s+#legacy)?\s*$")
_SPEC_ELEMENT_RE = re.compile(r"^element\s+(\w+)\b")
_TAG_DECL_RE = re.compile(r"^tag\s+\w+$")
_BLOCK_RE = re.compile(r"^(specification|model|views)\b")
_BRACES_ONLY_RE = re.compile(r"^[{}]+$")
_TOKEN_RE = re.compile(r"'[^']*'|->|[{}(),*]|[A-Za-z0-9_]+|\S")
_MARKER_RE = re.compile(r"<!--\s+(/?)likec4:([A-Za-z0-9_]+)\s+-->")


def _strip_line_comment(line: str) -> str:
    """Drop a trailing `//` comment, leaving single-quoted spans untouched."""
    out: list[str] = []
    in_q = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'":
            in_q = not in_q
            out.append(ch)
        elif not in_q and line[i : i + 2] == "//":
            break
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _brace_delta(code: str) -> int:
    depth = 0
    in_q = False
    for ch in code:
        if ch == "'":
            in_q = not in_q
        elif not in_q and ch == "{":
            depth += 1
        elif not in_q and ch == "}":
            depth -= 1
    return depth


def _c4_logical_lines(text: str) -> list[str]:
    """Comment-stripped lines with every unquoted `{`/`}` broken onto its own line."""
    decommented = "\n".join(_strip_line_comment(line) for line in text.splitlines())
    out: list[str] = []
    in_q = False
    for ch in decommented:
        if ch == "'":
            in_q = not in_q
            out.append(ch)
        elif not in_q and ch == "{":
            out.append("{\n")
        elif not in_q and ch == "}":
            out.append("\n}\n")
        else:
            out.append(ch)
    return [line.strip() for line in "".join(out).splitlines() if line.strip()]


def _model_files(root: Path) -> list[Path]:
    return sorted((root / "architecture" / "model").glob("*.c4"))


def parse_model(root: Path) -> ModelParse:
    """Parse `architecture/model/*.c4` under the constrained §5 authoring convention."""
    kinds: dict[str, bool] = {}
    elements: list[Element] = []
    seen_ids: set[str] = set()
    relations: list[Relation] = []
    rel_pairs: set[tuple[str, str]] = set()
    findings: list[str] = []
    for path in _model_files(root):
        _scan_model_file(path, kinds, elements, seen_ids, relations, rel_pairs, findings)
    for element in elements:
        if element.kind not in kinds:
            findings.append(f"element {element.id} has undeclared kind {element.kind}")
        element.virtual = kinds.get(element.kind, False)
    for relation in relations:
        for endpoint in (relation.src, relation.dst):
            if endpoint not in seen_ids:
                findings.append(f"relation {relation.src} -> {relation.dst} has unknown endpoint {endpoint}")
    return ModelParse(elements=elements, relations=relations, findings=findings)


def _scan_model_file(
    path: Path,
    kinds: dict[str, bool],
    elements: list[Element],
    seen_ids: set[str],
    relations: list[Relation],
    rel_pairs: set[tuple[str, str]],
    findings: list[str],
) -> None:
    region: str | None = None
    depth = 0
    parents: list[str] = []
    spec_kind: str | None = None
    for code in _c4_logical_lines(path.read_text(encoding="utf-8")):
        if region is None:
            m = _BLOCK_RE.match(code)
            if m:
                region = m.group(1) if m.group(1) in ("specification", "model") else "other"
                depth = _brace_delta(code)
                if depth <= 0:
                    region = None
            continue
        delta = _brace_delta(code)
        if region == "specification":
            spec_kind = _scan_spec_line(code, kinds, spec_kind, delta, findings)
        elif region == "model":
            _scan_model_line(code, elements, seen_ids, relations, rel_pairs, parents, delta, findings)
        if delta < 0:
            for _ in range(-delta):
                if parents:
                    parents.pop()
        depth += delta
        if depth <= 0:
            region = None
            parents.clear()
            spec_kind = None
        elif region == "specification" and depth == 1:
            spec_kind = None


def _scan_spec_line(
    code: str, kinds: dict[str, bool], spec_kind: str | None, delta: int, findings: list[str]
) -> str | None:
    m = _SPEC_ELEMENT_RE.match(code)
    if m:
        kinds.setdefault(m.group(1), False)
        return m.group(1) if delta > 0 else spec_kind
    if _TAG_DECL_RE.match(code) or _BRACES_ONLY_RE.match(code):
        return spec_kind
    if code.startswith("#"):
        if spec_kind is not None and code.lstrip("#").strip() == "virtual":
            kinds[spec_kind] = True
        return spec_kind
    findings.append(f"unrecognized specification line: {code}")
    return spec_kind


def _scan_model_line(
    code: str,
    elements: list[Element],
    seen_ids: set[str],
    relations: list[Relation],
    rel_pairs: set[tuple[str, str]],
    parents: list[str],
    delta: int,
    findings: list[str],
) -> None:
    m = _ELEMENT_RE.match(code)
    if m:
        eid, kind, title = m.group(1), m.group(2), m.group(3)
        if eid in seen_ids:
            findings.append(f"duplicate element {eid}")
        else:
            seen_ids.add(eid)
            elements.append(Element(id=eid, kind=kind, title=title, parent=parents[-1] if parents else None))
        if delta > 0:
            parents.append(eid)
        return
    rel = _RELATION_RE.match(code)
    if rel:
        src, dst = rel.group(1), rel.group(2)
        legacy = rel.group(3) is not None
        if parents:
            findings.append(f"relation {src} -> {dst} inside element body {parents[-1]}")
        elif (src, dst) in rel_pairs:
            findings.append(f"duplicate relation {src} -> {dst}")
        else:
            rel_pairs.add((src, dst))
            relations.append(Relation(src=src, dst=dst, legacy=legacy))
        return
    if _BRACES_ONLY_RE.match(code):
        return
    findings.append(f"unrecognized model line: {code}")


def _tokenize(text: str) -> list[tuple[str, str]]:
    stripped = "\n".join(_strip_line_comment(line) for line in text.splitlines())
    tokens: list[tuple[str, str]] = []
    for m in _TOKEN_RE.finditer(stripped):
        tok = m.group(0)
        if tok.startswith("'"):
            tokens.append(("str", tok[1:-1]))
        elif tok == "->":
            tokens.append(("arrow", tok))
        elif tok == "*":
            tokens.append(("star", tok))
        elif tok in "{}(),":
            tokens.append((tok, tok))
        elif tok[0].isalnum() or tok[0] == "_":
            tokens.append(("word", tok))
        else:
            tokens.append(("unknown", tok))
    return tokens


def parse_views(root: Path, model: ModelParse) -> ViewsParse:
    """Parse `architecture/views.c4` under the constrained include grammar (§5)."""
    path = root / "architecture" / "views.c4"
    if not path.exists():
        return ViewsParse(views=[], findings=[])
    tokens = _tokenize(path.read_text(encoding="utf-8"))
    top_level = [e.id for e in model.elements if e.parent is None]
    top_set = set(top_level)
    known = {e.id for e in model.elements}
    findings: list[str] = []
    views: list[View] = []
    seen_ids: set[str] = set()
    pos = 0

    def cur() -> tuple[str, str]:
        return tokens[pos] if pos < len(tokens) else ("eof", "")

    def advance() -> tuple[str, str]:
        nonlocal pos
        tok = cur()
        pos += 1
        return tok

    def anchor_ok(name: str, form: str) -> bool:
        if name not in known:
            findings.append(f"view include {form} references unknown id {name}")
            return False
        if name not in top_set:
            findings.append(f"view include {form} references non-top-level id {name}")
            return False
        return True

    def parse_body(vid: str) -> View:
        title = ""
        base: list[str] = []
        base_seen: set[str] = set()
        src_anchors: set[str] = set()
        dst_anchors: set[str] = set()

        def add_base(name: str) -> None:
            if name not in base_seen:
                base_seen.add(name)
                base.append(name)

        def read_spec() -> None:
            tok = advance()
            if tok[0] == "star":
                if cur()[0] == "arrow":
                    advance()
                    nxt = advance()
                    if nxt[0] == "star":
                        findings.append(f"view {vid}: '* -> *' is not supported")
                    elif nxt[0] == "word":
                        if anchor_ok(nxt[1], f"* -> {nxt[1]}"):
                            dst_anchors.add(nxt[1])
                    else:
                        findings.append(f"view {vid}: malformed predicate")
                else:
                    for eid in top_level:
                        add_base(eid)
            elif tok[0] == "word":
                if cur()[0] == "arrow":
                    advance()
                    nxt = advance()
                    if nxt[0] == "star":
                        if anchor_ok(tok[1], f"{tok[1]} -> *"):
                            src_anchors.add(tok[1])
                    elif nxt[0] == "word":
                        findings.append(f"view {vid}: unsupported predicate {tok[1]} -> {nxt[1]}")
                    else:
                        findings.append(f"view {vid}: malformed predicate")
                elif tok[1] not in known:
                    findings.append(f"view {vid} includes unknown id {tok[1]}")
                elif tok[1] not in top_set:
                    findings.append(f"view {vid} includes non-top-level id {tok[1]}")
                else:
                    add_base(tok[1])
            else:
                findings.append(f"view {vid}: unexpected include token {tok[1]!r}")

        while cur()[0] not in ("}", "eof"):
            tok = advance()
            if tok == ("word", "title"):
                st = advance()
                title = st[1] if st[0] == "str" else title
                if st[0] != "str":
                    findings.append(f"view {vid} title is not a string")
            elif tok == ("word", "include"):
                read_spec()
                while cur()[0] == ",":
                    advance()
                    read_spec()
            else:
                findings.append(f"view {vid} has unrecognized directive {tok[1]!r}")
        if cur()[0] == "}":
            advance()
        return _build_view(vid, title, base, src_anchors, dst_anchors, model)

    if advance() != ("word", "views") or advance()[0] != "{":
        findings.append("views.c4 does not open with a `views {` block")
        return ViewsParse(views=views, findings=findings)
    while cur()[0] not in ("}", "eof"):
        if advance() != ("word", "view"):
            findings.append("expected `view` in views block")
            continue
        idt = advance()
        if idt[0] != "word" or advance()[0] != "{":
            findings.append("malformed view declaration")
            continue
        view = parse_body(idt[1])
        if idt[1] in seen_ids:
            findings.append(f"duplicate view id {idt[1]}")
        seen_ids.add(idt[1])
        views.append(view)
    return ViewsParse(views=views, findings=findings)


def _build_view(
    vid: str,
    title: str,
    base: list[str],
    src_anchors: set[str],
    dst_anchors: set[str],
    model: ModelParse,
) -> View:
    among = set(base)
    predicate_edges = [
        r for r in model.relations if r.src in src_anchors or r.dst in dst_anchors
    ]
    edges = [
        Edge(src=r.src, dst=r.dst, legacy=r.legacy)
        for r in model.relations
        if (r.src in among and r.dst in among) or r.src in src_anchors or r.dst in dst_anchors
    ]
    node_ids: list[str] = []
    seen: set[str] = set()
    for name in base:
        if name not in seen:
            seen.add(name)
            node_ids.append(name)
    for r in predicate_edges:
        for endpoint in (r.src, r.dst):
            if endpoint not in seen:
                seen.add(endpoint)
                node_ids.append(endpoint)
    return View(id=vid, title=title, node_ids=node_ids, edges=edges)


def render_mermaid(view: View, model: ModelParse) -> str:
    """Deterministic classic-syntax mermaid for one view; legend iff a legacy edge is present."""
    by_id = {e.id: e for e in model.elements}
    lines = ["```mermaid", "flowchart TD", f"  %% {view.id}: {view.title}"]
    for nid in view.node_ids:
        element = by_id.get(nid)
        title = (element.title if element else nid).replace('"', "#quot;")
        if element is not None and element.virtual:
            lines.append(f'  {nid}("{title}")')
        else:
            lines.append(f'  {nid}["{title}"]')
    for edge in view.edges:
        lines.append(f"  {edge.src} {'-.->' if edge.legacy else '-->'} {edge.dst}")
    lines.append("```")
    if any(edge.legacy for edge in view.edges):
        lines.append("*Dashed arrows: legacy edges slated to die.*")
    return "\n".join(lines)


def _open_marker(vid: str) -> str:
    return f"<!-- likec4:{vid} -->"


def _close_marker(vid: str) -> str:
    return f"<!-- /likec4:{vid} -->"


def _canonical_block(view: View, model: ModelParse) -> str:
    return f"{_open_marker(view.id)}\n{render_mermaid(view, model)}\n{_close_marker(view.id)}"


def _marker_span(text: str, vid: str) -> tuple[tuple[int, int] | None, str | None]:
    """(span, error): the inclusive open→close span, or None + one of missing/duplicate/order."""
    open_tok, close_tok = _open_marker(vid), _close_marker(vid)
    n_open, n_close = text.count(open_tok), text.count(close_tok)
    if n_open == 0 or n_close == 0:
        return None, "missing"
    if n_open > 1 or n_close > 1:
        return None, "duplicate"
    oi, ci = text.index(open_tok), text.index(close_tok)
    if ci < oi:
        return None, "order"
    return (oi, ci + len(close_tok)), None


def _diagrams_map(root: Path) -> object:
    index = yaml.safe_load((root / "architecture" / "index.yaml").read_text(encoding="utf-8"))
    return index.get("diagrams")


def generate(root: Path) -> list[str]:
    """Rewrite each mapped doc's marker blocks from the model; return changed repo-relative paths."""
    diagrams = _diagrams_map(root)
    if not isinstance(diagrams, dict):
        raise ValueError("diagrams block in index.yaml must be a mapping of doc -> view-id list")
    model = parse_model(root)
    views = parse_views(root, model)
    by_id = {v.id: v for v in views.views}
    changed: list[str] = []
    for doc_key, vids in diagrams.items():
        doc_path = root / doc_key
        text = doc_path.read_text(encoding="utf-8")
        new_text = text
        for vid in vids:
            view = by_id.get(vid)
            if view is None:
                raise ValueError(f"view {vid} mapped to {doc_key} is not defined in views.c4")
            span, error = _marker_span(new_text, vid)
            if error is not None or span is None:
                raise ValueError(f"{doc_key}: {error} marker(s) for view {vid}")
            start, end = span
            new_text = new_text[:start] + _canonical_block(view, model) + new_text[end:]
        if new_text != text:
            doc_path.write_text(new_text, encoding="utf-8", newline="")
            changed.append(doc_key)
    return changed


def _validate_entry(doc_key: object, vids: object) -> tuple[list[str], list[str] | None]:
    if not (isinstance(doc_key, str) and doc_key.startswith("architecture/") and doc_key.endswith(".arc42.md")):
        return [f"diagrams-fresh: diagrams key {doc_key} must be an architecture/*.arc42.md path"], None
    if not isinstance(vids, list):
        return [f"diagrams-fresh: diagrams[{doc_key}] must be a list of view ids; run {FIX_CMD}"], None
    if not vids:
        return [f"diagrams-fresh: diagrams[{doc_key}] is empty"], None
    findings: list[str] = []
    seen: set[str] = set()
    valid: list[str] = []
    ok = True
    for vid in vids:
        if not isinstance(vid, str) or not vid:
            findings.append(f"diagrams-fresh: diagrams[{doc_key}] has an invalid view id {vid!r}")
            ok = False
        elif vid in seen:
            findings.append(f"diagrams-fresh: diagrams[{doc_key}] lists {vid} more than once")
            ok = False
        else:
            seen.add(vid)
            valid.append(vid)
    return findings, (valid if ok else None)


def check_diagrams_fresh(root: Path, model: ModelParse, views: ViewsParse) -> list[str]:
    """Fail-closed freshness check surfaced by arch_check; never raises on malformed input."""
    findings = [f"diagrams-fresh: {f}" for f in model.findings + views.findings]
    diagrams = _diagrams_map(root)
    mapping: dict[str, list[str]] = {}
    if diagrams is not None:
        if not isinstance(diagrams, dict):
            findings.append("diagrams-fresh: diagrams block in index.yaml must be a mapping of doc -> view-id list")
        else:
            for doc_key, vids in diagrams.items():
                entry_findings, valid = _validate_entry(doc_key, vids)
                findings += entry_findings
                if valid is not None:
                    mapping[doc_key] = valid
    findings += _check_freshness(root, model, views, mapping)
    findings += _check_orphan_markers(root, mapping)
    return findings


def _check_freshness(root: Path, model: ModelParse, views: ViewsParse, mapping: dict[str, list[str]]) -> list[str]:
    by_id = {v.id: v for v in views.views}
    findings: list[str] = []
    for doc_key, vids in mapping.items():
        doc_path = root / doc_key
        if not doc_path.exists():
            findings.append(f"diagrams-fresh: mapped doc {doc_key} does not exist; run {FIX_CMD}")
            continue
        text = doc_path.read_text(encoding="utf-8")
        for vid in vids:
            view = by_id.get(vid)
            if view is None:
                findings.append(f"diagrams-fresh: view {vid} mapped to {doc_key} does not exist in views.c4; run {FIX_CMD}")
                continue
            span, error = _marker_span(text, vid)
            if error == "missing":
                findings.append(f"diagrams-fresh: {doc_key} is missing the {_open_marker(vid)} marker pair; run {FIX_CMD}")
            elif error == "duplicate":
                findings.append(f"diagrams-fresh: {doc_key} has duplicate {_open_marker(vid)} markers; run {FIX_CMD}")
            elif error == "order":
                findings.append(f"diagrams-fresh: {doc_key} has the closing {vid} marker before the opening one; run {FIX_CMD}")
            elif span is not None and text[span[0] : span[1]] != _canonical_block(view, model):
                findings.append(f"diagrams-fresh: {doc_key} diagram {vid} is stale; run {FIX_CMD}")
    return findings


def _check_orphan_markers(root: Path, mapping: dict[str, list[str]]) -> list[str]:
    findings: list[str] = []
    for doc_path in sorted((root / "architecture").glob("*.arc42.md")):
        doc_key = f"architecture/{doc_path.name}"
        mapped = set(mapping.get(doc_key, []))
        for m in _MARKER_RE.finditer(doc_path.read_text(encoding="utf-8")):
            vid = m.group(2)
            if vid in mapped:
                continue
            kind = "closing" if m.group(1) == "/" else "opening"
            findings.append(f"diagrams-fresh: {doc_key} has an orphan {kind} likec4:{vid} marker; run {FIX_CMD}")
    return findings


def main(root: Path | None = None) -> int:
    changed = generate(root or REPO_ROOT)
    for doc_key in changed:
        print(doc_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
