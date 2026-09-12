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


class _RawView(BaseModel):
    """A view's include grammar, parsed before the depth knob is known."""

    id: str
    title: str
    base: list[str]
    src_anchors: set[str]
    dst_anchors: set[str]


_ELEMENT_RE = re.compile(r"^(\w+)\s*=\s*(\w+)\s+'([^']*)'(?:\s*\{)?\s*$")
_RELATION_RE = re.compile(r"^([\w.]+)\s*->\s*([\w.]+)(\s+#legacy)?\s*$")
_SPEC_ELEMENT_RE = re.compile(r"^element\s+(\w+)(?:\s*\{)?\s*$")
_TAG_DECL_RE = re.compile(r"^tag\s+\w+$")
_BLOCK_RE = re.compile(r"^(specification|model|views)\b")
_BRACES_ONLY_RE = re.compile(r"^[{}]+$")
_TOKEN_RE = re.compile(r"'[^']*'|->|[\w.]+|\S")
_MARKER_RE = re.compile(r"<!--\s+(/?)likec4:(\w+)\s+-->")

DEFAULT_VIEW_DEPTH = 3


def _fqn(local: str, parents: list[str]) -> str:
    return f"{parents[-1]}.{local}" if parents else local


def _top(eid: str) -> str:
    return eid.split(".")[0]


def _level(eid: str) -> int:
    return eid.count(".") + 1


def _ancestor_at_level(eid: str, level: int) -> str:
    return ".".join(eid.split(".")[:level])


def _is_or_ancestor(anc: str, eid: str) -> bool:
    return anc == eid or eid.startswith(anc + ".")


def _read_exact(path: Path) -> str:
    """Read preserving exact bytes (no universal-newline translation) for byte-for-byte checks."""
    return path.read_bytes().decode("utf-8")


def _is_arc42_doc_key(doc_key: object) -> bool:
    """True only for a direct architecture/<name>.arc42.md child (rejects traversal/nesting)."""
    if not (isinstance(doc_key, str) and doc_key.endswith(".arc42.md")):
        return False
    parts = Path(doc_key).parts
    return len(parts) == 2 and parts[0] == "architecture"


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
        _scan_model_file(
            path=path,
            kinds=kinds,
            elements=elements,
            seen_ids=seen_ids,
            relations=relations,
            rel_pairs=rel_pairs,
            findings=findings,
        )
    for element in elements:
        if element.kind not in kinds:
            findings.append(f"element {element.id} has undeclared kind {element.kind}")
        element.virtual = kinds.get(element.kind, False)
    for relation in relations:
        for endpoint in (relation.src, relation.dst):
            if endpoint not in seen_ids:
                findings.append(f"relation {relation.src} -> {relation.dst} has unknown endpoint {endpoint}")
    return ModelParse(elements=elements, relations=relations, findings=findings)


def _scan_model_file(  # NOSONAR(S3776) — cohesive brace/region state machine; splitting scatters the model grammar
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
            for stmt in _split_spec_statements(code) if delta == 0 else [code]:
                spec_kind = _scan_spec_line(
                    code=stmt, kinds=kinds, spec_kind=spec_kind, delta=delta, findings=findings
                )
        elif region == "model":
            _scan_model_line(
                code=code,
                elements=elements,
                seen_ids=seen_ids,
                relations=relations,
                rel_pairs=rel_pairs,
                parents=parents,
                delta=delta,
                findings=findings,
            )
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


def _split_spec_statements(code: str) -> list[str]:
    """Split a brace-free specification line into its `element`/`tag` statements."""
    return [part for part in re.split(r"\s+(?=(?:element|tag)\b)", code) if part]


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
        if spec_kind is not None and code == "#virtual":
            kinds[spec_kind] = True
            return spec_kind
        findings.append(f"unrecognized specification line: {code}")
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
        kind, title = m.group(2), m.group(3)
        eid = _fqn(m.group(1), parents)
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


def parse_views(root: Path, model: ModelParse) -> ViewsParse:  # NOSONAR(S3776) — one include-grammar parser; the nested cursor closures read clearer kept together
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

    def parse_body(vid: str) -> _RawView:
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
        return _RawView(id=vid, title=title, base=base, src_anchors=src_anchors, dst_anchors=dst_anchors)

    raw: list[_RawView] = []
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
        raw.append(parse_body(idt[1]))
        if idt[1] in seen_ids:
            findings.append(f"duplicate view id {idt[1]}")
        seen_ids.add(idt[1])
    depths = _view_depths(root=root, valid_ids=seen_ids, findings=findings)
    views = [
        _build_view(spec=spec, depth=depths.get(spec.id, DEFAULT_VIEW_DEPTH), model=model) for spec in raw
    ]
    return ViewsParse(views=views, findings=findings)


def _view_depths(root: Path, valid_ids: set[str], findings: list[str]) -> dict[str, int]:
    """Per-view render depth from index.yaml `view_depth`; malformed entries are findings, not raises."""
    index_path = root / "architecture" / "index.yaml"
    if not index_path.is_file():
        return {}
    try:
        index = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return {}
    depth_map = index.get("view_depth") if isinstance(index, dict) else None
    if depth_map is None:
        return {}
    if not isinstance(depth_map, dict):
        findings.append("view_depth must map view ids to positive integers")
        return {}
    depths: dict[str, int] = {}
    for vid, value in depth_map.items():
        if vid not in valid_ids:
            findings.append(f"view_depth references unknown view id {vid}")
        elif not isinstance(value, int) or isinstance(value, bool) or value < 1:
            findings.append(f"view_depth[{vid}] must be a positive integer, got {value!r}")
        else:
            depths[vid] = value
    return depths


def _children_map(model: ModelParse) -> dict[str, list[str]]:
    kids: dict[str, list[str]] = {}
    for e in model.elements:
        if e.parent is not None:
            kids.setdefault(e.parent, []).append(e.id)
    return kids


def _ancestor_chain(eid: str) -> list[str]:
    """`eid` and every ancestor id, top-level first (`a.b.c` -> [a, a.b, a.b.c])."""
    parts = eid.split(".")
    return [".".join(parts[: i + 1]) for i in range(len(parts))]


def _build_view(spec: _RawView, depth: int, model: ModelParse) -> View:
    """Expand base includes to `depth` (full subtree); predicate pulls add only the matched
    endpoint (collapsed to `depth`) and its ancestor chain. Deeper edges roll up to the cutoff."""
    among = set(spec.base)
    matched = [
        r for r in model.relations if _top(r.src) in spec.src_anchors or _top(r.dst) in spec.dst_anchors
    ]
    kids = _children_map(model)
    shown: set[str] = set()
    for top in spec.base:
        for eid in _subtree(top, kids):
            if _level(eid) <= depth:
                shown.add(eid)
    for r in matched:
        for endpoint in (r.src, r.dst):
            shown.update(_ancestor_chain(_ancestor_at_level(endpoint, min(depth, _level(endpoint)))))

    def represent(eid: str) -> str | None:
        for anc in reversed(_ancestor_chain(eid)):
            if anc in shown:
                return anc
        return None

    contributors: dict[tuple[str, str], list[Relation]] = {}
    order: list[tuple[str, str]] = []
    for r in model.relations:
        in_view = (_top(r.src) in among and _top(r.dst) in among) or _top(r.src) in spec.src_anchors or _top(r.dst) in spec.dst_anchors
        if not in_view:
            continue
        rep_src, rep_dst = represent(r.src), represent(r.dst)
        if rep_src is None or rep_dst is None or rep_src == rep_dst:
            continue
        key = (rep_src, rep_dst)
        if key not in contributors:
            contributors[key] = []
            order.append(key)
        contributors[key].append(r)
    edges = [Edge(src=s, dst=d, legacy=all(r.legacy for r in contributors[(s, d)])) for s, d in order]
    node_ids = [e.id for e in model.elements if e.id in shown]
    return View(id=spec.id, title=spec.title, node_ids=node_ids, edges=edges)


def _subtree(root: str, kids: dict[str, list[str]]) -> list[str]:
    """Pre-order ids of `root` and all its descendants (model declaration order within a parent)."""
    out = [root]
    for child in kids.get(root, []):
        out.extend(_subtree(child, kids))
    return out


def _mangle(eid: str) -> str:
    return eid.replace(".", "__")


def _escape_title(title: str) -> str:
    return title.replace('"', "#quot;")


def _node_line(element: Element | None, nid: str, indent: int) -> str:
    title = _escape_title(element.title if element else nid)
    shape = f'("{title}")' if element is not None and element.virtual else f'["{title}"]'
    return f"{' ' * indent}{_mangle(nid)}{shape}"


def render_mermaid(view: View, model: ModelParse) -> str:
    """Deterministic mermaid for one view: nested subgraphs when children are shown, else flat."""
    by_id = {e.id: e for e in model.elements}
    shown = set(view.node_ids)
    shown_kids: dict[str, list[str]] = {}
    for e in model.elements:
        if e.id in shown and e.parent in shown:
            shown_kids.setdefault(e.parent, []).append(e.id)
    lines = ["```mermaid", "flowchart TD", f"  %% {view.id}: {view.title}"]
    if any(shown_kids.values()):
        lines += _hierarchical_body(view=view, by_id=by_id, shown=shown, shown_kids=shown_kids)
    else:
        for nid in view.node_ids:
            lines.append(_node_line(by_id.get(nid), nid, 2))
        for edge in view.edges:
            lines.append(f"  {edge.src} {'-.->' if edge.legacy else '-->'} {edge.dst}")
    lines.append("```")
    if any(edge.legacy for edge in view.edges):
        lines.append("*Dashed arrows: legacy edges slated to die.*")
    return "\n".join(lines)


def _hierarchical_body(
    view: View, by_id: dict[str, Element], shown: set[str], shown_kids: dict[str, list[str]]
) -> list[str]:
    lines: list[str] = []
    leaves: list[str] = []

    def emit(nid: str, indent: int) -> None:
        pad = " " * indent
        if shown_kids.get(nid):
            lines.append(f'{pad}subgraph {_mangle(nid)}["{_escape_title(by_id[nid].title)}"]')
            for child in shown_kids[nid]:
                emit(child, indent + 2)
            lines.append(f"{pad}end")
        else:
            leaves.append(_mangle(nid))
            lines.append(_node_line(by_id.get(nid), nid, indent))

    for eid in view.node_ids:
        element = by_id.get(eid)
        if element is None or element.parent not in shown:
            emit(eid, 2)
    for edge in view.edges:
        lines.append(f"  {_mangle(edge.src)} {'-.->' if edge.legacy else '-->'} {_mangle(edge.dst)}")
    if leaves:
        lines.append("  classDef leaf fill:none;")
        lines.append(f"  class {','.join(leaves)} leaf;")
    return lines


def _open_marker(vid: str) -> str:
    return f"<!-- likec4:{vid} -->"


def _close_marker(vid: str) -> str:
    return f"<!-- /likec4:{vid} -->"


def _canonical_block(view: View, model: ModelParse) -> str:
    return f"{_open_marker(view.id)}\n{render_mermaid(view, model)}\n{_close_marker(view.id)}"


def _marker_span(text: str, vid: str) -> tuple[tuple[int, int] | None, str | None]:
    """(span, error): the open→close span, or None + one of missing/duplicate/order.

    Every whitespace form is counted via _MARKER_RE, so a stray variant marker for the
    same view (e.g. an extra double-space `<!--  likec4:x -->`) cannot slip past as fresh.
    """
    matches = [m for m in _MARKER_RE.finditer(text) if m.group(2) == vid]
    opens = [m for m in matches if not m.group(1)]
    closes = [m for m in matches if m.group(1)]
    if not opens or not closes:
        return None, "missing"
    if len(opens) > 1 or len(closes) > 1:
        return None, "duplicate"
    if closes[0].start() < opens[0].start():
        return None, "order"
    return (opens[0].start(), closes[0].end()), None


def _diagrams_map(root: Path) -> object:
    """The index.yaml `diagrams` mapping, or a string explaining why it is unusable (never raises)."""
    index_path = root / "architecture" / "index.yaml"
    if not index_path.is_file():
        return "index.yaml is missing"
    try:
        index = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return "index.yaml is not valid YAML"
    if not isinstance(index, dict):
        return "index.yaml is not a mapping"
    diagrams = index.get("diagrams")
    return "index.yaml has no diagrams block" if diagrams is None else diagrams


def _bad_diagrams_reason(diagrams: object) -> str:
    return diagrams if isinstance(diagrams, str) else "diagrams block in index.yaml must be a mapping of doc -> view-id list"


def _rewrite_markers(text: str, vids: list[str], by_id: dict[str, View], model: ModelParse, doc_key: str) -> str:
    """Rewrite every mapped view's marker block in one doc's text; raise on missing view/marker."""
    for vid in vids:
        view = by_id.get(vid)
        if view is None:
            raise ValueError(f"view {vid} mapped to {doc_key} is not defined in views.c4")
        span, error = _marker_span(text=text, vid=vid)
        if error is not None or span is None:
            raise ValueError(f"{doc_key}: {error} marker(s) for view {vid}")
        start, end = span
        text = text[:start] + _canonical_block(view, model) + text[end:]
    return text


def generate(root: Path) -> list[str]:
    """Rewrite each mapped doc's marker blocks from the model; return changed repo-relative paths."""
    diagrams = _diagrams_map(root)
    if not isinstance(diagrams, dict):
        raise ValueError(_bad_diagrams_reason(diagrams))
    model = parse_model(root)
    views = parse_views(root=root, model=model)
    problems = model.findings + views.findings
    if problems:
        raise ValueError("cannot regenerate diagrams — resolve model/views findings first:\n" + "\n".join(problems))
    by_id = {v.id: v for v in views.views}
    changed: list[str] = []
    for doc_key, vids in diagrams.items():
        if not _is_arc42_doc_key(doc_key):
            raise ValueError(f"diagrams key {doc_key!r} must be an architecture/*.arc42.md path")
        doc_path = root / doc_key
        text = _read_exact(doc_path)
        new_text = _rewrite_markers(text=text, vids=vids, by_id=by_id, model=model, doc_key=doc_key)
        if new_text != text:
            doc_path.write_text(new_text, encoding="utf-8", newline="")
            changed.append(doc_key)
    return changed


def _validate_entry(doc_key: object, vids: object) -> tuple[list[str], list[str] | None]:
    if not _is_arc42_doc_key(doc_key):
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


def _collect_mapping(diagrams: object, findings: list[str]) -> dict[str, list[str]]:
    """Validated doc -> view-ids mapping; appends schema findings, fail-closed on bad input."""
    if not isinstance(diagrams, dict):
        findings.append(f"diagrams-fresh: {_bad_diagrams_reason(diagrams)}; run {FIX_CMD}")
        return {}
    mapping: dict[str, list[str]] = {}
    for doc_key, vids in diagrams.items():
        entry_findings, valid = _validate_entry(doc_key=doc_key, vids=vids)
        findings += entry_findings
        if valid is not None:
            mapping[doc_key] = valid
    return mapping


def check_diagrams_fresh(root: Path, model: ModelParse, views: ViewsParse) -> list[str]:
    """Fail-closed freshness check surfaced by arch_check; never raises on malformed input."""
    findings = [f"diagrams-fresh: {f}" for f in model.findings + views.findings]
    mapping = _collect_mapping(diagrams=_diagrams_map(root), findings=findings)
    findings += _check_freshness(root=root, model=model, views=views, mapping=mapping)
    findings += _check_orphan_markers(root=root, mapping=mapping)
    return findings


def _check_freshness(root: Path, model: ModelParse, views: ViewsParse, mapping: dict[str, list[str]]) -> list[str]:
    by_id = {v.id: v for v in views.views}
    findings: list[str] = []
    for doc_key, vids in mapping.items():
        doc_path = root / doc_key
        if not doc_path.exists():
            findings.append(f"diagrams-fresh: mapped doc {doc_key} does not exist; run {FIX_CMD}")
            continue
        text = _read_exact(doc_path)
        for vid in vids:
            view = by_id.get(vid)
            if view is None:
                findings.append(f"diagrams-fresh: view {vid} mapped to {doc_key} does not exist in views.c4; run {FIX_CMD}")
                continue
            span, error = _marker_span(text=text, vid=vid)
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
        for m in _MARKER_RE.finditer(_read_exact(doc_path)):
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
