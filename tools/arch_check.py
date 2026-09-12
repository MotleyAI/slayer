"""Living-architecture cross-walk checker (see architecture/system.arc42.md)."""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
from functools import lru_cache
from pathlib import Path

import yaml


def _load_arch_diagrams():
    """Load the sibling generator/parser module (single parser of the §5 convention)."""
    path = Path(__file__).resolve().parent / "arch_diagrams.py"
    spec = importlib.util.spec_from_file_location("arch_diagrams", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


arch_diagrams = _load_arch_diagrams()

# Element-tree helpers shared with the single parser (FQN = dotted path).
_is_or_ancestor = arch_diagrams._is_or_ancestor

CHECK_IDS = frozenset(
    {
        "claims-exist",
        "claims-exactly-once",
        "arc42-exists",
        "model-identity",
        "spec-mapping",
        "baseline-ratchet",
        "model-truth",
        "enforced-tags",
        "diagrams-fresh",
    }
)

_TAG_RE = re.compile(r"\[(enforced|review|target)\b([^\]]*)\]")
_TAG_START_RE = re.compile(r"\[(enforced|review|target)\b")
_TARGET_ID_RE = re.compile(r"DEV-\d+")
_FENCE_RE = re.compile(r"`{3,}|~{3,}")
_PRINCIPLE_ITEM_RE = re.compile(r"^( {0,3})(\d+)\.\s")
_INIT_PY = "__init__.py"


def _load_index(root: Path) -> dict:
    return yaml.safe_load((root / "architecture" / "index.yaml").read_text(encoding="utf-8"))


def _root_package(index: dict) -> str:
    pkg = index.get("root_package")
    return pkg if isinstance(pkg, str) and pkg else "slayer"


def _node_claims(nodes: dict) -> dict[str, list[str]]:
    """Node id -> dotted units it claims (package + loose claims, or bucket packages)."""
    claims: dict[str, list[str]] = {}
    for node_id, spec in nodes.items():
        if spec.get("virtual"):
            claims[node_id] = list(spec.get("packages", []))
        else:
            claims[node_id] = [spec["package"], *spec.get("claims", [])]
    return claims


def _dotted_to_element(nodes: dict) -> dict[str, str]:
    """Declared dotted module path -> element FQN (node id, or `<node>.<child>` for children)."""
    mapping: dict[str, str] = {}
    for node_id, spec in nodes.items():
        if spec.get("virtual"):
            for pkg in spec.get("packages", []):
                mapping[pkg] = node_id
            continue
        package = spec["package"]
        mapping[package] = node_id
        for claim in spec.get("claims", []):
            mapping[claim] = node_id
        for child in spec.get("children", []):
            mapping[f"{package}.{child}"] = f"{node_id}.{child}"
    return mapping


def _attribute(module: str, dotted_to_element: dict[str, str]) -> str | None:
    """The finest declared element a module belongs to (longest dotted-path prefix), or None."""
    best_dotted: str | None = None
    for dotted in dotted_to_element:
        if (module == dotted or module.startswith(dotted + ".")) and (
            best_dotted is None or len(dotted) > len(best_dotted)
        ):
            best_dotted = dotted
    return dotted_to_element[best_dotted] if best_dotted is not None else None


def _module_path(root: Path, dotted: str) -> Path | None:
    base = root / Path(*dotted.split("."))
    if (base / _INIT_PY).is_file():
        return base
    py = base.with_suffix(".py")
    if py.is_file():
        return py
    return None


def _top_level_units(root: Path, root_package: str) -> set[str]:
    """Immediate children of the root package: subpackages and loose .py modules."""
    pkg_dir = root / root_package
    units: set[str] = set()
    for child in pkg_dir.iterdir():
        if child.name == "__pycache__":
            continue
        if child.is_dir() and (child / _INIT_PY).is_file():
            units.add(f"{root_package}.{child.name}")
        elif child.is_file() and child.suffix == ".py" and child.name != _INIT_PY:
            units.add(f"{root_package}.{child.stem}")
    return units


def _param_names(args: ast.arguments) -> set[str]:
    params = [*args.posonlyargs, *args.args, *args.kwonlyargs, args.vararg, args.kwarg]
    return {a.arg for a in params if a is not None}


def _assignment_targets(node: ast.AST) -> list[ast.expr]:
    if isinstance(node, (ast.Assign, ast.Delete)):
        return node.targets
    if isinstance(node, (ast.AugAssign, ast.AnnAssign, ast.For, ast.AsyncFor, ast.NamedExpr)):
        return [node.target]
    if isinstance(node, ast.comprehension):
        return [node.target]
    if isinstance(node, ast.withitem) and node.optional_vars is not None:
        return [node.optional_vars]
    return []


def _bound_names(node: ast.AST) -> set[str]:
    """Names (re)bound by a non-import node — used to invalidate typing aliases."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return {node.name, *_param_names(node.args)}
    if isinstance(node, ast.Lambda):
        return _param_names(node.args)
    if isinstance(node, ast.ClassDef):
        return {node.name}
    if isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)):
        return {node.name} if node.name else set()
    if isinstance(node, ast.MatchMapping):
        return {node.rest} if node.rest else set()
    return {
        sub.id
        for t in _assignment_targets(node)
        for sub in ast.walk(t)
        if isinstance(sub, ast.Name) and isinstance(sub.ctx, (ast.Store, ast.Del))
    }


def _import_bindings(node: ast.Import, modules: set[str], rebound: set[str]) -> None:
    for a in node.names:
        if a.name == "typing":
            modules.add(a.asname or "typing")
        else:
            rebound.add(a.asname or a.name.split(".")[0])


def _import_from_bindings(node: ast.ImportFrom, flags: set[str], rebound: set[str]) -> None:
    from_typing = node.level == 0 and node.module == "typing"
    for a in node.names:
        if from_typing and a.name == "TYPE_CHECKING":
            flags.add(a.asname or "TYPE_CHECKING")
        else:
            rebound.add(a.asname or a.name)


def _typing_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Names bound to typing / typing.TYPE_CHECKING, minus names rebound by anything else."""
    modules: set[str] = set()
    flags: set[str] = set()
    rebound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            _import_bindings(node, modules, rebound)
        elif isinstance(node, ast.ImportFrom):
            _import_from_bindings(node, flags, rebound)
        else:
            rebound |= _bound_names(node)
    return modules - rebound, flags - rebound


def _is_type_checking_test(test: ast.expr, modules: set[str], flags: set[str]) -> bool:
    if isinstance(test, ast.Name):
        return test.id in flags
    return (
        isinstance(test, ast.Attribute)
        and test.attr == "TYPE_CHECKING"
        and isinstance(test.value, ast.Name)
        and test.value.id in modules
    )


def _import_from_base(node: ast.ImportFrom, module_parts: list[str], is_pkg_init: bool) -> str:
    if node.level == 0:
        return node.module or ""
    ctx = module_parts if is_pkg_init else module_parts[:-1]
    ctx = ctx[: len(ctx) - (node.level - 1)]
    return ".".join(ctx + ([node.module] if node.module else []))


def _stmt_import_targets(node: ast.AST, module_parts: list[str], is_pkg_init: bool) -> set[str]:
    if isinstance(node, ast.Import):
        return {alias.name for alias in node.names}
    if isinstance(node, ast.ImportFrom):
        base = _import_from_base(node, module_parts, is_pkg_init)
        if not base:
            return set()
        return {base, *(f"{base}.{alias.name}" for alias in node.names)}
    return set()


def _runtime_import_targets(tree: ast.Module, module_parts: list[str], is_pkg_init: bool) -> set[str]:
    """Absolute dotted targets of runtime imports; typing.TYPE_CHECKING-guarded bodies excluded."""
    modules, flags = _typing_bindings(tree)
    targets: set[str] = set()
    stack: list[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.If) and _is_type_checking_test(node.test, modules, flags):
            stack.extend(node.orelse)
            continue
        targets |= _stmt_import_targets(node, module_parts, is_pkg_init)
        stack.extend(ast.iter_child_nodes(node))
    return targets


def _source_module(rel: Path, root_package: str) -> str | None:
    """Dotted module id for a repo-relative .py path, or None for the exempt root __init__."""
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if parts == [root_package]:
        return None
    return ".".join(parts)


def _internal(src_elem: str, dst_elem: str) -> bool:
    """Self-pairs and ancestor<->descendant pairs are internal plumbing, never governed by arrows."""
    return _is_or_ancestor(src_elem, dst_elem) or _is_or_ancestor(dst_elem, src_elem)


def measure_runtime_edges(
    root: Path, root_package: str, dotted_to_element: dict[str, str]
) -> dict[tuple[str, str], tuple[str, str]]:
    """Element-level runtime import edges -> a witness (importing module, imported module).

    Endpoints attribute to their finest declared element; self- and ancestor/descendant pairs
    drop as internal; TYPE_CHECKING-guarded imports are excluded; the root __init__ is exempt.
    """
    witnesses: dict[tuple[str, str], tuple[str, str]] = {}
    for py in sorted((root / root_package).rglob("*.py")):
        rel = py.relative_to(root)
        if "__pycache__" in rel.parts:
            continue
        parts = list(rel.with_suffix("").parts)
        is_pkg_init = parts[-1] == "__init__"
        src_module = _source_module(rel, root_package)
        if src_module is None:
            continue
        src_elem = _attribute(src_module, dotted_to_element)
        if src_elem is None:
            continue
        module_parts = parts[:-1] if is_pkg_init else parts
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for target in sorted(_runtime_import_targets(tree, module_parts, is_pkg_init)):
            dst_elem = _attribute(target, dotted_to_element)
            if dst_elem is None or src_elem == dst_elem or _internal(src_elem, dst_elem):
                continue
            witnesses.setdefault((src_elem, dst_elem), (src_module, target))
    return witnesses


def _covers(arrow: tuple[str, str], edge: tuple[str, str]) -> bool:
    return _is_or_ancestor(arrow[0], edge[0]) and _is_or_ancestor(arrow[1], edge[1])


def _more_specific(a: tuple[str, str], b: tuple[str, str]) -> bool:
    """a is strictly more specific than b: descends-from b on both endpoints, and differs."""
    return a != b and _is_or_ancestor(b[0], a[0]) and _is_or_ancestor(b[1], a[1])


def _arrow_is_live(arrow: tuple[str, str], edges: list[tuple[str, str]], arrows: list[tuple[str, str]]) -> bool:
    """An arrow is live iff it is a most-specific cover of at least one measured edge."""
    covered = [e for e in edges if _covers(arrow, e)]
    if not covered:
        return False
    for edge in covered:
        rivals = [a for a in arrows if _covers(a, edge)]
        if not any(_more_specific(a, arrow) for a in rivals):
            return True
    return False


def _check_model_truth(
    root: Path, root_package: str, nodes: dict, arrows: list[tuple[str, str]]
) -> list[str]:
    dotted_to_element = _dotted_to_element(nodes)
    witnesses = measure_runtime_edges(root, root_package, dotted_to_element)
    edges = list(witnesses)
    findings: list[str] = []
    # A parent<->child arrow is meaningless (those edges are internal, never measured) and would
    # asymmetrically cover sibling edges — reject it and keep it out of the coverage computation.
    valid = [a for a in arrows if not _internal(a[0], a[1])]
    for src, dst in arrows:
        if _internal(src, dst):
            findings.append(
                f"model-truth: modeled relation {src} -> {dst} connects an element to its own"
                " ancestor or descendant; such edges are internal and never declared"
            )
    for edge in sorted(edges):
        if not any(_covers(arrow, edge) for arrow in valid):
            src_mod, dst_mod = witnesses[edge]
            findings.append(
                f"model-truth: measured runtime edge {edge[0]} -> {edge[1]} is missing from the model"
                f" (import {src_mod} -> {dst_mod})"
            )
    for arrow in valid:
        if any(_covers(arrow, e) for e in edges):
            if not _arrow_is_live(arrow, edges, valid):
                findings.append(f"model-truth: modeled relation {arrow[0]} -> {arrow[1]} is fully shadowed")
        else:
            findings.append(f"model-truth: modeled relation {arrow[0]} -> {arrow[1]} has no measured runtime edge")
    return findings


@lru_cache(maxsize=None)
def _license_model(root_str: str) -> tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]:
    """Cached (dotted-path, element) pairs plus arrow set for `license`, keyed by repo root."""
    root = Path(root_str)
    index = _load_index(root)
    mapping = tuple(_dotted_to_element(index.get("nodes", {})).items())
    arrows = tuple(
        (r.src, r.dst) for r in arch_diagrams.parse_model(root).relations if not _internal(r.src, r.dst)
    )
    return mapping, arrows


def license(*, root: Path, src: str, dst: str) -> bool:
    """Whether the model licenses a runtime import from module `src` to module `dst`.

    Internal (self / ancestor-descendant) and unmodelled endpoints are never banned; otherwise
    the edge must be covered by a declared arrow. Exposes the one import law for parity testing.
    """
    mapping, arrows = _license_model(str(root))
    dotted_to_element = dict(mapping)
    src_elem = _attribute(src, dotted_to_element)
    dst_elem = _attribute(dst, dotted_to_element)
    if src_elem is None or dst_elem is None or _internal(src_elem, dst_elem):
        return True
    return any(_covers(arrow, (src_elem, dst_elem)) for arrow in arrows)


def _check_claims(root: Path, root_package: str, claims: dict[str, list[str]]) -> list[str]:
    findings: list[str] = []
    seen: dict[str, str] = {}
    for node_id, units in claims.items():
        for unit in units:
            if _module_path(root, unit) is None:
                findings.append(f"claims-exist: {node_id} claims {unit}, which does not exist on disk")
            if unit in seen:
                findings.append(f"claims-exactly-once: {unit} claimed by both {seen[unit]} and {node_id}")
            else:
                seen[unit] = node_id
    unclaimed = _top_level_units(root, root_package) - set(seen)
    for unit in sorted(unclaimed):
        findings.append(f"claims-exactly-once: top-level {unit} is claimed by no node")
    return findings


def _package_units(nodes: dict) -> set[str]:
    """Every dotted package/claim a node owns (for cross-node child collision detection)."""
    units: set[str] = set()
    for spec in nodes.values():
        if spec.get("virtual"):
            units.update(spec.get("packages", []))
        else:
            units.add(spec["package"])
            units.update(spec.get("claims", []))
    return units


def _check_children(root: Path, nodes: dict) -> list[str]:
    """Declared children resolve on disk under their node's package, are named at most once, and
    do not collide with another node's package or claim."""
    findings: list[str] = []
    owned = _package_units(nodes)
    for node_id, spec in nodes.items():
        children = spec.get("children", [])
        if not children:
            continue
        if spec.get("virtual"):
            findings.append(f"claims-exist: virtual node {node_id} may not declare children")
            continue
        package = spec["package"]
        seen: set[str] = set()
        for child in children:
            dotted = f"{package}.{child}"
            if child in seen:
                findings.append(f"claims-exactly-once: {node_id} declares child {child} more than once")
            seen.add(child)
            if dotted in owned:
                findings.append(
                    f"claims-exactly-once: {node_id} child {child} ({dotted}) collides with a declared package/claim"
                )
            if _module_path(root, dotted) is None:
                findings.append(f"claims-exist: {node_id} declares child {child}, which does not exist on disk")
    return findings


def _check_arc42(root: Path, index: dict) -> list[str]:
    findings: list[str] = []
    system = "architecture/system.arc42.md"
    if not (root / system).is_file():
        findings.append(f"arc42-exists: {system} is missing")
    registered = {system}
    for node_id, spec in index.get("nodes", {}).items():
        arc42 = spec.get("arc42")
        if arc42:
            registered.add(arc42)
            if not (root / arc42).is_file():
                findings.append(f"arc42-exists: {node_id} names {arc42}, which does not exist")
    for entry in index.get("cross_cutting_arc42", []):
        registered.add(entry)
        if not (root / entry).is_file():
            findings.append(f"arc42-exists: cross_cutting_arc42 names {entry}, which does not exist")
    for path in sorted((root / "architecture").glob("*.arc42.md")):
        rel = f"architecture/{path.name}"
        if rel not in registered:
            findings.append(
                f"arc42-exists: {rel} is orphaned — not {system}, a node arc42 entry, or in cross_cutting_arc42"
            )
    return findings


def _declared_children(nodes: dict) -> set[str]:
    return {f"{node_id}.{child}" for node_id, spec in nodes.items() for child in spec.get("children", [])}


def _check_model_identity(nodes: dict, elements: set[str]) -> list[str]:
    findings: list[str] = []
    children = _declared_children(nodes)
    for node_id in nodes:
        if node_id not in elements:
            findings.append(f"model-identity: node {node_id} is not declared as an element in model/*.c4")
    for child in sorted(children):
        if child not in elements:
            findings.append(f"model-identity: child {child} is not declared as an element in model/*.c4")
    for element in sorted(elements - set(nodes) - children):
        findings.append(f"model-identity: element {element} maps to no node or declared child in index.yaml")
    return findings


def _mapped_spec_groups(index: dict, findings: list[str]) -> dict[str, str]:
    """Spec group -> owning node (or cross_cutting_specs), appending duplicate-mapping findings."""
    nodes = index.get("nodes", {})
    mapped: dict[str, str] = {}
    for node_id, spec in nodes.items():
        for group in spec.get("specs", []):
            if group in mapped:
                findings.append(f"spec-mapping: {group} mapped by both {mapped[group]} and {node_id}")
            mapped[group] = node_id
    for group, spec in index.get("cross_cutting_specs", {}).items():
        if group in mapped:
            findings.append(f"spec-mapping: {group} mapped by both {mapped[group]} and cross_cutting_specs")
        mapped[group] = "cross_cutting_specs"
        for touched in spec.get("touches", []):
            if touched not in nodes:
                findings.append(f"spec-mapping: {group} touches unknown node {touched}")
    return mapped


def _check_spec_mapping(root: Path, index: dict) -> list[str]:
    findings: list[str] = []
    mapped = _mapped_spec_groups(index, findings)
    specs_dir = root / "openspec" / "specs"
    on_disk = {p.name for p in specs_dir.iterdir() if p.is_dir()} if specs_dir.is_dir() else set()
    for group in sorted(on_disk - set(mapped)):
        findings.append(f"spec-mapping: openspec/specs/{group} is mapped by no node and not cross-cutting")
    for group in sorted(set(mapped) - on_disk):
        findings.append(f"spec-mapping: {group} is mapped but openspec/specs/{group} does not exist")
    for group in sorted(set(mapped) & on_disk):
        if not any((specs_dir / group).rglob("spec.md")):
            findings.append(f"spec-mapping: openspec/specs/{group} contains no spec.md")
    return findings


def _check_legacy_ratchet(index: dict, arrows_legacy: int) -> list[str]:
    """The count of `#legacy` arrows in the model must equal the declared baseline (only ever lowered)."""
    spec = index.get("legacy_arrows")
    if not isinstance(spec, dict) or "baseline" not in spec:
        return ["baseline-ratchet: legacy_arrows.baseline is missing from index.yaml"]
    baseline = spec["baseline"]
    if not isinstance(baseline, int) or isinstance(baseline, bool) or baseline < 0:
        return [f"baseline-ratchet: legacy_arrows.baseline must be a non-negative integer, got {baseline!r}"]
    if arrows_legacy != baseline:
        return [f"baseline-ratchet: model has {arrows_legacy} legacy arrow(s), baseline is {baseline}"]
    return []


def _parse_tag_id(rest: str) -> str | None:
    """Tag id from ': <id>' — None when malformed (no colon, empty, or multi-line)."""
    if not rest.startswith(":"):
        return None
    tag_id = rest[1:].strip()
    if not tag_id or "\n" in tag_id:
        return None
    return tag_id


def _tag_occurrence(kind: str, rest: str, name: str) -> tuple[bool, list[str]]:
    """(counts as status coverage, findings) for one bracket tag."""
    if kind == "review":
        return (True, []) if not rest else (False, [f"enforced-tags: malformed [review] tag in {name}"])
    tag_id = _parse_tag_id(rest)
    if tag_id is None:
        return False, [f"enforced-tags: malformed [{kind}: …] tag in {name}"]
    if kind == "target":
        if _TARGET_ID_RE.fullmatch(tag_id) is None:
            return False, [f"enforced-tags: {name} target tag id {tag_id!r} does not match DEV-<number>"]
        return True, []
    if tag_id.startswith("test:") and tag_id != "test:":
        return True, []
    if tag_id.startswith("arch_check:") and tag_id.removeprefix("arch_check:") in CHECK_IDS:
        return True, []
    return False, [f"enforced-tags: {name} tags unknown enforcement id {tag_id!r}"]


def _strip_fences(text: str) -> str:
    """Blank out fenced code blocks so tags and numbered items inside are ignored."""
    out: list[str] = []
    fence = ""  # opening delimiter run; closer is delimiter-only, same char, >= length
    for line in text.splitlines():
        if not fence:
            m = _FENCE_RE.match(line.lstrip())
            if m:
                fence = m.group(0)
            out.append("" if m else line)
        else:
            m = _FENCE_RE.fullmatch(line.strip())
            if m and m.group(0)[0] == fence[0] and len(m.group(0)) >= len(fence):
                fence = ""
            out.append("")
    return "\n".join(out)


def _principle_items(text: str) -> list[tuple[str, str]]:
    """Top-level numbered items as (number, item text incl. continuation lines)."""
    items: list[tuple[str, str]] = []
    open_col = -1  # content column of the open item; -1 = closed
    for line in text.splitlines():
        m = _PRINCIPLE_ITEM_RE.match(line)
        # a number indented to the open item's content column is its content (CommonMark)
        if m and not (open_col >= 0 and len(m.group(1)) >= open_col):
            items.append((m.group(2), line))
            open_col = len(m.group(1)) + len(m.group(2)) + 2
        elif open_col >= 0 and line.strip() and not line.startswith("#"):
            num, body = items[-1]
            items[-1] = (num, body + "\n" + line)
        else:
            open_col = -1
    return items


def _check_enforced_tags(root: Path) -> list[str]:
    findings: list[str] = []
    for path in sorted((root / "architecture").glob("*.arc42.md")):
        text = _strip_fences(path.read_text(encoding="utf-8"))
        occurrences = list(_TAG_RE.finditer(text))
        for kind in ("enforced", "review", "target"):
            starts = sum(1 for m in _TAG_START_RE.finditer(text) if m.group(1) == kind)
            closed = sum(1 for m in occurrences if m.group(1) == kind)
            if starts != closed:
                findings.append(f"enforced-tags: malformed [{kind}: …] tag in {path.name}")
        for m in occurrences:
            findings += _tag_occurrence(m.group(1), m.group(2), path.name)[1]
        for num, body in _principle_items(text):
            covered = any(
                _tag_occurrence(t.group(1), t.group(2), path.name)[0] for t in _TAG_RE.finditer(body)
            )
            if not covered:
                findings.append(f"enforced-tags: {path.name} principle item {num} has no status tag")
    return findings


def run_checks(root: Path) -> list[str]:
    index = _load_index(root)
    root_package = _root_package(index)
    nodes = index.get("nodes", {})
    claims = _node_claims(nodes)
    model = arch_diagrams.parse_model(root)
    views = arch_diagrams.parse_views(root=root, model=model)
    elements = {e.id for e in model.elements}
    arrows = [(r.src, r.dst) for r in model.relations]
    legacy_count = sum(1 for r in model.relations if r.legacy)
    findings: list[str] = []
    findings += _check_claims(root, root_package, claims)
    findings += _check_children(root, nodes)
    findings += _check_arc42(root, index)
    findings += _check_model_identity(nodes=nodes, elements=elements)
    findings += _check_spec_mapping(root, index)
    findings += _check_legacy_ratchet(index, legacy_count)
    findings += _check_model_truth(root=root, root_package=root_package, nodes=nodes, arrows=arrows)
    findings += _check_enforced_tags(root)
    findings += arch_diagrams.check_diagrams_fresh(root=root, model=model, views=views)
    return findings


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    findings = run_checks(root)
    for finding in findings:
        print(finding)
    if findings:
        print(f"arch_check: {len(findings)} finding(s)")
        return 1
    print("arch_check: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
