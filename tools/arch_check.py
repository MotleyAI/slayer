"""Living-architecture cross-walk checker (see architecture/system.arc42.md)."""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

import yaml

CHECK_IDS = frozenset(
    {
        "claims-exist",
        "claims-exactly-once",
        "contracts-known",
        "arc42-exists",
        "model-identity",
        "spec-mapping",
        "baseline-ratchet",
        "model-truth",
        "enforced-tags",
    }
)

_ELEMENT_RE = re.compile(r"^\s*(\w+)\s*=\s*(\w+)\s+'[^']*'")
_RELATION_RE = re.compile(r"^(\w+)\s*->\s*(\w+)(?:\s+#legacy)?$")
_ENFORCED_RE = re.compile(r"\[enforced:\s*([^\]\s][^\]]*)\]")
_INIT_PY = "__init__.py"


def _load_index(root: Path) -> dict:
    return yaml.safe_load((root / "architecture" / "index.yaml").read_text(encoding="utf-8"))


def _pyproject_importlinter(root: Path) -> dict:
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    return data.get("tool", {}).get("importlinter", {})


def _node_claims(nodes: dict) -> dict[str, list[str]]:
    """Node id -> dotted units it claims (package + loose claims, or bucket packages)."""
    claims: dict[str, list[str]] = {}
    for node_id, spec in nodes.items():
        if spec.get("virtual"):
            claims[node_id] = list(spec.get("packages", []))
        else:
            claims[node_id] = [spec["package"], *spec.get("claims", [])]
    return claims


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


def _model_files(root: Path) -> list[Path]:
    return sorted((root / "architecture" / "model").glob("*.c4"))


def _parse_elements(root: Path) -> set[str]:
    ids: set[str] = set()
    for path in _model_files(root):
        for line in path.read_text(encoding="utf-8").splitlines():
            m = _ELEMENT_RE.match(line)
            if m:
                ids.add(m.group(1))
    return ids


def _parse_relations(root: Path) -> tuple[set[tuple[str, str]], list[str]]:
    """All `src -> dst` relation lines in model/*.c4, plus malformed-line findings."""
    relations: set[tuple[str, str]] = set()
    findings: list[str] = []
    for path in _model_files(root):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "->" not in line or line.lstrip().startswith("//"):
                continue
            m = _RELATION_RE.match(line.strip())
            if m:
                relations.add((m.group(1), m.group(2)))
            else:
                findings.append(f"model-truth: unparseable relation line {path.name}:{lineno}: {line.strip()}")
    return relations, findings


def _bound_names(node: ast.AST) -> set[str]:
    """Names (re)bound by a non-import node — used to invalidate typing aliases."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, ast.Delete):
        return {t.id for t in node.targets if isinstance(t, ast.Name)}
    targets: list[ast.expr] = []
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, (ast.AugAssign, ast.AnnAssign, ast.For, ast.AsyncFor, ast.NamedExpr)):
        targets = [node.target]
    elif isinstance(node, ast.withitem) and node.optional_vars is not None:
        targets = [node.optional_vars]
    return {sub.id for t in targets for sub in ast.walk(t) if isinstance(sub, ast.Name)}


def _typing_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Names bound to the typing module and to typing.TYPE_CHECKING, minus any rebound later."""
    modules: set[str] = set()
    flags: set[str] = set()
    rebound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(a.asname or a.name for a in node.names if a.name == "typing")
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == "typing":
            flags.update(a.asname or a.name for a in node.names if a.name == "TYPE_CHECKING")
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


def _source_unit(rel: Path, root_package: str, top_to_node: dict[str, str]) -> tuple[list[str], bool, str] | None:
    """(module_parts, is_pkg_init, src_node) for a repo-relative .py path, or None if node-less."""
    parts = list(rel.with_suffix("").parts)
    is_pkg_init = parts[-1] == "__init__"
    if is_pkg_init:
        parts = parts[:-1]
    if parts == [root_package]:
        return None
    src_node = top_to_node.get(parts[1])
    if src_node is None:
        return None
    return parts, is_pkg_init, src_node


def _target_node(target: str, root_package: str, top_to_node: dict[str, str]) -> str | None:
    tparts = target.split(".")
    if tparts[0] != root_package or len(tparts) < 2:
        return None
    return top_to_node.get(tparts[1])


def measure_runtime_node_edges(root: Path, root_package: str, top_to_node: dict[str, str]) -> set[tuple[str, str]]:
    """Node-level runtime import edges, AST-measured; the root __init__ is exempt."""
    edges: set[tuple[str, str]] = set()
    for py in sorted((root / root_package).rglob("*.py")):
        rel = py.relative_to(root)
        if "__pycache__" in rel.parts:
            continue
        unit = _source_unit(rel, root_package, top_to_node)
        if unit is None:
            continue
        parts, is_pkg_init, src_node = unit
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for target in _runtime_import_targets(tree, parts, is_pkg_init):
            dst_node = _target_node(target, root_package, top_to_node)
            if dst_node is not None and dst_node != src_node:
                edges.add((src_node, dst_node))
    return edges


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


def _check_contracts(index: dict, importlinter: dict) -> list[str]:
    findings: list[str] = []
    index_contracts = set(index.get("contracts", {}))
    for spec in index.get("nodes", {}).values():
        if "contract" in spec:
            index_contracts.add(spec["contract"])
    pyproject_contracts = {c.get("name") for c in importlinter.get("contracts", [])}
    for name in sorted(index_contracts - pyproject_contracts):
        findings.append(f"contracts-known: contract {name} is in index.yaml but not in [tool.importlinter]")
    for name in sorted(pyproject_contracts - index_contracts):
        findings.append(f"contracts-known: contract {name} is in [tool.importlinter] but unknown to index.yaml")
    return findings


def _check_arc42(root: Path, nodes: dict) -> list[str]:
    findings: list[str] = []
    if not (root / "architecture" / "system.arc42.md").is_file():
        findings.append("arc42-exists: architecture/system.arc42.md is missing")
    for node_id, spec in nodes.items():
        arc42 = spec.get("arc42")
        if arc42 and not (root / arc42).is_file():
            findings.append(f"arc42-exists: {node_id} names {arc42}, which does not exist")
    return findings


def _check_model_identity(root: Path, nodes: dict) -> list[str]:
    findings: list[str] = []
    elements = _parse_elements(root)
    children = {child for spec in nodes.values() for child in spec.get("children", [])}
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


def _check_baselines(index: dict, importlinter: dict) -> list[str]:
    findings: list[str] = []
    by_name = {c.get("name"): c for c in importlinter.get("contracts", [])}
    for name, spec in index.get("contracts", {}).items():
        contract = by_name.get(name)
        if contract is None:
            continue  # reported by contracts-known
        actual = len(contract.get("ignore_imports", []))
        baseline = spec.get("baseline", 0)
        if actual != baseline:
            findings.append(
                f"baseline-ratchet: contract {name} has {actual} ignore_imports, baseline is {baseline}"
            )
    return findings


def _check_model_truth(root: Path, root_package: str, claims: dict[str, list[str]]) -> list[str]:
    top_to_node = {unit.split(".", 1)[1]: node for node, units in claims.items() for unit in units}
    measured = measure_runtime_node_edges(root, root_package, top_to_node)
    modeled, findings = _parse_relations(root)
    for src, dst in sorted(measured - modeled):
        findings.append(f"model-truth: measured runtime edge {src} -> {dst} is missing from the model")
    for src, dst in sorted(modeled - measured):
        findings.append(f"model-truth: modeled relation {src} -> {dst} has no measured runtime edge")
    return findings


def _check_enforced_tags(root: Path, importlinter: dict) -> list[str]:
    findings: list[str] = []
    contract_names = {c.get("name") for c in importlinter.get("contracts", [])}
    for path in sorted((root / "architecture").glob("*.arc42.md")):
        text = path.read_text(encoding="utf-8")
        tag_ids = _ENFORCED_RE.findall(text)
        if text.count("[enforced") != len(tag_ids):
            findings.append(f"enforced-tags: malformed [enforced: …] tag in {path.name}")
        for tag_id in tag_ids:
            tag_id = tag_id.strip()
            if tag_id in contract_names or tag_id.startswith("test:"):
                continue
            if tag_id.startswith("arch_check:") and tag_id.removeprefix("arch_check:") in CHECK_IDS:
                continue
            findings.append(f"enforced-tags: {path.name} tags unknown enforcement id {tag_id!r}")
    return findings


def run_checks(root: Path) -> list[str]:
    index = _load_index(root)
    importlinter = _pyproject_importlinter(root)
    root_package = importlinter.get("root_package", "slayer")
    nodes = index.get("nodes", {})
    claims = _node_claims(nodes)
    findings: list[str] = []
    findings += _check_claims(root, root_package, claims)
    findings += _check_contracts(index, importlinter)
    findings += _check_arc42(root, nodes)
    findings += _check_model_identity(root, nodes)
    findings += _check_spec_mapping(root, index)
    findings += _check_baselines(index, importlinter)
    findings += _check_model_truth(root, root_package, claims)
    findings += _check_enforced_tags(root, importlinter)
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
