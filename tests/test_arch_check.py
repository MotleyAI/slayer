"""tools/arch_check.py cross-walk checks against tmp-dir repo fixtures."""

import importlib.util
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location("arch_check", REPO_ROOT / "tools" / "arch_check.py")
assert _spec is not None and _spec.loader is not None
arch_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arch_check)

PYPROJECT = """
[tool.importlinter]
root_package = "pkg"

[[tool.importlinter.contracts]]
name = "layers"
type = "layers"
layers = ["pkg.engine", "pkg.core"]
ignore_imports = ["pkg.core.a -> pkg.engine.b"]
"""

INDEX = """
nodes:
  core: {package: pkg.core}
  engine: {package: pkg.engine, arc42: architecture/engine.arc42.md}
contracts:
  layers: {baseline: 1}
cross_cutting_specs:
  queries: {touches: [core, engine]}
"""

MODEL = """
specification {
  element node
  tag legacy
}
model {
  core = node 'Core'
  engine = node 'Engine'
  core -> engine #legacy
  engine -> core
}
"""


def make_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel, content in {
        "pyproject.toml": PYPROJECT,
        "architecture/index.yaml": INDEX,
        "architecture/model/pkg.c4": MODEL,
        "architecture/system.arc42.md": "# System\n\n1. Layering. [enforced: layers]\n2. Soft rule. [review]\n",
        "architecture/engine.arc42.md": "# engine\n",
        "openspec/specs/queries/foo/spec.md": "# spec\n",
        "pkg/__init__.py": "",
        "pkg/core/__init__.py": "",
        "pkg/core/a.py": "from pkg.engine import b\n",
        "pkg/engine/__init__.py": "",
        "pkg/engine/b.py": "import pkg.core\n",
    }.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content), encoding="utf-8")
    return root


def findings_for(root: Path, check_id: str) -> list[str]:
    return [f for f in arch_check.run_checks(root) if f.startswith(f"{check_id}:")]


def test_healthy_fixture_passes(tmp_path):
    assert arch_check.run_checks(make_repo(tmp_path)) == []


def test_unclaimed_top_level_module(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "extra.py").write_text("", encoding="utf-8")
    assert any("pkg.extra" in f for f in findings_for(root, "claims-exactly-once"))


def test_duplicate_claim(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(
        index.replace("core: {package: pkg.core}", "core: {package: pkg.core, claims: [pkg.engine]}")
    )
    assert any("pkg.engine" in f for f in findings_for(root, "claims-exactly-once"))


def test_claimed_module_missing_on_disk(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(
        index.replace("core: {package: pkg.core}", "core: {package: pkg.core, claims: [pkg.ghost]}")
    )
    assert any("pkg.ghost" in f for f in findings_for(root, "claims-exist"))


def test_missing_arc42_file(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "engine.arc42.md").unlink()
    assert findings_for(root, "arc42-exists")


def test_unknown_touches_node(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(index.replace("[core, engine]", "[core, ghost]"))
    assert any("ghost" in f for f in findings_for(root, "spec-mapping"))


def test_unmapped_spec_group(tmp_path):
    root = make_repo(tmp_path)
    extra = root / "openspec" / "specs" / "models" / "bar" / "spec.md"
    extra.parent.mkdir(parents=True)
    extra.write_text("# spec\n", encoding="utf-8")
    assert any("models" in f for f in findings_for(root, "spec-mapping"))


def test_spec_group_without_spec_md(tmp_path):
    root = make_repo(tmp_path)
    (root / "openspec" / "specs" / "queries" / "foo" / "spec.md").unlink()
    assert any("no spec.md" in f for f in findings_for(root, "spec-mapping"))


def test_baseline_exceeded(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(index.replace("baseline: 1", "baseline: 0"))
    assert findings_for(root, "baseline-ratchet")


def test_contract_missing_from_pyproject(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(
        index.replace("layers: {baseline: 1}", "layers: {baseline: 1}\n  ghost: {baseline: 0}")
    )
    assert any("ghost" in f for f in findings_for(root, "contracts-known"))


def test_pyproject_contract_unknown_to_index(tmp_path):
    root = make_repo(tmp_path)
    py = (root / "pyproject.toml").read_text()
    (root / "pyproject.toml").write_text(
        py + '\n[[tool.importlinter.contracts]]\nname = "rogue"\ntype = "forbidden"\n'
    )
    assert any("rogue" in f for f in findings_for(root, "contracts-known"))


def test_node_missing_from_model(tmp_path):
    root = make_repo(tmp_path)
    model = (root / "architecture" / "model" / "pkg.c4").read_text()
    (root / "architecture" / "model" / "pkg.c4").write_text(model.replace("engine = node 'Engine'\n", ""))
    assert any("engine" in f for f in findings_for(root, "model-identity"))


def test_extra_element_in_model(tmp_path):
    root = make_repo(tmp_path)
    model = (root / "architecture" / "model" / "pkg.c4").read_text()
    (root / "architecture" / "model" / "pkg.c4").write_text(
        model.replace("engine = node 'Engine'", "engine = node 'Engine'\n  ghost = node 'Ghost'")
    )
    assert any("ghost" in f for f in findings_for(root, "model-identity"))


def test_modeled_relation_without_measured_edge(tmp_path):
    root = make_repo(tmp_path)
    model = (root / "architecture" / "model" / "pkg.c4").read_text()
    (root / "architecture" / "model" / "pkg.c4").write_text(model.replace("import pkg.core", ""))
    (root / "pkg" / "engine" / "b.py").write_text("", encoding="utf-8")
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_measured_edge_missing_from_model(tmp_path):
    root = make_repo(tmp_path)
    model = (root / "architecture" / "model" / "pkg.c4").read_text()
    (root / "architecture" / "model" / "pkg.c4").write_text(model.replace("  engine -> core\n", ""))
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_type_checking_import_is_not_an_edge(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_unknown_enforced_tag(tmp_path):
    root = make_repo(tmp_path)
    path = root / "architecture" / "system.arc42.md"
    path.write_text(path.read_text() + "3. Rule. [enforced: nonsense]\n", encoding="utf-8")
    assert any("nonsense" in f for f in findings_for(root, "enforced-tags"))


def test_known_enforced_tag_forms_accepted(tmp_path):
    root = make_repo(tmp_path)
    path = root / "architecture" / "system.arc42.md"
    path.write_text(
        path.read_text() + "3. A. [enforced: arch_check:model-truth]\n4. B. [enforced: test:tests/test_x.py]\n",
        encoding="utf-8",
    )
    assert findings_for(root, "enforced-tags") == []
