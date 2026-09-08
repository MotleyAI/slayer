"""tools/arch_check.py cross-walk checks against tmp-dir repo fixtures."""

import importlib.util
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location("arch_check", REPO_ROOT / "tools" / "arch_check.py")
assert _spec is not None
assert _spec.loader is not None
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


def test_aliased_typing_type_checking_attr_is_not_an_edge(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\nif t.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_attribute_target_does_not_kill_typing_alias(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\nt.cache = {}\nif t.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_tuple_del_typing_alias_still_measured(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\ndel (t,)\nif t.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_param_shadowed_typing_alias_still_measured(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\ndef f(t):\n    if t.TYPE_CHECKING:\n        import pkg.core\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_import_rebound_typing_alias_still_measured(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\nimport os as t\nif t.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_rebound_typing_alias_still_measured(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import typing as t\nt = object\nif t.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_unrelated_type_checking_attr_still_measured(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text(
        "import os\nif os.TYPE_CHECKING:\n    import pkg.core\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_baseline_slack_is_flagged(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(index.replace("baseline: 1", "baseline: 2"))
    assert findings_for(root, "baseline-ratchet")


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


def append_principles(root: Path, lines: str) -> None:
    path = root / "architecture" / "system.arc42.md"
    path.write_text(path.read_text() + lines, encoding="utf-8")


def test_malformed_enforced_tag_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [enforced]\n")
    assert any("malformed" in f for f in findings_for(root, "enforced-tags"))


def test_malformed_review_tag_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [review: DEV-1869]\n")
    assert any("malformed" in f and "review" in f for f in findings_for(root, "enforced-tags"))


def test_malformed_target_tag_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [target DEV-1841]\n")
    assert any("malformed" in f and "target" in f for f in findings_for(root, "enforced-tags"))


def test_target_tag_id_must_match_dev_number(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [target: 1841]\n")
    assert any("1841" in f for f in findings_for(root, "enforced-tags"))


def test_valid_target_tag_accepted(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Planned. [target: DEV-1841]\n")
    assert findings_for(root, "enforced-tags") == []


def test_untagged_principle_item_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Naked rule.\n")
    assert any("status tag" in f for f in findings_for(root, "enforced-tags"))


def test_mixed_tags_on_one_item_legal(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Broadcast clause. [review] Mode-axis clause. [target: DEV-1841]\n")
    assert findings_for(root, "enforced-tags") == []


def test_tag_on_continuation_line_accepted(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Wrapped rule\n   over two lines. [review]\n")
    assert findings_for(root, "enforced-tags") == []


def test_malformed_tags_in_prose_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\nProse note. [review: X]\nAnother note. [target 123]\n")
    fs = findings_for(root, "enforced-tags")
    assert any("malformed" in f and "review" in f for f in fs)
    assert any("malformed" in f and "target" in f for f in fs)


def test_malformed_tag_is_not_status_coverage(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. A. [review: DEV-1869]\n4. B. [target DEV-1841]\n")
    assert sum("status tag" in f for f in findings_for(root, "enforced-tags")) == 2


def test_tag_on_next_item_does_not_cover_previous(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Naked rule.\n4. Tagged rule. [review]\n")
    fs = findings_for(root, "enforced-tags")
    assert sum("status tag" in f for f in fs) == 1
    assert any("status tag" in f and "3" in f for f in fs)


def test_invalid_target_id_is_not_status_coverage(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [target: 1841]\n")
    assert any("status tag" in f for f in findings_for(root, "enforced-tags"))


def test_unknown_enforced_id_is_not_status_coverage(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [enforced: nonsense]\n")
    assert any("status tag" in f for f in findings_for(root, "enforced-tags"))


def test_fenced_code_blocks_ignored(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n```text\n3. not a principle\n[target 999]\n```\n")
    assert findings_for(root, "enforced-tags") == []


def test_orphan_arc42_file_flagged(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "rogue.arc42.md").write_text("# rogue\n", encoding="utf-8")
    assert any("rogue" in f for f in findings_for(root, "arc42-exists"))


def test_cross_cutting_arc42_file_accepted(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "semantics.arc42.md").write_text("# semantics\n", encoding="utf-8")
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(
        index + "cross_cutting_arc42: [architecture/semantics.arc42.md]\n"
    )
    assert findings_for(root, "arc42-exists") == []


def test_cross_cutting_arc42_missing_file_flagged(tmp_path):
    root = make_repo(tmp_path)
    index = (root / "architecture" / "index.yaml").read_text()
    (root / "architecture" / "index.yaml").write_text(
        index + "cross_cutting_arc42: [architecture/ghost.arc42.md]\n"
    )
    assert any("ghost" in f for f in findings_for(root, "arc42-exists"))
