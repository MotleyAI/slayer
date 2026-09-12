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

INDEX = """
root_package: pkg
nodes:
  core: {package: pkg.core}
  engine: {package: pkg.engine, arc42: architecture/engine.arc42.md}
legacy_arrows: {baseline: 1}
cross_cutting_specs:
  queries: {touches: [core, engine]}
diagrams:
  architecture/system.arc42.md: [land]
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

VIEWS = """
views {
  view land {
    title 'Landscape'
    include *
  }
}
"""

SYSTEM_MD = (
    "# System\n\n1. One law. [enforced: arch_check:model-truth]\n2. Soft rule. [review]\n\n"
    "<!-- likec4:land -->\n<!-- /likec4:land -->\n"
)

BASE_FILES = {
    "architecture/index.yaml": INDEX,
    "architecture/model/pkg.c4": MODEL,
    "architecture/views.c4": VIEWS,
    "architecture/system.arc42.md": SYSTEM_MD,
    "architecture/engine.arc42.md": "# engine\n",
    "openspec/specs/queries/foo/spec.md": "# spec\n",
    "pkg/__init__.py": "",
    "pkg/core/__init__.py": "",
    "pkg/core/a.py": "from pkg.engine import b\n",
    "pkg/engine/__init__.py": "",
    "pkg/engine/b.py": "import pkg.core\n",
}

CHILD_INDEX = """
root_package: pkg
nodes:
  core:
    package: pkg.core
    children: [query, models]
  engine:
    package: pkg.engine
    children: [syntax]
    arc42: architecture/engine.arc42.md
legacy_arrows: {baseline: 1}
cross_cutting_specs:
  queries: {touches: [core, engine]}
diagrams:
  architecture/system.arc42.md: [land]
"""

CHILD_MODEL = """
specification {
  element node
  tag legacy
}
model {
  core = node 'Core' {
    query = node 'Query'
    models = node 'Models'
  }
  engine = node 'Engine' {
    syntax = node 'Syntax'
  }
  core.query -> engine.syntax #legacy
  core.query -> core.models
  engine -> core
}
"""

CHILD_FILES = {
    "architecture/index.yaml": CHILD_INDEX,
    "architecture/model/pkg.c4": CHILD_MODEL,
    "architecture/views.c4": VIEWS,
    "architecture/system.arc42.md": SYSTEM_MD,
    "architecture/engine.arc42.md": "# engine\n",
    "openspec/specs/queries/foo/spec.md": "# spec\n",
    "pkg/__init__.py": "",
    "pkg/core/__init__.py": "",
    "pkg/core/query.py": "from pkg.engine.syntax import parse\nfrom pkg.core.models import Model\n",
    "pkg/core/models.py": "",
    "pkg/engine/__init__.py": "",
    "pkg/engine/syntax.py": "parse = 1\n",
    "pkg/engine/b.py": "import pkg.core\n",
}

GRAND_INDEX = """
root_package: pkg
nodes:
  core:
    package: pkg.core
    children: [query]
  engine:
    package: pkg.engine
    children: [syntax, syntax.deep]
    arc42: architecture/engine.arc42.md
legacy_arrows: {baseline: 0}
cross_cutting_specs:
  queries: {touches: [core, engine]}
diagrams:
  architecture/system.arc42.md: [land]
"""

GRAND_MODEL = """
specification {
  element node
  tag legacy
}
model {
  core = node 'Core' {
    query = node 'Query'
  }
  engine = node 'Engine' {
    syntax = node 'Syntax' {
      deep = node 'Deep'
    }
  }
  core.query -> engine.syntax.deep
  engine -> core
}
"""

GRAND_FILES = {
    "architecture/index.yaml": GRAND_INDEX,
    "architecture/model/pkg.c4": GRAND_MODEL,
    "architecture/views.c4": VIEWS,
    "architecture/system.arc42.md": SYSTEM_MD,
    "architecture/engine.arc42.md": "# engine\n",
    "openspec/specs/queries/foo/spec.md": "# spec\n",
    "pkg/__init__.py": "",
    "pkg/core/__init__.py": "",
    "pkg/core/query.py": "import pkg.engine.syntax.deep\n",
    "pkg/engine/__init__.py": "",
    "pkg/engine/syntax/__init__.py": "",
    "pkg/engine/syntax/deep.py": "",
    "pkg/engine/syntax/shallow.py": "",
    "pkg/engine/b.py": "import pkg.core\n",
}


def write_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "repo"
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content), encoding="utf-8")
    arch_check.arch_diagrams.generate(root)  # fill the diagram marker block so the fixture is fresh
    return root


def make_repo(tmp_path: Path) -> Path:
    return write_repo(tmp_path, BASE_FILES)


def make_child_repo(tmp_path: Path, *, index: str = CHILD_INDEX, model: str = CHILD_MODEL) -> Path:
    files = {**CHILD_FILES, "architecture/index.yaml": index, "architecture/model/pkg.c4": model}
    return write_repo(tmp_path, files)


def make_grandchild_repo(tmp_path: Path) -> Path:
    return write_repo(tmp_path, GRAND_FILES)


def findings_for(root: Path, check_id: str) -> list[str]:
    return [f for f in arch_check.run_checks(root) if f.startswith(f"{check_id}:")]


def edit(root: Path, rel: str, old: str, new: str) -> None:
    path = root / rel
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{rel}: edit target not found: {old!r}"
    path.write_text(text.replace(old, new), encoding="utf-8")


def append(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")


# --------------------------------------------------------------------------- node-level fixture (reduction)


def test_healthy_fixture_passes(tmp_path):
    assert arch_check.run_checks(make_repo(tmp_path)) == []


def test_import_linter_plumbing_dropped():
    for symbol in ("_pyproject_importlinter", "_check_contracts"):
        assert not hasattr(arch_check, symbol)
    assert "contracts-known" not in arch_check.CHECK_IDS


def test_unclaimed_top_level_module(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "extra.py").write_text("", encoding="utf-8")
    assert any("pkg.extra" in f for f in findings_for(root, "claims-exactly-once"))


def test_duplicate_claim(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "core: {package: pkg.core}", "core: {package: pkg.core, claims: [pkg.engine]}")
    assert any("pkg.engine" in f for f in findings_for(root, "claims-exactly-once"))


def test_claimed_module_missing_on_disk(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "core: {package: pkg.core}", "core: {package: pkg.core, claims: [pkg.ghost]}")
    assert any("pkg.ghost" in f for f in findings_for(root, "claims-exist"))


def test_missing_arc42_file(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "engine.arc42.md").unlink()
    assert findings_for(root, "arc42-exists")


def test_unknown_touches_node(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "[core, engine]", "[core, ghost]")
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


def test_node_missing_from_model(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/model/pkg.c4", "engine = node 'Engine'\n", "")
    assert any("engine" in f for f in findings_for(root, "model-identity"))


def test_extra_element_in_model(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/model/pkg.c4", "engine = node 'Engine'", "engine = node 'Engine'\n  ghost = node 'Ghost'")
    assert any("ghost" in f for f in findings_for(root, "model-identity"))


def test_modeled_relation_without_measured_edge(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text("", encoding="utf-8")
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_measured_edge_missing_from_model(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/model/pkg.c4", "  engine -> core\n", "")
    assert any("engine -> core" in f for f in findings_for(root, "model-truth"))


def test_missing_edge_finding_names_module_witness(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/model/pkg.c4", "  engine -> core\n", "")
    assert (
        "model-truth: measured runtime edge engine -> core is missing from the model"
        " (import pkg.engine.b -> pkg.core)"
    ) in arch_check.run_checks(root)


def test_dead_relation_finding_text_unchanged(tmp_path):
    root = make_repo(tmp_path)
    (root / "pkg" / "engine" / "b.py").write_text("", encoding="utf-8")
    assert "model-truth: modeled relation engine -> core has no measured runtime edge" in arch_check.run_checks(root)


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


# --------------------------------------------------------------------------- legacy-arrow ratchet


def test_legacy_count_above_baseline_flagged(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "baseline: 1", "baseline: 0")
    assert findings_for(root, "baseline-ratchet")


def test_legacy_count_below_baseline_flagged(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "baseline: 1", "baseline: 2")
    assert findings_for(root, "baseline-ratchet")


def test_zero_legacy_zero_baseline_green(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/model/pkg.c4", " #legacy", "")
    edit(root, "architecture/index.yaml", "baseline: 1", "baseline: 0")
    assert findings_for(root, "baseline-ratchet") == []
    assert findings_for(root, "model-truth") == []


def test_legacy_arrows_missing_is_finding(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "legacy_arrows: {baseline: 1}\n", "")
    fs = findings_for(root, "baseline-ratchet")
    assert fs
    assert any("legacy_arrows" in f for f in fs)


def test_legacy_arrows_non_integer_is_finding(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "baseline: 1", "baseline: fish")
    fs = findings_for(root, "baseline-ratchet")
    assert fs
    assert any("legacy_arrows" in f for f in fs)


def test_legacy_arrows_negative_is_finding(tmp_path):
    root = make_repo(tmp_path)
    edit(root, "architecture/index.yaml", "baseline: 1", "baseline: -1")
    fs = findings_for(root, "baseline-ratchet")
    assert fs
    assert any("legacy_arrows" in f for f in fs)


# --------------------------------------------------------------------------- children schema


def test_child_missing_on_disk_flagged(tmp_path):
    index = CHILD_INDEX.replace("children: [query, models]", "children: [query, models, ghost]")
    model = CHILD_MODEL.replace("    models = node 'Models'\n", "    models = node 'Models'\n    ghost = node 'Ghost'\n")
    root = make_child_repo(tmp_path, index=index, model=model)
    assert any("ghost" in f for f in findings_for(root, "claims-exist"))


def test_duplicate_child_flagged(tmp_path):
    index = CHILD_INDEX.replace("children: [query, models]", "children: [query, query, models]")
    root = make_child_repo(tmp_path, index=index)
    assert any("query" in f for f in findings_for(root, "claims-exactly-once"))


def test_children_on_virtual_node_flagged(tmp_path):
    index = """
    root_package: pkg
    nodes:
      core: {package: pkg.core}
      misc:
        virtual: true
        packages: [pkg.misc]
        children: [x]
    legacy_arrows: {baseline: 0}
    diagrams:
      architecture/system.arc42.md: [land]
    """
    model = """
    specification {
      element node
      element bucket {
        #virtual
      }
    }
    model {
      core = node 'Core'
      misc = bucket 'Misc' {
        x = node 'X'
      }
    }
    """
    files = {
        "architecture/index.yaml": index,
        "architecture/model/pkg.c4": model,
        "architecture/views.c4": VIEWS,
        "architecture/system.arc42.md": SYSTEM_MD,
        "pkg/__init__.py": "",
        "pkg/core/__init__.py": "",
        "pkg/misc/__init__.py": "",
        "pkg/misc/x.py": "",
    }
    root = write_repo(tmp_path, files)
    assert any("misc" in f and "virtual" in f for f in findings_for(root, "claims-exist"))


def test_index_child_missing_from_model_flagged(tmp_path):
    model = CHILD_MODEL.replace("    models = node 'Models'\n", "").replace("  core.query -> core.models\n", "")
    root = make_child_repo(tmp_path, model=model)
    assert any("core.models" in f for f in findings_for(root, "model-identity"))


def test_model_child_unknown_to_index_flagged(tmp_path):
    model = CHILD_MODEL.replace("    models = node 'Models'\n", "    models = node 'Models'\n    extra = node 'Extra'\n")
    root = make_child_repo(tmp_path, model=model)
    assert any("core.extra" in f for f in findings_for(root, "model-identity"))


# --------------------------------------------------------------------------- law: attribution


def test_child_healthy_fixture_passes(tmp_path):
    assert arch_check.run_checks(make_child_repo(tmp_path)) == []


def test_grandchild_healthy_fixture_passes(tmp_path):
    assert arch_check.run_checks(make_grandchild_repo(tmp_path)) == []


def test_longest_prefix_attribution(tmp_path):
    root = make_grandchild_repo(tmp_path)
    (root / "pkg" / "core" / "query.py").write_text("import pkg.engine.syntax.shallow\n", encoding="utf-8")
    fs = findings_for(root, "model-truth")
    assert any("measured runtime edge core.query -> engine.syntax is missing" in f for f in fs)
    assert any("modeled relation core.query -> engine.syntax.deep has no measured runtime edge" in f for f in fs)


def test_undeclared_module_attributes_to_its_node(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "engine" / "other.py").write_text("", encoding="utf-8")
    append(root, "pkg/core/query.py", "import pkg.engine.other\n")
    fs = findings_for(root, "model-truth")
    assert any("measured runtime edge core.query -> engine is missing" in f for f in fs)
    assert any("(import pkg.core.query -> pkg.engine.other)" in f for f in fs)


def test_prefix_attribution_respects_segment_boundaries(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "engine" / "syntaxx.py").write_text("", encoding="utf-8")
    append(root, "pkg/core/models.py", "import pkg.engine.syntaxx\n")
    fs = findings_for(root, "model-truth")
    assert any("core.models -> engine is missing" in f for f in fs)
    assert not any("core.models -> engine.syntax is missing" in f for f in fs)


def test_relative_imports_resolve(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "core" / "query.py").write_text(
        "from ..engine.syntax import parse\nfrom .models import Model\nfrom . import models\n", encoding="utf-8"
    )
    assert findings_for(root, "model-truth") == []


def test_from_package_import_submodule_measures_both_edges(tmp_path):
    root = make_child_repo(tmp_path)
    append(root, "pkg/core/models.py", "from pkg.engine import syntax\n")
    fs = findings_for(root, "model-truth")
    assert any("core.models -> engine is missing" in f for f in fs)
    assert any("core.models -> engine.syntax is missing" in f for f in fs)


def test_reexported_attribute_resolves_like_the_submodule(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "engine" / "__init__.py").write_text("syntax = 1\n", encoding="utf-8")
    append(root, "pkg/core/models.py", "from pkg.engine import syntax\n")
    assert any("core.models -> engine.syntax is missing" in f for f in findings_for(root, "model-truth"))


def test_type_checking_exclusion_at_child_level(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "core" / "query.py").write_text(
        "from typing import TYPE_CHECKING\nfrom pkg.core.models import Model\n"
        "if TYPE_CHECKING:\n    from pkg.engine.syntax import parse\n",
        encoding="utf-8",
    )
    assert any(
        "modeled relation core.query -> engine.syntax has no measured runtime edge" in f
        for f in findings_for(root, "model-truth")
    )


def test_root_init_exempt(tmp_path):
    root = make_child_repo(tmp_path)
    (root / "pkg" / "__init__.py").write_text("from pkg.engine.syntax import parse\n", encoding="utf-8")
    assert findings_for(root, "model-truth") == []


# --------------------------------------------------------------------------- law: coverage


def test_sibling_child_edge_needs_arrow(tmp_path):
    model = CHILD_MODEL.replace("  core.query -> core.models\n", "")
    root = make_child_repo(tmp_path, model=model)
    fs = findings_for(root, "model-truth")
    assert any("measured runtime edge core.query -> core.models is missing" in f for f in fs)
    assert any("(import pkg.core.query -> pkg.core.models)" in f for f in fs)


def test_ancestor_descendant_edges_internal(tmp_path):
    root = make_child_repo(tmp_path)
    append(root, "pkg/core/query.py", "import pkg.core\n")
    (root / "pkg" / "core" / "util.py").write_text("import pkg.core.query\n", encoding="utf-8")
    append(root, "pkg/engine/__init__.py", "from pkg.engine import syntax as _syntax\n")
    assert findings_for(root, "model-truth") == []


def test_parent_arrow_covers_child_cross_node_edges(tmp_path):
    root = make_child_repo(tmp_path)
    append(root, "pkg/engine/b.py", "import pkg.core.query\nimport pkg.core.models\n")
    assert findings_for(root, "model-truth") == []


def test_specific_and_parent_arrows_both_live(tmp_path):
    model = CHILD_MODEL.replace("  engine -> core\n", "  engine -> core\n  engine -> core.query\n")
    root = make_child_repo(tmp_path, model=model)
    append(root, "pkg/engine/b.py", "import pkg.core.query\n")
    assert findings_for(root, "model-truth") == []


def test_fully_shadowed_arrow_is_dead(tmp_path):
    model = CHILD_MODEL.replace("  engine -> core\n", "  engine -> core\n  engine -> core.query\n")
    root = make_child_repo(tmp_path, model=model)
    (root / "pkg" / "engine" / "b.py").write_text("import pkg.core.query\n", encoding="utf-8")
    fs = findings_for(root, "model-truth")
    assert len(fs) == 1
    assert "modeled relation engine -> core is fully shadowed" in fs[0]


def test_incomparable_covers_both_live(tmp_path):
    model = CHILD_MODEL.replace(
        "  core.query -> engine.syntax #legacy\n", "  core.query -> engine\n  core -> engine.syntax\n"
    )
    index = CHILD_INDEX.replace("baseline: 1", "baseline: 0")
    root = make_child_repo(tmp_path, index=index, model=model)
    assert findings_for(root, "model-truth") == []
    assert findings_for(root, "baseline-ratchet") == []


def test_dead_child_arrow_flagged(tmp_path):
    model = CHILD_MODEL.replace("  engine -> core\n", "  engine -> core\n  core.models -> engine.syntax\n")
    root = make_child_repo(tmp_path, model=model)
    assert any(
        "modeled relation core.models -> engine.syntax has no measured runtime edge" in f
        for f in findings_for(root, "model-truth")
    )


def test_internal_arrow_rejected_and_does_not_license_sibling(tmp_path):
    # A parent->child arrow is internal: it must be flagged and must NOT cover the sibling edge.
    model = CHILD_MODEL.replace("  core.query -> core.models\n", "  core -> core.models\n")
    root = make_child_repo(tmp_path, model=model)
    fs = findings_for(root, "model-truth")
    assert any("core -> core.models" in f and "ancestor or descendant" in f for f in fs)
    assert any("measured runtime edge core.query -> core.models is missing" in f for f in fs)


def test_internal_arrow_not_licensed_by_license_helper(tmp_path):
    model = CHILD_MODEL.replace("  core.query -> core.models\n", "  core -> core.models\n")
    root = make_child_repo(tmp_path, model=model)
    assert not arch_check.license(root=root, src="pkg.core.query", dst="pkg.core.models")


def test_child_path_colliding_with_claim_flagged(tmp_path):
    index = CHILD_INDEX.replace(
        "    children: [syntax]\n    arc42: architecture/engine.arc42.md",
        "    children: [syntax]\n    claims: [pkg.core.query]\n    arc42: architecture/engine.arc42.md",
    )
    root = make_child_repo(tmp_path, index=index)
    assert any("collides" in f and "pkg.core.query" in f for f in findings_for(root, "claims-exactly-once"))


def test_child_path_nested_under_claim_flagged(tmp_path):
    """Hierarchical overlap, not just exact: a claim nested inside a declared child's subtree
    still splits it across nodes, since `_attribute` resolves by longest prefix."""
    index = CHILD_INDEX.replace(
        "    children: [syntax]\n    arc42: architecture/engine.arc42.md",
        "    children: [syntax]\n    claims: [pkg.core.query.helpers]\n    arc42: architecture/engine.arc42.md",
    )
    root = make_child_repo(tmp_path, index=index)
    assert any("collides" in f and "pkg.core.query" in f for f in findings_for(root, "claims-exactly-once"))


# --------------------------------------------------------------------------- license helper


def test_license_helper_child_fixture(tmp_path):
    root = make_child_repo(tmp_path)
    assert arch_check.license(root=root, src="pkg.core.query", dst="pkg.engine.syntax")
    assert not arch_check.license(root=root, src="pkg.core.models", dst="pkg.engine.syntax")
    assert not arch_check.license(root=root, src="pkg.core.query", dst="pkg.engine.b")
    assert arch_check.license(root=root, src="pkg.engine.b", dst="pkg.core.query")
    assert arch_check.license(root=root, src="pkg.engine.syntax", dst="pkg.core.models")


# --------------------------------------------------------------------------- enforced-tags


def test_unknown_enforced_tag(tmp_path):
    root = make_repo(tmp_path)
    append(root, "architecture/system.arc42.md", "3. Rule. [enforced: nonsense]\n")
    assert any("nonsense" in f for f in findings_for(root, "enforced-tags"))


def test_contract_ids_no_longer_valid_enforcement(tmp_path):
    root = make_repo(tmp_path)
    append(root, "architecture/system.arc42.md", "3. Old law. [enforced: layers]\n")
    assert any("layers" in f for f in findings_for(root, "enforced-tags"))


def test_known_enforced_tag_forms_accepted(tmp_path):
    root = make_repo(tmp_path)
    append(
        root,
        "architecture/system.arc42.md",
        "3. A. [enforced: arch_check:model-truth]\n4. B. [enforced: test:tests/test_x.py]\n",
    )
    assert findings_for(root, "enforced-tags") == []


def append_principles(root: Path, lines: str) -> None:
    append(root, "architecture/system.arc42.md", lines)


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


def test_indented_untagged_principle_item_flagged(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "  3. Indented naked rule.\n")
    assert any("status tag" in f for f in findings_for(root, "enforced-tags"))


def test_three_space_numbered_line_is_continuation(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule with nested steps. [review]\n   1. nested step\n")
    assert findings_for(root, "enforced-tags") == []


def test_indented_sibling_item_is_not_continuation(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n   3. Tagged rule. [review]\n   4. Naked sibling.\n")
    assert any("status tag" in f and "4" in f for f in findings_for(root, "enforced-tags"))


def test_tag_spanning_lines_is_malformed(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [enforced: test:tests/a.py\ntest:tests/b.py]\n")
    assert any("malformed" in f for f in findings_for(root, "enforced-tags"))


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


def test_empty_test_id_is_not_status_coverage(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "3. Rule. [enforced: test:]\n")
    assert any("status tag" in f for f in findings_for(root, "enforced-tags"))


def test_fenced_code_blocks_ignored(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n```text\n3. not a principle\n[target 999]\n```\n")
    assert findings_for(root, "enforced-tags") == []


def test_tilde_fenced_code_blocks_ignored(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n~~~text\n3. not a principle\n[target 999]\n~~~\n")
    assert findings_for(root, "enforced-tags") == []


def test_longer_fence_swallows_inner_backtick_fence(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n````md\n```\n3. not a principle\n[target 999]\n````\n")
    assert findings_for(root, "enforced-tags") == []


def test_fence_line_with_info_string_is_not_a_closer(tmp_path):
    root = make_repo(tmp_path)
    append_principles(root, "\n```\n```python\n3. not a principle\n[target 999]\n```\n")
    assert findings_for(root, "enforced-tags") == []


def test_orphan_arc42_file_flagged(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "rogue.arc42.md").write_text("# rogue\n", encoding="utf-8")
    assert any("rogue" in f for f in findings_for(root, "arc42-exists"))


def test_cross_cutting_arc42_file_accepted(tmp_path):
    root = make_repo(tmp_path)
    (root / "architecture" / "semantics.arc42.md").write_text("# semantics\n", encoding="utf-8")
    append(root, "architecture/index.yaml", "cross_cutting_arc42: [architecture/semantics.arc42.md]\n")
    assert findings_for(root, "arc42-exists") == []


def test_cross_cutting_arc42_missing_file_flagged(tmp_path):
    root = make_repo(tmp_path)
    append(root, "architecture/index.yaml", "cross_cutting_arc42: [architecture/ghost.arc42.md]\n")
    assert any("ghost" in f for f in findings_for(root, "arc42-exists"))


# --------------------------------------------------------------------------- real repo


def test_repo_arch_check_green():
    assert arch_check.run_checks(REPO_ROOT) == []


def test_repo_pyproject_has_no_importlinter():
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "importlinter" not in pyproject
    assert "import-linter" not in pyproject


def test_repo_ci_runs_arch_check():
    assert "tools/arch_check.py" in (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")


def test_repo_arc42_docs_no_longer_name_lint_imports():
    for path in sorted((REPO_ROOT / "architecture").glob("*.arc42.md")):
        assert "lint-imports" not in path.read_text(encoding="utf-8"), path.name
