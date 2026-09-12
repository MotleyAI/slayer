"""tools/arch_diagrams.py generator + parser, and arch_check's diagrams-fresh check."""

import importlib.util
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "tools" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


arch_diagrams = _load("arch_diagrams")
arch_check = _load("arch_check")

FIX_CMD = "poetry run python tools/arch_diagrams.py"


def arch_only(
    tmp_path: Path, model_text: str, views_text: str | None = None, index_text: str | None = None
) -> Path:
    """Write just architecture/model + optional views/index under tmp_path, return it as root."""
    model_dir = tmp_path / "architecture" / "model"
    model_dir.mkdir(parents=True)
    (model_dir / "m.c4").write_text(textwrap.dedent(model_text), encoding="utf-8")
    if views_text is not None:
        (tmp_path / "architecture" / "views.c4").write_text(textwrap.dedent(views_text), encoding="utf-8")
    if index_text is not None:
        (tmp_path / "architecture" / "index.yaml").write_text(textwrap.dedent(index_text), encoding="utf-8")
    return tmp_path


def view_by_id(views, view_id: str):
    return next(v for v in views if v.id == view_id)


def edge_tuples(view):
    return [(e.src, e.dst, e.legacy) for e in view.edges]


# --------------------------------------------------------------------------- parse_model

BASIC_MODEL = """
specification {
  element node
  element bucket {
    #virtual
  }
  tag virtual
  tag legacy
}
model {
  a = node 'Node A'
  b = node 'Node B'
  c = bucket 'Bucket C'
  a -> b
  b -> a #legacy
  a -> c
}
"""


def test_parse_model_elements_in_declaration_order(tmp_path):
    mp = arch_diagrams.parse_model(arch_only(tmp_path, BASIC_MODEL))
    assert [e.id for e in mp.elements] == ["a", "b", "c"]
    assert [e.title for e in mp.elements] == ["Node A", "Node B", "Bucket C"]
    assert mp.findings == []


def test_parse_model_virtual_kind_flag(tmp_path):
    mp = arch_diagrams.parse_model(arch_only(tmp_path, BASIC_MODEL))
    assert {e.id: e.virtual for e in mp.elements} == {"a": False, "b": False, "c": True}


def test_parse_model_relations_and_legacy(tmp_path):
    mp = arch_diagrams.parse_model(arch_only(tmp_path, BASIC_MODEL))
    assert [(r.src, r.dst, r.legacy) for r in mp.relations] == [
        ("a", "b", False),
        ("b", "a", True),
        ("a", "c", False),
    ]


def test_parse_model_child_gets_fqn_id_and_parent(tmp_path):
    model = """
    specification { element node }
    model {
      p = node 'P' {
        kid = node 'K'
      }
      q = node 'Q'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert {e.id: e.parent for e in mp.elements} == {"p": None, "p.kid": "p", "q": None}
    assert mp.findings == []


def test_parse_model_same_leaf_name_under_two_parents_legal(tmp_path):
    model = """
    specification { element node }
    model {
      p = node 'P' {
        kid = node 'K1'
      }
      q = node 'Q' {
        kid = node 'K2'
      }
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert {e.id for e in mp.elements} == {"p", "p.kid", "q", "q.kid"}
    assert mp.findings == []


def test_parse_model_duplicate_child_in_same_parent_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      p = node 'P' {
        kid = node 'K'
        kid = node 'K again'
      }
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("p.kid" in f for f in mp.findings)


def test_parse_model_dotted_relation_endpoints(tmp_path):
    model = """
    specification { element node  tag legacy }
    model {
      p = node 'P' {
        kid = node 'K'
      }
      q = node 'Q'
      p.kid -> q #legacy
      q -> p.kid
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert [(r.src, r.dst, r.legacy) for r in mp.relations] == [("p.kid", "q", True), ("q", "p.kid", False)]
    assert mp.findings == []


def test_parse_model_unknown_dotted_endpoint_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      p = node 'P' {
        kid = node 'K'
      }
      q = node 'Q'
      p.ghost -> q
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("p.ghost" in f for f in mp.findings)


def test_parse_model_local_child_name_endpoint_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      p = node 'P' {
        kid = node 'K'
      }
      q = node 'Q'
      kid -> q
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("kid" in f for f in mp.findings)


def test_parse_model_unrecognized_model_line_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A'
      total garbage here
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert mp.findings


def test_parse_model_unrecognized_specification_line_is_finding(tmp_path):
    model = """
    specification {
      element node
      total junk here
    }
    model {
      a = node 'A'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert mp.findings


def test_parse_model_relation_in_element_body_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A' {
        a -> b
      }
      b = node 'B'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert mp.findings


def test_parse_model_duplicate_element_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A'
      a = node 'A again'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("a" in f for f in mp.findings)


def test_parse_model_duplicate_relation_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A'
      b = node 'B'
      a -> b
      a -> b
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert mp.findings


def test_parse_model_unknown_relation_endpoint_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A'
      a -> ghost
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("ghost" in f for f in mp.findings)


def test_parse_model_undeclared_kind_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = widget 'A'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("widget" in f for f in mp.findings)


def test_parse_model_brace_in_title_does_not_corrupt_nesting(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'Has } a brace'
      b = node 'B'
      a -> b
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert [e.id for e in mp.elements] == ["a", "b"]
    assert [(r.src, r.dst) for r in mp.relations] == [("a", "b")]
    assert mp.findings == []


def test_parse_model_comment_brace_is_ignored(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A'
      // a stray } in a comment
      b = node 'B'
      a -> b
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert [e.id for e in mp.elements] == ["a", "b"]
    assert mp.findings == []


def test_parse_model_element_trailing_content_is_finding(tmp_path):
    model = """
    specification { element node }
    model {
      a = node 'A' unexpected junk
      b = node 'B'
    }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("unexpected junk" in f for f in mp.findings)


def test_parse_model_non_virtual_spec_tag_is_finding(tmp_path):
    model = """
    specification {
      element node {
        #bogus
      }
    }
    model { a = node 'A' }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("bogus" in f for f in mp.findings)


def test_parse_model_spec_element_trailing_content_is_finding(tmp_path):
    model = """
    specification {
      element node oops
    }
    model { a = node 'A' }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("oops" in f for f in mp.findings)


def test_parse_model_double_hash_virtual_is_finding(tmp_path):
    model = """
    specification {
      element bucket {
        ##virtual
      }
    }
    model { a = bucket 'A' }
    """
    mp = arch_diagrams.parse_model(arch_only(tmp_path, model))
    assert any("##virtual" in f for f in mp.findings)
    assert {e.id: e.virtual for e in mp.elements} == {"a": False}


# --------------------------------------------------------------------------- parse_views

CHAIN_MODEL = """
specification { element node }
model {
  x = node 'X'
  y = node 'Y'
  z = node 'Z'
  x -> y
  y -> z
  z -> x
}
"""

STAR_MODEL = """
specification { element node }
model {
  x = node 'X'
  y = node 'Y'
  z = node 'Z'
  x -> y
  x -> z
  y -> z
}
"""

CHILD_MODEL = """
specification { element node }
model {
  p = node 'P' {
    kid = node 'K'
  }
  q = node 'Q'
  p -> q
}
"""


def parsed_view(tmp_path, model_text, views_text, view_id):
    root = arch_only(tmp_path, model_text, views_text)
    vp = arch_diagrams.parse_views(root, arch_diagrams.parse_model(root))
    return view_by_id(vp.views, view_id), vp.findings


def test_parse_views_include_star(tmp_path):
    v, findings = parsed_view(tmp_path, BASIC_MODEL, "views { view v { title 'V' include * } }", "v")
    assert v.node_ids == ["a", "b", "c"]
    assert edge_tuples(v) == [("a", "b", False), ("b", "a", True), ("a", "c", False)]
    assert findings == []


def test_parse_views_star_shows_children_to_depth(tmp_path):
    v, _ = parsed_view(tmp_path, CHILD_MODEL, "views { view v { title 'V' include * } }", "v")
    assert v.node_ids == ["p", "p.kid", "q"]
    assert edge_tuples(v) == [("p", "q", False)]


def test_parse_views_include_listed_edges_among_only(tmp_path):
    v, _ = parsed_view(tmp_path, BASIC_MODEL, "views { view v { title 'V' include a, b } }", "v")
    assert v.node_ids == ["a", "b"]
    assert edge_tuples(v) == [("a", "b", False), ("b", "a", True)]


def test_parse_views_src_star_predicate(tmp_path):
    v, _ = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include x, x -> * } }", "v")
    assert v.node_ids == ["x", "y"]
    assert edge_tuples(v) == [("x", "y", False)]


def test_parse_views_star_dst_predicate(tmp_path):
    v, _ = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include x, * -> x } }", "v")
    assert v.node_ids == ["x", "z"]
    assert edge_tuples(v) == [("z", "x", False)]


def test_parse_views_focus_excludes_neighbor_to_neighbor_edges(tmp_path):
    v, _ = parsed_view(tmp_path, STAR_MODEL, "views { view v { title 'V' include x, x -> * } }", "v")
    assert v.node_ids == ["x", "y", "z"]
    assert edge_tuples(v) == [("x", "y", False), ("x", "z", False)]
    assert ("y", "z", False) not in edge_tuples(v)


def test_parse_views_stable_dedup_on_repeated_includes(tmp_path):
    views = """
    views {
      view v {
        title 'V'
        include x, x -> *
        include x
        include x -> *
      }
    }
    """
    v, _ = parsed_view(tmp_path, STAR_MODEL, views, "v")
    assert v.node_ids == ["x", "y", "z"]
    assert edge_tuples(v) == [("x", "y", False), ("x", "z", False)]


def test_parse_views_multiline_union_equals_single_line(tmp_path):
    views = """
    views {
      view oneline { title 'T' include x, x -> * }
      view multiline {
        title 'T'
        include x
        include x -> *
      }
    }
    """
    root = arch_only(tmp_path, CHAIN_MODEL, views)
    vp = arch_diagrams.parse_views(root, arch_diagrams.parse_model(root))
    one = view_by_id(vp.views, "oneline")
    multi = view_by_id(vp.views, "multiline")
    assert multi.node_ids == one.node_ids
    assert edge_tuples(multi) == edge_tuples(one)


def test_parse_views_unknown_id_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include ghost } }", "v")
    assert any("ghost" in f for f in findings)


def test_parse_views_local_child_name_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHILD_MODEL, "views { view v { title 'V' include kid } }", "v")
    assert any("kid" in f for f in findings)


def test_parse_views_child_fqn_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHILD_MODEL, "views { view v { title 'V' include p.kid } }", "v")
    assert any("p.kid" in f for f in findings)


def test_parse_views_unknown_predicate_anchor_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include ghost -> * } }", "v")
    assert any("ghost" in f for f in findings)


def test_parse_views_child_predicate_anchor_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHILD_MODEL, "views { view v { title 'V' include p.kid -> * } }", "v")
    assert any("p.kid" in f for f in findings)


def test_parse_views_unsupported_predicate_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include x -> y } }", "v")
    assert findings


def test_parse_views_star_to_star_is_finding(tmp_path):
    _, findings = parsed_view(tmp_path, CHAIN_MODEL, "views { view v { title 'V' include * -> * } }", "v")
    assert findings


def test_parse_views_unrecognized_line_is_finding(tmp_path):
    views = """
    views {
      view v {
        title 'V'
        include *
        bogus directive
      }
    }
    """
    _, findings = parsed_view(tmp_path, CHAIN_MODEL, views, "v")
    assert findings


def test_parse_views_duplicate_view_id_is_finding(tmp_path):
    views = """
    views {
      view dup { title 'One' include * }
      view dup { title 'Two' include * }
    }
    """
    root = arch_only(tmp_path, CHAIN_MODEL, views)
    vp = arch_diagrams.parse_views(root, arch_diagrams.parse_model(root))
    assert any("dup" in f for f in vp.findings)


# --------------------------------------------------------------------------- mermaid emission

EMIT_VIEWS = """
views {
  view whole {
    title 'Whole thing'
    include *
  }
  view focus {
    title 'A focus'
    include a, a -> *
  }
}
"""


def render(root: Path, view_id: str) -> str:
    model = arch_diagrams.parse_model(root)
    views = arch_diagrams.parse_views(root, model)
    return arch_diagrams.render_mermaid(view_by_id(views.views, view_id), model)


def test_render_mermaid_shapes_edges_and_legend(tmp_path):
    root = arch_only(tmp_path, BASIC_MODEL, EMIT_VIEWS)
    expected = "\n".join(
        [
            "```mermaid",
            "flowchart TD",
            "  %% whole: Whole thing",
            '  a["Node A"]',
            '  b["Node B"]',
            '  c("Bucket C")',
            "  a --> b",
            "  b -.-> a",
            "  a --> c",
            "```",
            "*Dashed arrows: legacy edges slated to die.*",
        ]
    )
    assert render(root, "whole") == expected


def test_render_mermaid_no_legend_when_no_legacy_edge(tmp_path):
    root = arch_only(tmp_path, BASIC_MODEL, EMIT_VIEWS)
    expected = "\n".join(
        [
            "```mermaid",
            "flowchart TD",
            "  %% focus: A focus",
            '  a["Node A"]',
            '  b["Node B"]',
            '  c("Bucket C")',
            "  a --> b",
            "  a --> c",
            "```",
        ]
    )
    assert render(root, "focus") == expected


def test_render_mermaid_escapes_double_quote_in_title(tmp_path):
    model = """
    specification { element node }
    model {
      x = node 'Say "hi"'
    }
    """
    root = arch_only(tmp_path, model, "views { view v { title 'V' include * } }")
    expected = "\n".join(
        [
            "```mermaid",
            "flowchart TD",
            "  %% v: V",
            '  x["Say #quot;hi#quot;"]',
            "```",
        ]
    )
    assert render(root, "v") == expected


# --------------------------------------------------------------------------- fixture repo for generate + diagrams-fresh

PYPROJECT = """
[tool.poetry]
name = "fixture"
"""

INDEX_BASE = """
root_package: pkg
nodes:
  core: {package: pkg.core}
  engine: {package: pkg.engine, arc42: architecture/engine.arc42.md}
legacy_arrows: {baseline: 1}
cross_cutting_specs:
  queries: {touches: [core, engine]}
"""

DIAGRAMS_ONE = "diagrams:\n  architecture/system.arc42.md: [land]\n"
INDEX = INDEX_BASE + DIAGRAMS_ONE

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

SYSTEM_MD = """
# System

1. One law. [enforced: arch_check:model-truth]
2. Soft rule. [review]

## Diagrams

<!-- likec4:land -->
<!-- /likec4:land -->
"""

SYSTEM_MD_NO_MARKERS = """
# System

1. One law. [enforced: arch_check:model-truth]
2. Soft rule. [review]
"""

ENGINE_MD = "# engine\n"


def index_with(diagrams_block: str) -> str:
    return INDEX_BASE + diagrams_block


def make_repo(
    tmp_path: Path,
    *,
    regenerate: bool = True,
    index: str = INDEX,
    model: str = MODEL,
    views: str = VIEWS,
    system_md: str = SYSTEM_MD,
    engine_md: str = ENGINE_MD,
) -> Path:
    root = tmp_path / "repo"
    files = {
        "pyproject.toml": PYPROJECT,
        "architecture/index.yaml": index,
        "architecture/model/pkg.c4": model,
        "architecture/views.c4": views,
        "architecture/system.arc42.md": system_md,
        "architecture/engine.arc42.md": engine_md,
        "openspec/specs/queries/foo/spec.md": "# spec\n",
        "pkg/__init__.py": "",
        "pkg/core/__init__.py": "",
        "pkg/core/a.py": "from pkg.engine import b\n",
        "pkg/engine/__init__.py": "",
        "pkg/engine/b.py": "import pkg.core\n",
    }
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content), encoding="utf-8")
    if regenerate:
        arch_diagrams.generate(root)
    return root


def fresh_findings(root: Path) -> list[str]:
    return [f for f in arch_check.run_checks(root) if f.startswith("diagrams-fresh:")]


def between(text: str, view_id: str) -> str:
    return text.split(f"<!-- likec4:{view_id} -->", 1)[1].split(f"<!-- /likec4:{view_id} -->", 1)[0]


# --------------------------------------------------------------------------- generate / marker rewriting


def test_generate_fills_empty_markers(tmp_path):
    root = make_repo(tmp_path, regenerate=False)
    doc = root / "architecture" / "system.arc42.md"
    assert "flowchart TD" not in doc.read_text(encoding="utf-8")
    arch_diagrams.generate(root)
    text = doc.read_text(encoding="utf-8")
    assert "flowchart TD" in text
    assert "core -.-> engine" in text
    assert "engine --> core" in text
    assert "*Dashed arrows: legacy edges slated to die.*" in text


def test_generate_preserves_surrounding_bytes(tmp_path):
    root = make_repo(tmp_path, regenerate=False)
    doc = root / "architecture" / "system.arc42.md"
    before = doc.read_bytes()
    open_m, close_m = b"<!-- likec4:land -->", b"<!-- /likec4:land -->"
    pre, post = before.split(open_m)[0], before.split(close_m)[1]
    arch_diagrams.generate(root)
    after = doc.read_bytes()
    assert after.split(open_m)[0] == pre
    assert after.split(close_m)[1] == post


def test_generate_reports_changed_doc_then_is_idempotent(tmp_path):
    root = make_repo(tmp_path, regenerate=False)
    changed = arch_diagrams.generate(root)
    assert any("system.arc42.md" in c for c in changed)
    assert arch_diagrams.generate(root) == []


def test_generate_writes_lf_newlines(tmp_path):
    root = make_repo(tmp_path, regenerate=True)
    assert b"\r" not in (root / "architecture" / "system.arc42.md").read_bytes()


VIEWS_TWO = """
views {
  view land {
    title 'Landscape'
    include *
  }
  view cview {
    title 'Core view'
    include core, core -> *
  }
}
"""

SYSTEM_MD_TWO = """
# System

1. One law. [enforced: arch_check:model-truth]
2. Soft rule. [review]

## Diagrams

<!-- likec4:land -->
<!-- /likec4:land -->

<!-- likec4:cview -->
<!-- /likec4:cview -->
"""


def test_generate_fills_correct_block_per_view(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: [land, cview]\n")
    root = make_repo(tmp_path, regenerate=True, index=index, views=VIEWS_TWO, system_md=SYSTEM_MD_TWO)
    text = (root / "architecture" / "system.arc42.md").read_text(encoding="utf-8")
    assert "%% land: Landscape" in between(text, "land")
    assert "%% cview: Core view" in between(text, "cview")
    assert "%% cview" not in between(text, "land")
    assert "%% land" not in between(text, "cview")


def test_generate_errors_on_missing_marker(tmp_path):
    root = make_repo(tmp_path, regenerate=False, system_md=SYSTEM_MD_NO_MARKERS)
    with pytest.raises(ValueError) as exc:
        arch_diagrams.generate(root)
    assert "land" in str(exc.value)


def test_generate_errors_on_duplicate_markers(tmp_path):
    dup = SYSTEM_MD + "\n<!-- likec4:land -->\n<!-- /likec4:land -->\n"
    root = make_repo(tmp_path, regenerate=False, system_md=dup)
    with pytest.raises(ValueError):
        arch_diagrams.generate(root)


def test_generate_errors_on_open_without_close(tmp_path):
    system_md = SYSTEM_MD_NO_MARKERS + "\n<!-- likec4:land -->\n"
    root = make_repo(tmp_path, regenerate=False, system_md=system_md)
    with pytest.raises(ValueError):
        arch_diagrams.generate(root)


def test_generate_errors_on_close_without_open(tmp_path):
    system_md = SYSTEM_MD_NO_MARKERS + "\n<!-- /likec4:land -->\n"
    root = make_repo(tmp_path, regenerate=False, system_md=system_md)
    with pytest.raises(ValueError):
        arch_diagrams.generate(root)


def test_generate_aborts_when_model_has_findings(tmp_path):
    bad_model = MODEL.replace("  engine -> core\n", "  engine -> core\n  total garbage here\n")
    root = make_repo(tmp_path, regenerate=False, model=bad_model)
    with pytest.raises(ValueError) as exc:
        arch_diagrams.generate(root)
    assert "total garbage here" in str(exc.value)


def test_generate_raises_on_missing_diagrams_block(tmp_path):
    root = make_repo(tmp_path, regenerate=False, index=INDEX_BASE, system_md=SYSTEM_MD_NO_MARKERS)
    with pytest.raises(ValueError) as exc:
        arch_diagrams.generate(root)
    assert "diagrams block" in str(exc.value)


def test_generate_rejects_path_traversal_doc_key(tmp_path):
    index = index_with("diagrams:\n  architecture/../evil.arc42.md: [land]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    with pytest.raises(ValueError) as exc:
        arch_diagrams.generate(root)
    assert "must be an architecture" in str(exc.value)


def test_main_prints_changed_files(tmp_path, capsys):
    root = make_repo(tmp_path, regenerate=False)
    rc = arch_diagrams.main(root)
    out = capsys.readouterr().out
    assert rc == 0
    assert "system.arc42.md" in out


# --------------------------------------------------------------------------- diagrams-fresh through run_checks


def test_diagrams_fresh_in_check_ids():
    assert "diagrams-fresh" in arch_check.CHECK_IDS


def test_diagrams_fresh_healthy_has_no_findings(tmp_path):
    assert fresh_findings(make_repo(tmp_path)) == []


def test_diagrams_fresh_stale_content_flagged_with_fix_command(tmp_path):
    root = make_repo(tmp_path)
    doc = root / "architecture" / "system.arc42.md"
    doc.write_text(doc.read_text(encoding="utf-8").replace('core["Core"]', 'core["Cor"]'), encoding="utf-8")
    findings = fresh_findings(root)
    assert findings
    assert any(FIX_CMD in f for f in findings)


def test_diagrams_fresh_model_edit_without_regen_flagged(tmp_path):
    root = make_repo(tmp_path)
    model = root / "architecture" / "model" / "pkg.c4"
    model.write_text(model.read_text(encoding="utf-8").replace("core = node 'Core'", "core = node 'Kore'"), encoding="utf-8")
    findings = fresh_findings(root)
    assert findings
    assert any(FIX_CMD in f for f in findings)


def test_diagrams_fresh_title_only_view_edit_flagged(tmp_path):
    root = make_repo(tmp_path)
    views = root / "architecture" / "views.c4"
    views.write_text(views.read_text(encoding="utf-8").replace("title 'Landscape'", "title 'Land'"), encoding="utf-8")
    findings = fresh_findings(root)
    assert findings
    assert any(FIX_CMD in f for f in findings)


def test_diagrams_fresh_missing_marker_flagged(tmp_path):
    root = make_repo(tmp_path, regenerate=False, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("land" in f for f in fresh_findings(root))


def test_diagrams_fresh_orphan_opening_marker_flagged(tmp_path):
    root = make_repo(tmp_path, engine_md="# engine\n\n<!-- likec4:land -->\n")
    assert fresh_findings(root)


def test_diagrams_fresh_orphan_closing_marker_flagged(tmp_path):
    root = make_repo(tmp_path, engine_md="# engine\n\n<!-- /likec4:land -->\n")
    assert fresh_findings(root)


def test_diagrams_fresh_duplicate_marker_pair_flagged(tmp_path):
    dup = SYSTEM_MD + "\n<!-- likec4:land -->\n<!-- /likec4:land -->\n"
    root = make_repo(tmp_path, regenerate=False, system_md=dup)
    assert any("land" in f for f in fresh_findings(root))


def test_diagrams_fresh_mapped_view_missing_flagged(tmp_path):
    root = make_repo(tmp_path, regenerate=False, index=INDEX.replace("[land]", "[ghost]"))
    assert any("ghost" in f for f in fresh_findings(root))


def test_diagrams_fresh_mapped_doc_missing_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/ghost.arc42.md: [land]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("ghost.arc42.md" in f for f in fresh_findings(root))


def test_diagrams_fresh_schema_value_not_a_list_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: land\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_schema_non_arc42_key_flagged(tmp_path):
    index = index_with("diagrams:\n  docs/foo.md: [land]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("docs/foo.md" in f for f in fresh_findings(root))


def test_diagrams_fresh_schema_empty_list_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: []\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_schema_duplicate_view_in_list_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: [land, land]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_schema_non_string_entry_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: [123]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_schema_empty_view_id_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/system.arc42.md: ['']\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_schema_malformed_container_flagged(tmp_path):
    root = make_repo(tmp_path, regenerate=False, index=index_with("diagrams: [a, b]\n"), system_md=SYSTEM_MD_NO_MARKERS)
    assert fresh_findings(root)


def test_diagrams_fresh_parse_error_is_finding_other_checks_still_run(tmp_path):
    bad_views = """
    views {
      view land {
        title 'L'
        include ghost -> ghost
      }
    }
    """
    root = make_repo(tmp_path, regenerate=False, views=bad_views)
    (root / "pkg" / "extra.py").write_text("", encoding="utf-8")  # independent failure
    findings = arch_check.run_checks(root)  # must not raise
    assert any(f.startswith("diagrams-fresh:") for f in findings)
    assert any(f.startswith("claims-exactly-once:") for f in findings)


def test_diagrams_fresh_missing_diagrams_block_flagged(tmp_path):
    root = make_repo(tmp_path, regenerate=False, index=INDEX_BASE, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("diagrams block" in f for f in fresh_findings(root))


def test_diagrams_fresh_path_traversal_key_flagged(tmp_path):
    index = index_with("diagrams:\n  architecture/../evil.arc42.md: [land]\n")
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("must be an architecture" in f for f in fresh_findings(root))


@pytest.mark.parametrize("content", [None, "", "- a\n- b\n", "just a scalar\n"])
def test_diagrams_map_fails_closed_without_raising(tmp_path, content):
    arch_dir = tmp_path / "architecture"
    arch_dir.mkdir(parents=True)
    if content is not None:
        (arch_dir / "index.yaml").write_text(content, encoding="utf-8")
    assert isinstance(arch_diagrams._diagrams_map(tmp_path), str)  # a reason string, never a raise


def test_diagrams_fresh_crlf_block_is_stale(tmp_path):
    root = make_repo(tmp_path)
    doc = root / "architecture" / "system.arc42.md"
    doc.write_bytes(doc.read_bytes().replace(b"\n", b"\r\n"))
    assert any("stale" in f for f in fresh_findings(root))


def test_diagrams_fresh_whitespace_variant_marker_flagged(tmp_path):
    root = make_repo(tmp_path)
    doc = root / "architecture" / "system.arc42.md"
    doc.write_text(doc.read_text(encoding="utf-8") + "\n<!--  likec4:land -->\n", encoding="utf-8")
    assert any("land" in f for f in fresh_findings(root))


# --------------------------------------------------------------------------- view depth, roll-up, subgraphs

CHILD_VIEW_MODEL = """
specification { element node  tag legacy }
model {
  p = node 'P' {
    kid = node 'Kid'
    kid2 = node 'Kid two'
  }
  q = node 'Q'
  r = node 'R'
  p.kid -> q #legacy
  p.kid2 -> q
  p.kid -> p.kid2
  q -> r
}
"""

DEPTH_MODEL = """
specification { element node }
model {
  p = node 'P' {
    kid = node 'Kid' {
      grand = node 'Grand' {
        leaf = node 'Leaf'
      }
    }
  }
  q = node 'Q'
  p.kid.grand.leaf -> q
}
"""

ONE_VIEW = "views { view v { title 'V' include * } }"

STYLING_PREFIXES = ("classDef", "class ", "style ", "direction ")


def structural(text: str) -> list[str]:
    """Mermaid lines minus styling/layout directives — the styling pass may tune those freely."""
    return [line for line in text.splitlines() if not line.strip().startswith(STYLING_PREFIXES)]


def depth_view(tmp_path, model_text, index_text, view_id):
    root = arch_only(tmp_path, model_text, ONE_VIEW, index_text=index_text)
    vp = arch_diagrams.parse_views(root, arch_diagrams.parse_model(root))
    return view_by_id(vp.views, view_id)


def test_view_depth_default_shows_three_levels(tmp_path):
    v, findings = parsed_view(tmp_path, DEPTH_MODEL, ONE_VIEW, "v")
    assert v.node_ids == ["p", "p.kid", "p.kid.grand", "q"]
    assert edge_tuples(v) == [("p.kid.grand", "q", False)]
    assert findings == []


def test_view_depth_override_collapses_children(tmp_path):
    v = depth_view(tmp_path, DEPTH_MODEL, "view_depth:\n  v: 2\n", "v")
    assert v.node_ids == ["p", "p.kid", "q"]
    assert edge_tuples(v) == [("p.kid", "q", False)]


def test_view_depth_one_reduces_to_node_level(tmp_path):
    v = depth_view(tmp_path, DEPTH_MODEL, "view_depth:\n  v: 1\n", "v")
    assert v.node_ids == ["p", "q"]
    assert edge_tuples(v) == [("p", "q", False)]


def test_rollup_dedupes_and_drops_self_edges(tmp_path):
    v = depth_view(tmp_path, CHILD_VIEW_MODEL, "view_depth:\n  v: 1\n", "v")
    assert v.node_ids == ["p", "q", "r"]
    assert edge_tuples(v) == [("p", "q", False), ("q", "r", False)]


def test_rollup_dashed_iff_all_contributors_legacy(tmp_path):
    model = CHILD_VIEW_MODEL.replace("  p.kid2 -> q\n", "  p.kid2 -> q #legacy\n")
    v = depth_view(tmp_path, model, "view_depth:\n  v: 1\n", "v")
    assert edge_tuples(v) == [("p", "q", True), ("q", "r", False)]


def test_listed_include_expands_children_and_scopes_edges(tmp_path):
    v, _ = parsed_view(tmp_path, CHILD_VIEW_MODEL, "views { view v { title 'V' include p, q } }", "v")
    assert v.node_ids == ["p", "p.kid", "p.kid2", "q"]
    assert edge_tuples(v) == [("p.kid", "q", True), ("p.kid2", "q", False), ("p.kid", "p.kid2", False)]


def test_focus_src_predicate_matches_by_top_ancestor(tmp_path):
    v, _ = parsed_view(tmp_path, CHILD_VIEW_MODEL, "views { view v { title 'V' include r, p -> * } }", "v")
    assert set(v.node_ids) == {"r", "p", "p.kid", "p.kid2", "q"}
    assert set(edge_tuples(v)) == {("p.kid", "q", True), ("p.kid2", "q", False), ("p.kid", "p.kid2", False)}


def test_focus_dst_predicate_matches_by_top_ancestor(tmp_path):
    v, _ = parsed_view(tmp_path, CHILD_VIEW_MODEL, "views { view v { title 'V' include r, * -> q } }", "v")
    assert set(v.node_ids) == {"r", "p", "p.kid", "p.kid2", "q"}
    assert set(edge_tuples(v)) == {("p.kid", "q", True), ("p.kid2", "q", False)}


FOCUS_SUBTREE_MODEL = """
specification { element node }
model {
  p = node 'P' {
    kid = node 'Kid'
    kid2 = node 'Kid two'
  }
  q = node 'Q'
  q -> p.kid
}
"""


def test_focus_predicate_pulls_ancestor_chain_not_whole_subtree(tmp_path):
    # `q -> *` matches only q -> p.kid; it pulls p.kid + its ancestor p, never the unrelated p.kid2.
    v, findings = parsed_view(tmp_path, FOCUS_SUBTREE_MODEL, "views { view v { title 'V' include q, q -> * } }", "v")
    assert set(v.node_ids) == {"q", "p", "p.kid"}
    assert "p.kid2" not in v.node_ids
    assert edge_tuples(v) == [("q", "p.kid", False)]
    assert findings == []


def test_render_mermaid_subgraphs_golden(tmp_path):
    root = arch_only(tmp_path, CHILD_VIEW_MODEL, ONE_VIEW)
    expected = [
        "```mermaid",
        "flowchart TD",
        "  %% v: V",
        '  subgraph p["P"]',
        '    p__kid["Kid"]',
        '    p__kid2["Kid two"]',
        "  end",
        '  q["Q"]',
        '  r["R"]',
        "  p__kid -.-> q",
        "  p__kid2 --> q",
        "  p__kid --> p__kid2",
        "  q --> r",
        "```",
        "*Dashed arrows: legacy edges slated to die.*",
    ]
    assert structural(render(root, "v")) == expected


def test_render_mermaid_nested_subgraphs_golden(tmp_path):
    root = arch_only(tmp_path, DEPTH_MODEL, ONE_VIEW)
    expected = [
        "```mermaid",
        "flowchart TD",
        "  %% v: V",
        '  subgraph p["P"]',
        '    subgraph p__kid["Kid"]',
        '      p__kid__grand["Grand"]',
        "    end",
        "  end",
        '  q["Q"]',
        "  p__kid__grand --> q",
        "```",
    ]
    assert structural(render(root, "v")) == expected


def test_render_mermaid_subgraph_view_has_classdef_styling(tmp_path):
    root = arch_only(tmp_path, CHILD_VIEW_MODEL, ONE_VIEW)
    assert any(line.strip().startswith("classDef") for line in render(root, "v").splitlines())


def test_render_collapsed_view_matches_classic_emission(tmp_path):
    root = arch_only(tmp_path, DEPTH_MODEL, ONE_VIEW, index_text="view_depth:\n  v: 1\n")
    expected = "\n".join(
        [
            "```mermaid",
            "flowchart TD",
            "  %% v: V",
            '  p["P"]',
            '  q["Q"]',
            "  p --> q",
            "```",
        ]
    )
    assert render(root, "v") == expected


MODEL_WITH_CHILD = """
specification {
  element node
  tag legacy
}
model {
  core = node 'Core' {
    query = node 'Query'
  }
  engine = node 'Engine'
  core.query -> engine #legacy
  engine -> core
}
"""


def test_generate_fills_subgraph_blocks(tmp_path):
    root = make_repo(tmp_path, regenerate=False, model=MODEL_WITH_CHILD)
    arch_diagrams.generate(root)
    text = (root / "architecture" / "system.arc42.md").read_text(encoding="utf-8")
    assert 'subgraph core["Core"]' in text
    assert 'core__query["Query"]' in text
    assert "core__query -.-> engine" in text
    assert "engine --> core" in text
    assert arch_diagrams.generate(root) == []


def test_diagrams_fresh_child_model_healthy(tmp_path):
    assert fresh_findings(make_repo(tmp_path, model=MODEL_WITH_CHILD)) == []


def test_diagrams_fresh_child_edit_without_regen_flagged(tmp_path):
    root = make_repo(tmp_path, model=MODEL_WITH_CHILD)
    model = root / "architecture" / "model" / "pkg.c4"
    model.write_text(
        model.read_text(encoding="utf-8").replace("query = node 'Query'", "query = node 'Q2'"), encoding="utf-8"
    )
    findings = fresh_findings(root)
    assert findings
    assert any(FIX_CMD in f for f in findings)


def test_view_depth_non_integer_is_finding(tmp_path):
    index = INDEX + "view_depth:\n  land: fish\n"
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("view_depth" in f for f in arch_check.run_checks(root))


def test_view_depth_unknown_view_id_is_finding(tmp_path):
    index = INDEX + "view_depth:\n  ghost: 2\n"
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("ghost" in f for f in arch_check.run_checks(root))


def test_view_depth_zero_is_finding(tmp_path):
    index = INDEX + "view_depth:\n  land: 0\n"
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("view_depth" in f for f in arch_check.run_checks(root))


def test_view_depth_negative_is_finding(tmp_path):
    index = INDEX + "view_depth:\n  land: -1\n"
    root = make_repo(tmp_path, regenerate=False, index=index, system_md=SYSTEM_MD_NO_MARKERS)
    assert any("view_depth" in f for f in arch_check.run_checks(root))


def test_generate_raises_on_malformed_view_depth(tmp_path):
    index = INDEX + "view_depth:\n  land: fish\n"
    root = make_repo(tmp_path, regenerate=False, index=index)
    with pytest.raises(ValueError):
        arch_diagrams.generate(root)


# --------------------------------------------------------------------------- parser consolidation (task 2.1)


def test_arch_check_dropped_regex_parser():
    for symbol in ("_ELEMENT_RE", "_RELATION_RE", "_parse_elements", "_parse_relations"):
        assert not hasattr(arch_check, symbol)


def test_model_identity_finding_text_unchanged(tmp_path):
    model = MODEL.replace("engine = node 'Engine'", "engine = node 'Engine'\n  ghost = node 'Ghost'")
    root = make_repo(tmp_path, regenerate=False, model=model)
    assert "model-identity: element ghost maps to no node or declared child in index.yaml" in arch_check.run_checks(root)


def test_model_truth_missing_finding_names_module_witness(tmp_path):
    root = make_repo(tmp_path, regenerate=False, model=MODEL.replace("  engine -> core\n", ""))
    assert (
        "model-truth: measured runtime edge engine -> core is missing from the model"
        " (import pkg.engine.b -> pkg.core)"
    ) in arch_check.run_checks(root)


# --------------------------------------------------------------------------- real repo (tasks 3.1-3.4, freshness)


def test_repo_has_no_stale_diagrams():
    assert [f for f in arch_check.run_checks(REPO_ROOT) if f.startswith("diagrams-fresh:")] == []


def test_repo_views_has_core_focus():
    views = (REPO_ROOT / "architecture" / "views.c4").read_text(encoding="utf-8")
    assert "view core_focus" in views
    assert "Core in context" in views
    assert "core -> *" in views
    assert "* -> core" in views


def test_repo_views_has_sql_focus():
    views = (REPO_ROOT / "architecture" / "views.c4").read_text(encoding="utf-8")
    assert "view sql_focus" in views
    assert "SQL generation in context" in views
    assert "sql -> *" in views
    assert "* -> sql" in views


def test_repo_index_diagrams_mapping():
    index = yaml.safe_load((REPO_ROOT / "architecture" / "index.yaml").read_text(encoding="utf-8"))
    assert index.get("diagrams") == {
        "architecture/system.arc42.md": ["landscape"],
        "architecture/core.arc42.md": ["core_focus"],
        "architecture/engine.arc42.md": ["query_pipeline"],
        "architecture/sql.arc42.md": ["sql_focus"],
    }


def test_repo_mapped_docs_have_markers():
    for doc, view in [
        ("system.arc42.md", "landscape"),
        ("core.arc42.md", "core_focus"),
        ("engine.arc42.md", "query_pipeline"),
        ("sql.arc42.md", "sql_focus"),
    ]:
        text = (REPO_ROOT / "architecture" / doc).read_text(encoding="utf-8")
        assert f"<!-- likec4:{view} -->" in text
        assert f"<!-- /likec4:{view} -->" in text


def test_repo_semantics_doc_has_no_markers():
    assert "<!-- likec4:" not in (REPO_ROOT / "architecture" / "semantics.arc42.md").read_text(encoding="utf-8")


def test_repo_system_doc_documents_diagrams_fresh():
    text = (REPO_ROOT / "architecture" / "system.arc42.md").read_text(encoding="utf-8")
    assert "diagrams-fresh" in text
    assert "arch_diagrams.py" in text


def test_repo_enforced_tags_stay_green():
    assert [f for f in arch_check.run_checks(REPO_ROOT) if f.startswith("enforced-tags:")] == []
