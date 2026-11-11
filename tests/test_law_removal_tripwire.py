"""Removal tripwire: compatibility code tagged for removal fails the suite once its major version ships.

The tag is a call to ``slayer.core.deprecation.warn_deprecated`` or ``raise_moved`` (any import
spelling) with a string-literal ``removal``. Spec: compatibility/removal-tripwire.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from pydantic import BaseModel

import slayer
from tests.test_law_resource_ownership import _canonical, _import_bindings

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"
HELPERS = frozenset({"slayer.core.deprecation.warn_deprecated", "slayer.core.deprecation.raise_moved"})
HELPER_MODULE = "slayer/core/deprecation.py"

#: Every covered workaround: (file, a token its message spells literally) -> removal version.
PINNED: dict[tuple[str, str], str] = {
    **{
        (f"slayer/{old.replace('.', '/')}.py", f"slayer.{old}"): "3.0"
        for old in (
            "sql.client", "sql.engine_factory", "sql.sqlite_introspect", "sql.conformance",
            "engine.ingestion", "engine.schema_drift", "engine.schema_scope", "engine.introspect_utils",
            "engine.broken_dependents", "engine.internal_tables", "engine.fk_edges", "storage.type_refinement",
            "engine.home", "engine.dimension_routing", "engine.filter_reachability",
            "engine.join_safety", "engine.unrelated",
        )
    },
    ("slayer/engine/population/__init__.py", "slayer.engine.population."): "3.0",
    ("slayer/engine/cardinality/__init__.py", "slayer.engine.cardinality."): "3.0",
}


class Site(BaseModel):
    file: str
    line: int
    removal: str | None
    message: str

    @property
    def where(self) -> str:
        return f"{self.file}:{self.line}"


def _package_version() -> str:
    return slayer.__version__


def _major(version: str) -> int:
    return int(version.split(".")[0])


def _module_of(rel: str) -> str:
    return rel.removesuffix(".py").removesuffix("/__init__").replace("/", ".")


def _bindings(tree: ast.AST, *, rel: str) -> dict[str, str]:
    """``_import_bindings`` plus relative ``from . import`` forms."""
    bindings = _import_bindings(tree)
    package = _module_of(rel).split(".")
    if not rel.endswith("__init__.py"):
        package = package[:-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level:
            base = package[: len(package) - (node.level - 1)]
            module = ".".join([*base, *([node.module] if node.module else [])])
            for alias in node.names:
                bindings[alias.asname or alias.name] = f"{module}.{alias.name}"
    return bindings


def _literal_text(node: ast.AST | None) -> str:
    """A message's literal text; interpolations read as ``{}``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(str(v.value) if isinstance(v, ast.Constant) else "{}" for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _literal_text(node.left) + _literal_text(node.right)
    return ""


def _sites_in(source: str, *, rel: str) -> list[Site]:
    tree = ast.parse(source)
    bindings = _bindings(tree, rel=rel)
    out: list[Site] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _canonical(node.func, bindings) in HELPERS):
            continue
        keywords = {k.arg: k.value for k in node.keywords}
        removal = keywords.get("removal")
        out.append(Site(
            file=rel,
            line=node.lineno,
            removal=removal.value if isinstance(removal, ast.Constant) and isinstance(removal.value, str) else None,
            message=_literal_text(node.args[0] if node.args else keywords.get("message", keywords.get("old"))),
        ))
    return out


def _scan() -> list[Site]:
    return [
        site
        for path in sorted(SLAYER.rglob("*.py"))
        for site in _sites_in(path.read_text(), rel=path.relative_to(REPO).as_posix())
    ]


def _problems(sites: list[Site], *, version: str) -> list[str]:
    out: list[str] = []
    for site in sites:
        if site.removal is None:
            out.append(f"{site.where}: removal version is not a string literal: {site.message}")
        elif _major(site.removal) <= _major(version):
            out.append(f"{site.where}: due for removal in {site.removal} (package {version}): {site.message}")
    return out


def _feature(site: Site) -> tuple[str, str] | None:
    matches = [(f, token) for f, token in PINNED if f == site.file and token in site.message]
    return matches[0] if len(matches) == 1 else None


def test_no_tagged_code_outlives_its_removal_version() -> None:
    problems = _problems(_scan(), version=_package_version())
    assert not problems, "remove the compatibility code (or fix its tag):\n" + "\n".join(problems)


def test_tagged_sites_are_exactly_the_covered_workarounds() -> None:
    sites = _scan()
    unmatched = [f"{s.where}: {s.message}" for s in sites if _feature(s) is None]
    assert not unmatched, "tagged sites matching no (or several) pinned workarounds:\n" + "\n".join(unmatched)
    features = [_feature(s) for s in sites]
    assert sorted(f for f in features if f is not None) == sorted(PINNED)
    wrong = [f"{s.where}: {s.removal}" for s in sites if (f := _feature(s)) is not None and s.removal != PINNED[f]]
    assert not wrong, "tagged sites with a removal version other than the pinned one:\n" + "\n".join(wrong)


def _named_at(version: str, monkeypatch) -> set[tuple[str, str] | None]:
    monkeypatch.setattr(slayer, "__version__", version)
    sites = _scan()
    problems = _problems(sites, version=_package_version())
    return {_feature(s) for s in sites if any(p.startswith(f"{s.where}: ") for p in problems)}


def test_at_the_next_major_the_tripwire_names_every_pinned_site(monkeypatch) -> None:
    assert _named_at(f"{_major(_package_version()) + 1}.0.0", monkeypatch) == set(PINNED)


def test_only_the_helper_emits_future_warnings() -> None:
    offenders = [
        f"{path.relative_to(REPO).as_posix()}:{node.lineno}"
        for path in sorted(SLAYER.rglob("*.py"))
        if path.relative_to(REPO).as_posix() != HELPER_MODULE
        for node in ast.walk(ast.parse(path.read_text()))
        if (isinstance(node, ast.Name) and node.id == "FutureWarning")
        or (isinstance(node, ast.Attribute) and node.attr == "FutureWarning")
    ]
    assert not offenders, "emit deprecations through warn_deprecated:\n" + "\n".join(offenders)


def test_aliased_and_relative_uses_are_found() -> None:
    source = textwrap.dedent("""
        import slayer.core.deprecation as dep
        from slayer.core import deprecation
        from slayer.core.deprecation import warn_deprecated as _wd
        from .deprecation import warn_deprecated
        from slayer.core.deprecation import raise_moved as _moved
        from .deprecation import raise_moved

        def f():
            dep.warn_deprecated("a", removal="2.0")
            deprecation.warn_deprecated("b", removal="2.0")
            _wd("c", removal="2.0")
            warn_deprecated(message="d", removal="2.0")
            dep.raise_moved("e", "e2", removal="3.0")
            _moved("f", "f2", removal="3.0")
            raise_moved(old="g", new="g2", removal="3.0")
    """)
    sites = _sites_in(source, rel="slayer/core/x.py")
    assert sorted((s.message, s.removal) for s in sites) == [
        ("a", "2.0"), ("b", "2.0"), ("c", "2.0"), ("d", "2.0"), ("e", "3.0"), ("f", "3.0"), ("g", "3.0"),
    ]


def test_a_non_literal_removal_version_is_rejected() -> None:
    source = textwrap.dedent("""
        from slayer.core.deprecation import warn_deprecated
        REMOVAL = "2.0"

        def f(name):
            warn_deprecated(f"{name}() is deprecated", removal=REMOVAL)
    """)
    sites = _sites_in(source, rel="slayer/x.py")
    assert _problems(sites, version="1.6.0") == [
        "slayer/x.py:6: removal version is not a string literal: {}() is deprecated",
    ]


def test_a_non_literal_removal_version_on_a_moved_path_is_rejected() -> None:
    source = textwrap.dedent("""
        from slayer.core.deprecation import raise_moved
        REMOVAL = "3.0"

        raise_moved("slayer.old.mod", "slayer.new.mod", removal=REMOVAL)
    """)
    sites = _sites_in(source, rel="slayer/old/mod.py")
    assert _problems(sites, version="1.6.0") == [
        "slayer/old/mod.py:5: removal version is not a string literal: slayer.old.mod",
    ]


def test_unrelated_warn_calls_are_not_tags() -> None:
    source = textwrap.dedent("""
        import warnings
        from mylib.deprecation import warn_deprecated

        def f():
            warnings.warn("x", UserWarning)
            warn_deprecated("y", removal="2.0")
    """)
    assert _sites_in(source, rel="slayer/x.py") == []
