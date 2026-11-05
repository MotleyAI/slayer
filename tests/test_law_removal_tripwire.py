"""Removal tripwire: compatibility code tagged for removal fails the suite once its major version ships.

The tag is a call to ``slayer.core.deprecation.warn_deprecated`` (any import spelling) with a
string-literal ``removal``. Spec: compatibility/removal-tripwire.
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
HELPER = "slayer.core.deprecation.warn_deprecated"
HELPER_MODULE = "slayer/core/deprecation.py"

#: Every covered workaround: (file, a token its warning message spells literally).
PINNED: frozenset[tuple[str, str]] = frozenset({
    ("slayer/engine/ingestion.py", "ingest_datasource()"),
    ("slayer/engine/ingestion.py", "ingest_datasource_report()"),
    ("slayer/engine/ingestion.py", "ingest_datasource_idempotent()"),
    ("slayer/engine/schema_drift.py", "validate_datasource()"),
    ("slayer/dbt/converter.py", "DbtToSlayerConverter"),
    ("slayer/osi/converter.py", "OsiToSlayerConverter"),
    ("slayer/storage/legacy_backends.py", "save_datasource"),
    ("slayer/storage/legacy_backends.py", "_list_memories_rows"),
    ("slayer/cli.py", "--models-dir"),
    ("slayer/storage/base.py", "SLAYER_MODELS_DIR"),
    ("slayer/mcp/server.py", "inspect_model"),
    ("slayer/core/errors.py", ".identifier"),
    ("slayer/sql/client.py", "get_column_types_sync()"),
    ("slayer/sql/client.py", "SlayerSQLClient.get_column_types()"),
    ("slayer/engine/query_engine.py", "SlayerQueryEngine.get_column_types()"),
    ("slayer/sql/client.py", "stops installing"),
    ("slayer/sql/client.py", "A SQL client's get_column_types()"),
    ("slayer/sql/client.py", "DelegatingSqlClient.get_column_types()"),
})


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
        if not (isinstance(node, ast.Call) and _canonical(node.func, bindings) == HELPER):
            continue
        keywords = {k.arg: k.value for k in node.keywords}
        removal = keywords.get("removal")
        out.append(Site(
            file=rel,
            line=node.lineno,
            removal=removal.value if isinstance(removal, ast.Constant) and isinstance(removal.value, str) else None,
            message=_literal_text(node.args[0] if node.args else keywords.get("message")),
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
    assert all(s.removal == "2.0" for s in sites)


def test_at_2_0_0_the_tripwire_names_every_pinned_site(monkeypatch) -> None:
    monkeypatch.setattr(slayer, "__version__", "2.0.0")
    sites = _scan()
    problems = _problems(sites, version=_package_version())
    named = {_feature(s) for s in sites if any(p.startswith(f"{s.where}: ") for p in problems)}
    assert named == set(PINNED)


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

        def f():
            dep.warn_deprecated("a", removal="2.0")
            deprecation.warn_deprecated("b", removal="2.0")
            _wd("c", removal="2.0")
            warn_deprecated(message="d", removal="2.0")
    """)
    sites = _sites_in(source, rel="slayer/core/x.py")
    assert [(s.message, s.removal) for s in sites] == [("a", "2.0"), ("b", "2.0"), ("c", "2.0"), ("d", "2.0")]


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


def test_unrelated_warn_calls_are_not_tags() -> None:
    source = textwrap.dedent("""
        import warnings
        from mylib.deprecation import warn_deprecated

        def f():
            warnings.warn("x", UserWarning)
            warn_deprecated("y", removal="2.0")
    """)
    assert _sites_in(source, rel="slayer/x.py") == []


#: Each dialect's 1.6.0 classification: the class attribute every one of these dialect files declares.
LEGACY_TABLE = "legacy_type_categories"
LEGACY_DIALECT_FILES = frozenset({
    "slayer/sql/dialects/postgres.py", "slayer/sql/dialects/mysql.py", "slayer/sql/dialects/tsql.py",
    "slayer/sql/dialects/snowflake.py", "slayer/sql/dialects/duckdb.py", "slayer/sql/dialects/sqlite.py",
})
CATEGORY_READER = ("slayer/sql/client.py", "type_categories")
#: The tagged deprecated probes: the only readers of ``type_categories``.
CATEGORY_PROBES = frozenset({
    ("slayer/sql/client.py", "get_column_types_sync"),
    ("slayer/sql/client.py", "SlayerSQLClient.get_column_types"),
    ("slayer/sql/client.py", "DelegatingSqlClient.get_column_types"),
    ("slayer/engine/query_engine.py", "SlayerQueryEngine.get_column_types"),
})


class Use(BaseModel):
    file: str
    line: int
    scope: str  # dotted enclosing class/function; "" at module level
    in_function: bool
    declares: bool


def _uses_in(source: str, *, rel: str, name: str) -> list[Use]:
    """Every mention of ``name`` (a name, attribute or assignment target), with its enclosing scope."""
    out: list[Use] = []

    def visit(node: ast.AST, scope: list[str], in_function: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                visit(child, [*scope, child.name], in_function or not isinstance(child, ast.ClassDef))
                continue
            if (isinstance(child, ast.Name) and child.id == name) or (
                    isinstance(child, ast.Attribute) and child.attr == name):
                out.append(Use(file=rel, line=child.lineno, scope=".".join(scope), in_function=in_function,
                               declares=isinstance(child.ctx, ast.Store)))
            visit(child, scope, in_function)

    visit(ast.parse(source), [], False)
    return out


def _uses(name: str) -> list[Use]:
    return [
        use
        for path in sorted(SLAYER.rglob("*.py"))
        for use in _uses_in(path.read_text(), rel=path.relative_to(REPO).as_posix(), name=name)
    ]


def test_every_1_6_0_dialect_declares_its_classification() -> None:
    declaring = {u.file for u in _uses(LEGACY_TABLE) if u.declares and not u.in_function}
    assert LEGACY_DIALECT_FILES <= declaring, sorted(LEGACY_DIALECT_FILES - declaring)


def test_the_1_6_0_classifications_are_read_only_by_type_categories() -> None:
    reads = [u for u in _uses(LEGACY_TABLE) if not u.declares]
    stray = [f"{u.file}:{u.line} in {u.scope or '<module>'}" for u in reads
             if (u.file, u.scope) != CATEGORY_READER
             and not (u.file.startswith("slayer/sql/dialects/") and not u.in_function)]
    assert reads
    assert not stray, "only type_categories reads the 1.6.0 classifications:\n" + "\n".join(stray)


def test_type_categories_is_called_only_by_the_tagged_probes() -> None:
    calls = [u for u in _uses("type_categories") if u.in_function and not u.declares]
    callers = {(u.file, u.scope) for u in calls}
    assert callers == CATEGORY_PROBES, sorted(callers ^ CATEGORY_PROBES)
    tagged = {(u.file, u.scope) for u in _uses("warn_deprecated") if u.in_function}
    assert CATEGORY_PROBES <= tagged, sorted(CATEGORY_PROBES - tagged)


def test_a_classification_read_outside_type_categories_is_found() -> None:
    source = textwrap.dedent("""
        class D:
            legacy_type_categories = {"interval": "time"}

        def type_categories(types, *, dialect):
            return dialect.legacy_type_categories

        def elsewhere(dialect):
            return dialect.legacy_type_categories
    """)
    uses = _uses_in(source, rel="slayer/sql/client.py", name=LEGACY_TABLE)
    assert [(u.scope, u.declares) for u in uses] == [("D", True), ("type_categories", False), ("elsewhere", False)]
