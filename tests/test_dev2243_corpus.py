"""Spec: models/semantic-model-documents › The Ossie corpus loads, compiles and round-trips.

The corpus root is ``$SLAYER_OSSIE_CORPUS`` (the weekly job points it at a fresh upstream fetch), defaulting to the
vendored snapshot.
"""

from __future__ import annotations

import contextlib
import re
import shutil
import subprocess
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import sqlglot
import yaml
from sqlglot import exp
from sqlglot.errors import ParseError

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.osi.exporter import export_osi
from slayer.sql.ossie_dialects import reader_for
from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import STORAGES, error, importer_module, make_storage, semantic_model_module
from tests._engine_helpers import owned_query_engine
from tests._ossie_gate import TIER1, corpus_root

ROOT = corpus_root()
DS = "corpus"
SOURCES = ("examples/*.yaml", "compliance/portability/m[0-8]/*.ossie.yaml", "compliance/wide_table/*/*.ossie.yaml",
           "compliance/query/scott/*.ossie.yaml", "harvested/*.yaml")


def _is_semantic_model(path: Path) -> bool:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return isinstance(data, dict) and "datasets" in data


DOCUMENTS = sorted({p for pattern in SOURCES for p in ROOT.glob(pattern) if _is_semantic_model(p)})
IDS = [p.relative_to(ROOT).as_posix() for p in DOCUMENTS]


def _load(path: Path) -> tuple[str, dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    return text, yaml.safe_load(text)


# --------------------------------------------------------------------------- #
# Upstream validate.py, once over the whole corpus, from a copy (it writes __pycache__ beside its loader)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def upstream_reports(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Each corpus document's report block from one upstream ``validate.py`` run; skips without uv."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; the upstream Ossie validator runs through `uv run --script`")
    copy = tmp_path_factory.mktemp("ossie_validator")
    for part in ("validation", "core-spec", "python"):
        shutil.copytree(ROOT / part, copy / part)
    run = subprocess.run([uv, "run", "--script", str(copy / "validation" / "validate.py"), *map(str, DOCUMENTS)],
                         capture_output=True, text=True, check=False, timeout=600)
    blocks: dict[str, str] = {}
    current: str | None = None
    for line in run.stdout.splitlines():
        if line.startswith("== "):
            current = line[3:]
            blocks[current] = ""
        elif current is not None:
            blocks[current] += line + "\n"
    return blocks


# --------------------------------------------------------------------------- #
# Which metrics SLayer cannot evaluate yet (model-scoped metrics Ossie rejects at query time)
# --------------------------------------------------------------------------- #
def _expression_text(expression: dict[str, Any]) -> str | None:
    """The first readable candidate: top-level ``sql``, then OSSIE_SQL_2026, ANSI_SQL, any SQL entry; else None."""
    if expression.get("sql"):
        return expression["sql"]
    entries = expression.get("dialects") or []
    for wanted in ("OSSIE_SQL_2026", "ANSI_SQL"):
        for entry in entries:
            if entry.get("dialect") == wanted:
                return entry["expression"]
    return next((e["expression"] for e in entries if reader_for(e.get("dialect") or "") is not None), None)


def _parse(sql: str | None) -> exp.Expr | None:
    if not sql:
        return None
    try:
        return sqlglot.parse_one(sql)
    except ParseError:
        return None


#: Date-part units sqlglot reads as bare columns (``DATEDIFF(day, …)``).
DATE_PARTS = frozenset({"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond",
                        "microsecond", "dayofweek", "dayofyear", "isoweek", "isoyear"})


def _is_agg_call(node: exp.Expr) -> bool:
    return type(node).__name__ == "Agg" or (isinstance(node, exp.Anonymous) and node.name.upper() == "AGG")


def _agg_argument(node: exp.Expr) -> exp.Expr | None:
    """An ``AGG`` call's single argument, in either sqlglot shape."""
    if type(node).__name__ == "Agg":
        return node.this
    args = node.expressions if isinstance(node, exp.Anonymous) else []
    return args[0] if len(args) == 1 else None


def _agg_columns(tree: exp.Expr) -> list[exp.Column]:
    return [a for n in tree.walk() if _is_agg_call(n) and isinstance(a := _agg_argument(n), exp.Column)]


def _evaluates_source_measure(tree: exp.Expr) -> bool:
    """Calls ``AGG(source.measure)``, which no SLayer source can evaluate (Ossie rule 9)."""
    return any(c.table.casefold() == "source" for c in _agg_columns(tree))


#: Ranking and offset functions: legal only under ``OVER``, never an aggregate (sqlglot types them ``AggFunc``).
WINDOW_ONLY = (exp.RowNumber, exp.Rank, exp.DenseRank, exp.PercentRank, exp.CumeDist, exp.Ntile, exp.Lag, exp.Lead,
               exp.FirstValue, exp.LastValue, exp.NthValue)


def _windowed_call(node: exp.Expr) -> bool:
    """The function an ``OVER`` applies, its ``FILTER`` included."""
    parent = node.parent
    if isinstance(parent, exp.Filter) and parent.this is node:
        parent = parent.parent
    return isinstance(parent, exp.Window)


def _is_aggregate(node: exp.Expr) -> bool:
    return _is_agg_call(node) or (isinstance(node, exp.AggFunc) and not isinstance(node, WINDOW_ONLY)
                                  and not _windowed_call(node))


def _aggregated(node: exp.Expr) -> bool:
    """Inside an aggregate call, its FILTER clause or an ``AGG(d.m)`` call."""
    parent = node.parent
    while parent is not None:
        if _is_aggregate(parent) or (isinstance(parent, exp.Filter) and _is_aggregate(parent.this)):
            return True
        parent = parent.parent
    return False


def _measure_level(window: exp.Window) -> bool:
    """An aggregate in the window's arguments or keys."""
    return any(_is_aggregate(n) for n in window.walk() if n is not window)


def _window_grain(tree: exp.Expr) -> list[exp.Column]:
    """The row-level columns measure-level windows read outside an aggregate: query dimensions, not field reads."""
    return [c for w in tree.find_all(exp.Window) if _measure_level(w) for c in w.find_all(exp.Column)
            if not _aggregated(c)]


def _model_scoped_rejection(tree: exp.Expr, *, metric_names: set[str]) -> str | None:
    """Why a model-scoped metric loads unevaluable (decision 35: an unqualified or unaggregated field read, a row-level
    window, an aggregate naming no dataset), else None; an aggregate reading several datasets evaluates as in a
    query."""
    if any(not _measure_level(w) and not _aggregated(w) for w in tree.find_all(exp.Window)):
        return "unaggregated field"
    grain = {id(c) for c in _window_grain(tree)}
    for column in tree.find_all(exp.Column):
        if (not column.table and column.name.casefold() in DATE_PARTS) or id(column) in grain:
            continue
        if _aggregated(column):
            if not column.table:
                return "unqualified field"
        elif column.table:
            return "unaggregated field"
        elif column.name.casefold() not in metric_names:
            return "unqualified field"
    for aggregate in tree.find_all(exp.AggFunc):
        if _is_aggregate(aggregate) and not _is_agg_call(aggregate) and not _aggregated(aggregate) and not any(
                c.table for c in aggregate.find_all(exp.Column)):
            return "aggregate over no dataset"
    return None


#: (document, metric) → the typed error its query raises: Ossie-schema-valid metrics no query can evaluate.
REFUSED: dict[tuple[str, str], str] = {
    ("harvested/216f1c025746.yaml", "revenue"): "UnknownReferenceError",
    ("harvested/738601d2a838.yaml", "revenue"): "UnknownReferenceError",
    ("harvested/ddeb8527e638.yaml", "revenue"): "UnknownReferenceError",
    ("harvested/3cdbe43998c6.yaml", "now_metric"): "NoRowSourceError",
    ("harvested/41093682a932.yaml", "revenue"): "NoRowSourceError",
    ("harvested/c82499fbddd2.yaml", "revenue"): "NoRowSourceError",
    ("harvested/fad5e6f81b85.yaml", "revenue"): "NoRowSourceError",
    ("compliance/wide_table/retail/retail_model.ossie.yaml", "order_less_return"): "UnsafeRelationshipInputError",
}

#: (document, metric) whose runnable form SLayer's expression reader rejects: loaded not evaluable.
UNPARSEABLE: set[tuple[str, str]] = {("harvested/58a8515b964f.yaml", "revenue")}


def expected_outcomes(data: dict[str, Any]) -> dict[str, str]:
    """Metric reference → ``compiled`` or ``unevaluable``."""
    outcomes: dict[str, str] = {}
    for dataset in data["datasets"]:
        for metric in dataset.get("metrics") or []:
            text = _expression_text(metric["expression"])
            tree = _parse(text)
            unevaluable = text is None or tree is not None and _evaluates_source_measure(tree)
            outcomes[f"{dataset['name']}.{metric['name']}"] = "unevaluable" if unevaluable else "compiled"
    metric_names = {m["name"].casefold() for m in data.get("metrics") or []}
    trees: dict[str, exp.Expr | None] = {}
    for metric in data.get("metrics") or []:
        text = _expression_text(metric["expression"])
        tree = trees[metric["name"]] = _parse(text)
        unevaluable = text is None or tree is not None and _model_scoped_rejection(
            tree, metric_names=metric_names) is not None
        outcomes[metric["name"]] = "unevaluable" if unevaluable else "compiled"
    return _propagated(outcomes, trees)


def _propagated(outcomes: dict[str, str], trees: dict[str, exp.Expr | None]) -> dict[str, str]:
    """Model-scoped metrics reaching an unevaluable metric (``AGG(d.m)`` or a bare model-scoped name) are too."""
    blocked = {k.casefold() for k, v in outcomes.items() if v == "unevaluable"}
    changed = True
    while changed:
        changed = False
        for name, tree in trees.items():
            if name.casefold() in blocked or tree is None:
                continue
            reached = {f"{c.table}.{c.name}".casefold() for c in _agg_columns(tree)}
            reached |= {c.name.casefold() for c in tree.find_all(exp.Column) if not c.table}
            if reached & blocked:
                blocked.add(name.casefold())
                outcomes[name] = "unevaluable"
                changed = True
    return outcomes


def _query(reference: str, *, data: dict[str, Any]) -> dict[str, Any]:
    """A dataset metric is read from its own dataset; a model-scoped metric root-less, by its bare name; either grouped
    by the row-level columns its measure-level windows read."""
    home, _, name = reference.rpartition(".")
    metrics = next(d for d in data["datasets"] if d["name"] == home).get("metrics") if home else data.get("metrics")
    tree = _parse(next((_expression_text(m["expression"]) for m in metrics or [] if m["name"] == name), None))
    dimensions = list(dict.fromkeys(f"{c.table or home}.{c.name}" for c in _window_grain(tree))) if tree else []
    query: dict[str, Any] = {"population": home, "metrics": [reference]} if home else {"metrics": [reference]}
    return query | ({"dimensions": dimensions} if dimensions else {})


# --------------------------------------------------------------------------- #
# Storage and engine over an empty datasource
# --------------------------------------------------------------------------- #
def _datasource(tmp: Path, dialect: str = "duckdb") -> DatasourceConfig:
    """A DuckDB file, or any other dialect's type alone (compiled with a dry run; Snowflake names a profile)."""
    if dialect == "snowflake":
        return DatasourceConfig(name=DS, type=dialect, connection_name="dry_run")
    if dialect != "duckdb":
        return DatasourceConfig(name=DS, type=dialect)
    return DatasourceConfig(name=DS, type="duckdb", database=str(tmp / "corpus.duckdb"))


async def _store(storage: StorageBackend, *, text: str, data: dict[str, Any]) -> None:
    await storage.save_semantic_model_text(data_source=DS, name=data["name"], text=text)


@contextlib.asynccontextmanager
async def _env(kind: str, tmp: Path, dialect: str = "duckdb") -> AsyncGenerator[tuple[StorageBackend, SlayerQueryEngine]]:
    """Empty ``kind`` storage holding only the datasource, and an engine over it."""
    storage = make_storage(kind, tmp)
    async with owned_query_engine(storage=storage, datasources=[_datasource(tmp, dialect)]) as engine:
        yield storage, engine


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_the_corpus_holds_every_source() -> None:
    """Spec: the corpus is the examples, m0–m8, the wide_table models, scott and the harvested documents."""
    assert {p.split("/")[0] for p in IDS} == {"examples", "compliance", "harvested"}
    assert {p.split("/")[2] for p in IDS if p.startswith("compliance/portability/")} == {f"m{i}" for i in range(9)}
    assert any(p.startswith("compliance/wide_table/retail/") for p in IDS)
    assert any(p.startswith("compliance/wide_table/stores/") for p in IDS)
    assert any(p.startswith("compliance/query/scott/") for p in IDS)
    assert sum(p.startswith("examples/") for p in IDS) >= 4


@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
def test_upstream_validate_passes(path: Path, upstream_reports: dict[str, str]) -> None:
    assert "Validation PASSED" in upstream_reports.get(str(path), ""), upstream_reports.get(str(path))


@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
def test_the_native_validity_check_agrees(path: Path) -> None:
    _, data = _load(path)
    assert semantic_model_module().ossie_validity_errors(data) == []


@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
async def test_stores_and_loads_natively(path: Path, tmp_path: Path) -> None:
    text, data = _load(path)
    async with _env("yaml", tmp_path) as (storage, engine):
        await _store(storage, text=text, data=data)
        datasets, failures = await storage.load_datasets(data_source=DS)
        assert failures == []
        assert sorted(d.name for d in datasets) == sorted(d["name"] for d in data["datasets"])
        metrics = await storage.list_model_scoped_metrics(DS)
        assert sorted(m.name for m in metrics) == sorted(m["name"] for m in data.get("metrics") or [])


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
async def test_every_metric_binds_and_compiles(path: Path, dialect: str, tmp_path: Path) -> None:
    """Every metric compiles to SQL in every Tier-1 dialect; an unevaluable one raises a typed error naming it, a
    ``REFUSED`` one its error, identically in every dialect."""
    text, data = _load(path)
    doc_id = path.relative_to(ROOT).as_posix()
    expected = (expected_outcomes(data) | {m: f"refused: {e}" for (d, m), e in REFUSED.items() if d == doc_id}
                | {m: "unevaluable" for d, m in UNPARSEABLE if d == doc_id})
    async with _env("yaml", tmp_path, dialect) as (storage, engine):
        await _store(storage, text=text, data=data)
        actual: dict[str, str] = {}
        for reference in expected:
            try:
                response = await engine.execute(_query(reference, data=data), dry_run=True)
            except (error("UnevaluableEntityError"), error("ModelMetricReferenceError")) as exc:
                name = reference.rsplit(".", 1)[-1]
                actual[reference] = "unevaluable" if re.search(re.escape(name), str(exc)) else f"unnamed: {exc}"
            except Exception as exc:  # noqa: BLE001 — reported per metric
                refused = f"refused: {type(exc).__name__}"
                actual[reference] = refused if expected.get(reference) == refused else f"{refused}: {exc}"
            else:
                actual[reference] = "compiled" if response.sql else "no SQL"
        assert actual == expected


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
async def test_store_load_save_unchanged_and_export_reproduce_the_text(path: Path, kind: str, tmp_path: Path) -> None:
    text, data = _load(path)
    async with _env(kind, tmp_path) as (storage, engine):
        await _store(storage, text=text, data=data)
        assert await storage.get_semantic_model_text(data["name"], DS) == text
        for name in await storage.list_datasets(data_source=DS):
            dataset = await storage.get_dataset(name, data_source=DS)
            assert dataset is not None
            await storage.save_dataset(dataset)
        assert await storage.get_semantic_model_text(data["name"], DS) == text
        exported = await export_osi(engine=engine, datasource=DS)
        assert exported.text == text


@pytest.mark.parametrize("path", DOCUMENTS, ids=IDS)
async def test_every_corpus_document_round_trips(path: Path, tmp_path: Path) -> None:
    """Every corpus document round-trips."""
    text, data = _load(path)
    async with _env("yaml", tmp_path) as (storage, engine):
        result = await importer_module().import_osi(storage=storage, path=path, data_source=DS)
        assert result.refused == []
        assert result.stored == [data["name"]]
        exported = await export_osi(engine=engine, datasource=DS)
        assert exported.text == text
