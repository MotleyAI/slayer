"""Shared DuckDB seed, importer runners and query helpers for the importer meaning-preservation tests."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import tempfile
from collections.abc import Callable, Coroutine, Generator, Iterable, Sequence
from pathlib import Path
from typing import Any

import sqlalchemy as sa
import yaml
from sqlalchemy import event

from slayer.cli import main as cli_main
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.syntax import AggCall, Ref, parse_expr
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.cube.report import CubeConversionIssue, CubeConversionResult
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.database.client import SlayerSQLClient, SqlClient
from slayer.database import engine_factory
from slayer.sql.dialects import dialect_for_sa_backend
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import build_exec_engine, disposable_engine

DS = "ds"

ORDERS_DDL = (
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, status VARCHAR, state VARCHAR, amount INTEGER, "
    "tags VARCHAR[], customer_id INTEGER, legacy_ref INTEGER, region VARCHAR, rate DOUBLE, "
    "created_at TIMESTAMP, note VARCHAR)",
    "INSERT INTO orders VALUES "
    "(1, 'paid', 'open', 10, ['a', 'b'], 101, 102, 'n', 1.5, '2026-01-01 10:00:00', 'O''Brien\\x'), "
    "(2, 'paid', 'paid', 20, ['b'], 101, 102, 'n', 2.5, '2026-01-02 10:00:00', NULL), "
    "(3, 'open', 'x', 5, NULL, 102, 101, 's', 1.0, '2026-01-03 10:00:00', 'plain')",
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region VARCHAR)",
    "INSERT INTO customers VALUES (101, 'Ann', 'n'), (102, 'Bo', 's')",
)


def seed(directory: Path, statements: Sequence[str] = ORDERS_DDL) -> Path:
    """An on-disk DuckDB database under ``directory`` holding ``statements``."""
    path = directory / "shop.duckdb"
    with disposable_engine(f"duckdb:///{path}") as engine, engine.connect() as conn:
        for statement in statements:
            conn.execute(sa.text(statement))
        conn.commit()
    return path


@contextlib.contextmanager
def unreachable_engine(directory: Path) -> Generator[tuple[sa.Engine, list[object]]]:
    """An engine whose every connection fails, and the list of its connection attempts."""
    attempts: list[object] = []
    with disposable_engine(f"duckdb:///{directory / 'no_such_dir' / 'x.duckdb'}") as engine:
        event.listen(engine, "do_connect", lambda *args: attempts.append(args))
        yield engine, attempts


# ── importers ─────────────────────────────────────────────────────────────

def _datasource(db: Path) -> DatasourceConfig:
    return DatasourceConfig(name=DS, type="duckdb", database=str(db))


Collect = Callable[[SqlClient], Coroutine[Any, Any, None]]


def collect_over_db(collect: Collect, *, db: Path) -> None:
    """Run an importer's ``collect_metadata`` through an engine-owned client for ``db``."""
    datasource = _datasource(db)
    with tempfile.TemporaryDirectory() as tmp:
        engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=tmp))
        try:
            engine.run_sync(collect(engine.client_for(datasource)))
        finally:
            engine.close()
            engine_factory.invalidate_engine(datasource)


def collect_over_engine(*, collect: Collect, engine: sa.Engine) -> None:
    """Run an importer's ``collect_metadata`` through a client over the caller's ``engine``."""
    datasource = DatasourceConfig(name=DS, type=dialect_for_sa_backend(engine.url.get_backend_name()).sqlglot_name,
                                  connection_string=str(engine.url))
    with tempfile.TemporaryDirectory() as tmp:
        owner = SlayerQueryEngine(storage=YAMLStorage(base_dir=tmp),
                                  client_factory=lambda ds: SlayerSQLClient(datasource=ds, sa_engine=engine))
        try:
            owner.run_sync(collect(owner.client_for(datasource)))
        finally:
            owner.close()


def import_cube(cubes: Iterable[dict[str, Any]], *, views: Iterable[dict[str, Any]] = (),
                db: Path | None = None) -> CubeConversionResult:
    """Convert Cube ``cubes``/``views`` mappings, typing against ``db`` when given."""
    project = CubeProject.model_validate({"cubes": list(cubes), "views": list(views)})
    converter = CubeToSlayerConverter(project=project, data_source=DS)
    if db is not None:
        collect_over_db(lambda client: converter.collect_metadata(client=client), db=db)
    return converter.convert()


def import_dbt(semantic_models: Iterable[dict[str, Any]], *, regular_datasets: Iterable[dict[str, Any]] = (),
               metrics: Iterable[dict[str, Any]] = (), db: Path | None = None,
               target_dialect: str | None = None) -> ConversionResult:
    """Convert dbt mappings, typing against ``db`` when given."""
    project = DbtProject.model_validate({
        "semantic_models": list(semantic_models), "regular_models": list(regular_datasets), "metrics": list(metrics),
    })
    if db is None:
        return DbtToSlayerConverter(project=project, data_source=DS, target_dialect=target_dialect).convert()
    converter = DbtToSlayerConverter(project=project, data_source=DS, target_dialect=target_dialect or "duckdb")
    collect_over_db(lambda client: converter.collect_metadata(client=client), db=db)
    return converter.convert()


def import_osi(*documents: dict[str, Any], db: Path, target_dialect: str = "duckdb") -> ConversionResult:
    """Convert current-spec OSI document mappings against ``db``."""
    return convert_osi([OSIDocument.model_validate(d) for d in documents], db=db, target_dialect=target_dialect)


def convert_osi(docs: list[OSIDocument], *, db: Path, target_dialect: str = "duckdb") -> ConversionResult:
    converter = OsiToSlayerConverter(documents=docs, data_source=DS, target_dialect=target_dialect)
    collect_over_db(lambda client: converter.collect_metadata(client=client), db=db)
    return converter.convert()


# ── model inspection ──────────────────────────────────────────────────────

def datasets_of(result: CubeConversionResult | ConversionResult) -> dict[str, SlayerDataset]:
    return {m.name: m for m in result.datasets}


def column(dataset: SlayerDataset, name: str) -> DatasetField:
    col = dataset.get_field(name)
    assert col is not None, f"column {name!r} missing on {dataset.name}: {[c.name for c in dataset.fields]}"
    return col


def source_column(dataset: SlayerDataset, metric: str) -> DatasetField:
    """The column a single-aggregation measure aggregates."""
    found = dataset.get_metric(metric)
    assert found is not None, f"measure {metric!r} missing on {dataset.name}"
    parsed = parse_expr(found.text)
    assert isinstance(parsed, AggCall), found.expression
    assert isinstance(parsed.source, Ref), found.expression
    return column(dataset, parsed.source.name)


def has_metric(result: CubeConversionResult | ConversionResult, dataset: str, metric: str) -> bool:
    found = datasets_of(result).get(dataset)
    return found is not None and found.get_metric(metric) is not None


def has_column(result: CubeConversionResult | ConversionResult, dataset: str, name: str) -> bool:
    found = datasets_of(result).get(dataset)
    return found is not None and found.get_field(name) is not None


def cube_issues(result: CubeConversionResult, name: str, *, severity: str | None = None) -> list[CubeConversionIssue]:
    """Report issues about ``name`` (as member or in the message), optionally of one severity."""
    return [i for i in result.report.issues
            if (i.member == name or name in i.message) and (severity is None or i.severity == severity)]


def entries(result: ConversionResult, name: str, *, severity: str | None = None) -> list[ConversionWarning]:
    """dbt/OSI report entries about ``name`` (as metric or in the message), optionally of one severity."""
    return [e for e in (*result.unconverted_metrics, *result.warnings)
            if (e.metric_name == name or name in e.message) and (severity is None or e.severity == severity)]


def typing_notes(messages: Iterable[str]) -> list[str]:
    """Report messages saying types were not checked against the database."""
    return [m for m in messages if "database" in m.lower() and "type" in m.lower()]


# ── execution ─────────────────────────────────────────────────────────────

def _save_order(datasets: Iterable[SlayerDataset]) -> list[SlayerDataset]:
    """Join targets before the models joining them, cube views last."""
    return sorted(datasets, key=lambda m: ((m.meta or {}).get("cube_kind") == "view", bool(m.relationships)))


async def rows(*, datasets: Iterable[SlayerDataset], db: Path, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Save ``models`` (validated) over ``db`` and run one query, disposing every engine."""
    engine = await build_exec_engine(str(db), dialect="duckdb", datasets=_save_order(datasets), datasource=DS,
                                     validate=True)
    try:
        return (await engine.execute(payload)).data
    finally:
        engine.close()
        engine_factory.invalidate_engine(DatasourceConfig(name=DS, type="duckdb", database=str(db)))


async def value(*, datasets: Iterable[SlayerDataset], db: Path, dataset: str, expression: str,
                variables: dict[str, Any] | None = None) -> Any:
    """The single value of ``formula`` over ``model``."""
    payload: dict[str, Any] = {"population": dataset, "metrics": [{"metric": expression, "alias": "v"}]}
    if variables is not None:
        payload["variables"] = variables
    (row,) = await rows(datasets=datasets, db=db, payload=payload)
    return row[f"{dataset}.v"]


async def counts_by(*, datasets: Iterable[SlayerDataset], db: Path, dataset: str, dimension: str,
                    variables: dict[str, Any] | None = None) -> dict[Any, int]:
    """Row counts of ``model`` grouped by ``dimension``."""
    payload: dict[str, Any] = {"population": dataset, "dimensions": [dimension],
                               "metrics": [{"metric": "count(*)", "alias": "n"}]}
    if variables is not None:
        payload["variables"] = variables
    out = await rows(datasets=datasets, db=db, payload=payload)
    return {r[f"{dataset}.{dimension}"]: r[f"{dataset}.n"] for r in out}


async def by_id(*, datasets: Iterable[SlayerDataset], db: Path, dataset: str, dimension: str) -> list[Any]:
    """``dimension`` per row of ``model``, ordered by ``id``."""
    out = await rows(datasets=datasets, db=db, payload={
        "population": dataset, "dimensions": ["id", dimension], "order": [{"field": "id"}]})
    return [r[f"{dataset}.{dimension}"] for r in out]


async def drift_entries(*, datasets: Iterable[SlayerDataset], db: Path, base_dir: Path) -> list[dict[str, Any]]:
    """``validate_models`` findings for ``models`` saved over ``db``."""
    ds = DatasourceConfig(name=DS, type="duckdb", database=str(db))
    storage = YAMLStorage(base_dir=str(base_dir))
    await storage.save_datasource(ds)
    for model in _save_order(datasets):
        await storage.save_dataset(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        return [e.model_dump(mode="json") for e in await engine.validate_datasets(data_source=DS)]
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)


def deletes(found: Iterable[dict[str, Any]], *, dataset: str) -> list[dict[str, Any]]:
    """Findings that delete ``dataset`` or remove any of its fields or metrics."""
    return [e for e in found if e.get("dataset_name") == dataset
            and (e.get("tool") == "delete_dataset" or (e.get("remove") or {}))]


# ── CLI ───────────────────────────────────────────────────────────────────

def run_cli(*argv: str) -> int:
    """Run ``slayer <argv>``, returning its exit code."""
    original = sys.argv
    sys.argv = ["slayer", *argv]
    try:
        cli_main()
    except SystemExit as exc:  # NOSONAR(S5754) — the CLI's exit code is the result
        return int(exc.code or 0)
    finally:
        sys.argv = original
    return 0


def write_yaml(path: Path, mapping: Any) -> Path:
    os.makedirs(path.parent, exist_ok=True)
    path.write_text(yaml.safe_dump(mapping, sort_keys=False), encoding="utf-8")
    return path


def save_datasource(store: Path, *, name: str = DS, database: Path) -> None:
    asyncio.run(YAMLStorage(base_dir=str(store)).save_datasource(
        DatasourceConfig(name=name, type="duckdb", database=str(database))))


def stored_dataset(store: Path, name: str) -> SlayerDataset:
    model = asyncio.run(YAMLStorage(base_dir=str(store)).get_dataset(name))
    assert model is not None, f"model {name!r} not saved under {store}"
    return model


def cube_report_messages(store: Path) -> list[dict[str, Any]]:
    return json.loads((store / "cube_import_report.json").read_text(encoding="utf-8"))["issues"]
