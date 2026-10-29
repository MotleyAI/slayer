"""Shared DuckDB seed, importer runners and query helpers for the importer meaning-preservation tests."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
from collections.abc import Generator, Iterable, Sequence
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import yaml

from slayer.cli import main as cli_main
from slayer.core.models import Column, DatasourceConfig, SlayerModel
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
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import build_exec_engine, disposable_engine

DS = "ds"

#: Reading a physical column whose name a different column holds; DEV-2175 lifts these.
MASKED = pytest.mark.xfail(strict=True, reason="DEV-2175: physical name held by a different column")

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
def sa_engine(db: Path) -> Generator[sa.Engine]:
    with disposable_engine(f"duckdb:///{db}") as engine:
        yield engine


# ── importers ─────────────────────────────────────────────────────────────

def import_cube(cubes: Iterable[dict[str, Any]], *, views: Iterable[dict[str, Any]] = (),
                db: Path | None = None) -> CubeConversionResult:
    """Convert Cube ``cubes``/``views`` mappings, typing against ``db`` when given."""
    project = CubeProject.model_validate({"cubes": list(cubes), "views": list(views)})
    if db is None:
        return CubeToSlayerConverter(project=project, data_source=DS).convert()
    with sa_engine(db) as engine:
        return CubeToSlayerConverter(project=project, data_source=DS, sa_engine=engine).convert()


def import_dbt(semantic_models: Iterable[dict[str, Any]], *, regular_models: Iterable[dict[str, Any]] = (),
               metrics: Iterable[dict[str, Any]] = (), db: Path | None = None,
               target_dialect: str | None = None) -> ConversionResult:
    """Convert dbt mappings, typing against ``db`` when given."""
    project = DbtProject.model_validate({
        "semantic_models": list(semantic_models), "regular_models": list(regular_models), "metrics": list(metrics),
    })
    if db is None:
        return DbtToSlayerConverter(project=project, data_source=DS, target_dialect=target_dialect).convert()
    with sa_engine(db) as engine:
        return DbtToSlayerConverter(project=project, data_source=DS, sa_engine=engine,
                                    target_dialect=target_dialect or "duckdb").convert()


def import_osi(*documents: dict[str, Any], db: Path, target_dialect: str = "duckdb") -> ConversionResult:
    """Convert current-spec OSI document mappings against ``db``."""
    docs = [OSIDocument.model_validate(d) for d in documents]
    with sa_engine(db) as engine:
        return OsiToSlayerConverter(documents=docs, data_source=DS, sa_engine=engine,
                                    target_dialect=target_dialect).convert()


def convert_osi(docs: list[OSIDocument], *, db: Path, target_dialect: str = "duckdb") -> ConversionResult:
    with sa_engine(db) as engine:
        return OsiToSlayerConverter(documents=docs, data_source=DS, sa_engine=engine,
                                    target_dialect=target_dialect).convert()


# ── model inspection ──────────────────────────────────────────────────────

def models_of(result: CubeConversionResult | ConversionResult) -> dict[str, SlayerModel]:
    return {m.name: m for m in result.models}


def column(model: SlayerModel, name: str) -> Column:
    col = model.get_column(name)
    assert col is not None, f"column {name!r} missing on {model.name}: {[c.name for c in model.columns]}"
    return col


def source_column(model: SlayerModel, measure: str) -> Column:
    """The column a single-aggregation measure aggregates."""
    found = model.get_measure(measure)
    assert found is not None, f"measure {measure!r} missing on {model.name}"
    parsed = parse_expr(found.formula)
    assert isinstance(parsed, AggCall), found.formula
    assert isinstance(parsed.source, Ref), found.formula
    return column(model, parsed.source.name)


def has_measure(result: CubeConversionResult | ConversionResult, model: str, measure: str) -> bool:
    found = models_of(result).get(model)
    return found is not None and found.get_measure(measure) is not None


def has_column(result: CubeConversionResult | ConversionResult, model: str, name: str) -> bool:
    found = models_of(result).get(model)
    return found is not None and found.get_column(name) is not None


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

def _save_order(models: Iterable[SlayerModel]) -> list[SlayerModel]:
    """Join targets before the models joining them, cube views last."""
    return sorted(models, key=lambda m: ((m.meta or {}).get("cube_kind") == "view", bool(m.joins)))


async def rows(*, models: Iterable[SlayerModel], db: Path, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Save ``models`` (validated) over ``db`` and run one query, disposing every engine."""
    engine = await build_exec_engine(str(db), dialect="duckdb", models=_save_order(models), datasource=DS,
                                     validate=True)
    try:
        return (await engine.execute(payload)).data
    finally:
        engine.close()
        engine_factory.invalidate_engine(DatasourceConfig(name=DS, type="duckdb", database=str(db)))


async def value(*, models: Iterable[SlayerModel], db: Path, model: str, formula: str,
                variables: dict[str, Any] | None = None) -> Any:
    """The single value of ``formula`` over ``model``."""
    payload: dict[str, Any] = {"source_model": model, "measures": [{"formula": formula, "name": "v"}]}
    if variables is not None:
        payload["variables"] = variables
    (row,) = await rows(models=models, db=db, payload=payload)
    return row[f"{model}.v"]


async def counts_by(*, models: Iterable[SlayerModel], db: Path, model: str, dimension: str,
                    variables: dict[str, Any] | None = None) -> dict[Any, int]:
    """Row counts of ``model`` grouped by ``dimension``."""
    payload: dict[str, Any] = {"source_model": model, "dimensions": [dimension],
                               "measures": [{"formula": "count(*)", "name": "n"}]}
    if variables is not None:
        payload["variables"] = variables
    out = await rows(models=models, db=db, payload=payload)
    return {r[f"{model}.{dimension}"]: r[f"{model}.n"] for r in out}


async def by_id(*, models: Iterable[SlayerModel], db: Path, model: str, dimension: str) -> list[Any]:
    """``dimension`` per row of ``model``, ordered by ``id``."""
    out = await rows(models=models, db=db, payload={
        "source_model": model, "dimensions": ["id", dimension], "order": [{"column": "id"}]})
    return [r[f"{model}.{dimension}"] for r in out]


async def drift_entries(*, models: Iterable[SlayerModel], db: Path, base_dir: Path) -> list[dict[str, Any]]:
    """``validate_models`` findings for ``models`` saved over ``db``."""
    ds = DatasourceConfig(name=DS, type="duckdb", database=str(db))
    storage = YAMLStorage(base_dir=str(base_dir))
    await storage.save_datasource(ds)
    for model in _save_order(models):
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        return [e.model_dump(mode="json") for e in await engine.validate_models(data_source=DS)]
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)


def deletes(found: Iterable[dict[str, Any]], *, model: str) -> list[dict[str, Any]]:
    """Findings that delete ``model`` or remove any of its columns or measures."""
    return [e for e in found if e.get("model_name") == model
            and (e.get("tool") == "delete_model" or (e.get("remove") or {}))]


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


def stored_model(store: Path, name: str) -> SlayerModel:
    model = asyncio.run(YAMLStorage(base_dir=str(store)).get_model(name))
    assert model is not None, f"model {name!r} not saved under {store}"
    return model


def cube_report_messages(store: Path) -> list[dict[str, Any]]:
    return json.loads((store / "cube_import_report.json").read_text(encoding="utf-8"))["issues"]
