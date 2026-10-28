"""Shared builders, seeding and query helpers for the OSI importer tests."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import yaml

import slayer.osi.converter as osi_converter_module

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.sql import engine_factory
from tests._engine_helpers import build_exec_engine, disposable_engine

FIXTURES = Path(__file__).parent / "fixtures" / "osi"
SCHEMA_PATH = FIXTURES / "ossie-schema.json"
DATASOURCE = "osids"

SHOP_DDL = [
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, "
    "o_totalprice DECIMAL(12,2), amount DOUBLE, quantity INTEGER, tip DOUBLE, status VARCHAR, "
    "region VARCHAR, is_paid BOOLEAN, total_cents INTEGER, ordered_on DATE, ordered_at TIMESTAMP, "
    "order_year INTEGER, cust_code VARCHAR, first_name VARCHAR, last_name VARCHAR, "
    "\"LegalType\" VARCHAR, o_comment VARCHAR)",
    "CREATE TABLE customers (c_custkey INTEGER PRIMARY KEY, segment VARCHAR, discount DOUBLE, "
    "cust_code VARCHAR, email VARCHAR)",
    "CREATE TABLE visits (visit_id INTEGER PRIMARY KEY, status VARCHAR, customer_id VARCHAR)",
    "INSERT INTO orders VALUES "
    "(1, 101, 10.00, 10, 1, 1, 'completed', 'north', TRUE, 1000, '2026-01-05', '2026-01-05 10:00:00', "
    "2026, 'ab', 'Ann', 'Lee', 'llc', 'x'), "
    "(2, 101, 5.00, 5, 2, NULL, 'open', NULL, FALSE, 500, '2026-01-06', '2026-01-06 11:00:00', "
    "2026, 'cd', 'Bo', NULL, 'gmbh', NULL), "
    "(3, 102, 7.00, 7, 3, 2, 'completed', 'south', TRUE, 700, '2026-02-01', '2026-02-01 12:00:00', "
    "2026, 'ef', 'Cy', 'Kim', 'llc', NULL)",
    "INSERT INTO customers VALUES (101, 'b2b', 1.0, 'AB', 'a@x.io'), (102, 'b2c', 0.5, 'EF', 'b@x.io')",
    "INSERT INTO visits VALUES (1, 'completed', 'c1'), (2, 'completed', 'c1'), (3, 'open', 'c2'), "
    "(4, 'completed', 'c3')",
]

_URL_SCHEME = {"duckdb": "duckdb", "sqlite": "sqlite"}
_DB_SUFFIX = {"duckdb": ".duckdb", "sqlite": ".db"}


def seed_db(*, directory: Path, dialect: str = "duckdb", statements: Sequence[str] = SHOP_DDL) -> Path:
    """Create and seed an on-disk ``dialect`` database under ``directory``."""
    path = directory / f"shop{_DB_SUFFIX[dialect]}"
    with disposable_engine(db_url(path=path, dialect=dialect)) as engine, engine.connect() as conn:
        for statement in statements:
            conn.execute(sa.text(statement))
        conn.commit()
    return path


def disable_type_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every derived-expression type probe fail, as on a database that cannot report one."""
    def _fail(**_: Any) -> dict[str, str]:
        raise RuntimeError("probe unavailable")

    monkeypatch.setattr(osi_converter_module, "get_column_types_sync", _fail)


def db_url(*, path: Path, dialect: str) -> str:
    return f"{_URL_SCHEME[dialect]}:///{path}"


def f(name: str, sql: str | None = None, **extra: Any) -> dict[str, Any]:
    """A field whose expression is the portable top-level ``sql`` (default: its own name)."""
    return {"name": name, "expression": {"sql": sql if sql is not None else name}, **extra}


def f_dialects(name: str, *entries: tuple[str, str], sql: str | None = None, **extra: Any) -> dict[str, Any]:
    """A field with ``dialects`` entries ``(dialect, expression)`` and an optional top-level ``sql``."""
    expression: dict[str, Any] = {"dialects": [{"dialect": d, "expression": e} for d, e in entries]}
    if sql is not None:
        expression["sql"] = sql
    return {"name": name, "expression": expression, **extra}


def m(name: str, sql: str, **extra: Any) -> dict[str, Any]:
    """A metric whose expression is the portable top-level ``sql``."""
    return {"name": name, "expression": {"sql": sql}, **extra}


def ds(name: str, *, source: str | None = None, fields: list[dict[str, Any]] | None = None,
       **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "source": source or name, **extra}
    if fields is not None:
        out["fields"] = fields
    return out


def rel(name: str, *, src: str, to: str, from_columns: list[str], to_columns: list[str]) -> dict[str, Any]:
    return {"name": name, "from": src, "to": to, "from_columns": from_columns, "to_columns": to_columns}


def flat(datasets: list[dict[str, Any]], *, relationships: list[dict[str, Any]] | None = None,
         metrics: list[dict[str, Any]] | None = None, name: str = "shop") -> dict[str, Any]:
    """A current-spec (flat, single-model) document mapping."""
    out: dict[str, Any] = {"version": "0.2.0.dev0", "name": name, "datasets": datasets}
    if relationships is not None:
        out["relationships"] = relationships
    if metrics is not None:
        out["metrics"] = metrics
    return out


def legacy(*models: dict[str, Any]) -> dict[str, Any]:
    """A legacy-wrapper document mapping holding ``models`` as its ``semantic_model`` array."""
    return {"version": "0.2.0.dev0", "semantic_model": list(models)}


def write_yaml(path: Path, mapping: Any) -> Path:
    path.write_text(yaml.safe_dump(mapping, sort_keys=False), encoding="utf-8")
    return path


def documents(*mappings: dict[str, Any]) -> list[OSIDocument]:
    """Validate flat document mappings as current-spec documents."""
    return [OSIDocument.model_validate(mapping) for mapping in mappings]


def convert(*, db: Path, docs: list[OSIDocument], db_dialect: str = "duckdb", **kwargs: Any) -> ConversionResult:
    """Run the converter over ``docs`` against the seeded ``db`` (target defaults to its dialect)."""
    kwargs.setdefault("target_dialect", db_dialect)
    with disposable_engine(db_url(path=db, dialect=db_dialect)) as engine:
        return OsiToSlayerConverter(
            documents=docs, data_source=DATASOURCE, sa_engine=engine, **kwargs,
        ).convert()


def by_name(result: ConversionResult) -> dict[str, SlayerModel]:
    return {model.name: model for model in result.models}


def cols(model: SlayerModel) -> dict[str, Column]:
    return {c.name: c for c in model.columns}


def visible(model: SlayerModel) -> set[str]:
    return {c.name for c in model.columns if not c.hidden}


def hidden(model: SlayerModel) -> list[Column]:
    return [c for c in model.columns if c.hidden]


def entries(result: ConversionResult) -> list[ConversionWarning]:
    return [*result.unconverted_metrics, *result.warnings]


def mentions(result: ConversionResult, name: str, *, severity: str | None = None) -> list[ConversionWarning]:
    """Report entries about ``name`` (as their metric or in their message), optionally of one severity."""
    return [
        e for e in entries(result)
        if (e.metric_name == name or name in e.message) and (severity is None or e.severity == severity)
    ]


def unconverted(result: ConversionResult, metric: str) -> bool:
    """True when ``metric`` is reported as unconverted and is on no model."""
    on_a_model = any(meas.name == metric for model in result.models for meas in model.measures)
    return not on_a_model and any(e.metric_name == metric for e in result.unconverted_metrics)


def measure_home(result: ConversionResult, measure: str) -> SlayerModel:
    homes = [model for model in result.models if any(meas.name == measure for meas in model.measures)]
    assert len(homes) == 1, f"measure {measure!r} on {[h.name for h in homes]}"
    return homes[0]


async def query(*, result: ConversionResult, db: Path, payload: dict[str, Any],
                dialect: str = "duckdb") -> list[dict[str, Any]]:
    """Save the imported models over ``db`` and run one query, disposing every engine."""
    engine = await build_exec_engine(str(db), dialect=dialect, models=result.models, datasource=DATASOURCE)
    try:
        response = await engine.execute(payload)
    finally:
        engine.close()
        engine_factory.invalidate_engine(DatasourceConfig(name=DATASOURCE, type=dialect, database=str(db)))
    return response.data


async def scalar(*, result: ConversionResult, db: Path, model: str, formula: str,
                 dialect: str = "duckdb") -> Any:
    """The single value of ``formula`` over ``model``."""
    rows = await query(result=result, db=db, dialect=dialect,
                       payload={"source_model": model, "measures": [{"formula": formula, "name": "v"}]})
    assert len(rows) == 1
    return rows[0][f"{model}.v"]
