"""Shared builders and seeding helpers for the OSI importer tests."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

import sqlalchemy as sa
import yaml

from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionResult
from slayer.osi.models import OSIDocument
from slayer.database import engine_factory
from slayer.database.client import SqlClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine

FIXTURES = Path(__file__).parent / "fixtures" / "osi"
SCHEMA_PATH = Path(__file__).parent / "fixtures" / "ossie_corpus" / "core-spec" / "ossie-schema.json"
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


class MetadataConverter(Protocol):
    """An importer that reads the database in ``collect_metadata`` and converts purely."""

    async def collect_metadata(self, *, client: SqlClient) -> None: ...

    def convert(self) -> ConversionResult: ...


def run_converter(converter: MetadataConverter, *, datasource: DatasourceConfig) -> ConversionResult:
    """Collect ``converter``'s metadata through an engine-owned client for ``datasource``, then convert."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=tmp))
        try:
            engine.run_sync(converter.collect_metadata(client=engine.client_for(datasource)))
        finally:
            engine.close()
            engine_factory.invalidate_engine(datasource)
    return converter.convert()


def db_url(*, path: Path, dialect: str) -> str:
    return f"{_URL_SCHEME[dialect]}:///{path}"


def f(name: str, sql: str | None = None, **extra: Any) -> dict[str, Any]:
    """A field whose expression is the portable top-level ``sql`` (default: its own name)."""
    return {"name": name, "expression": {"sql": sql if sql is not None else name}, **extra}


def m(name: str, sql: str, **extra: Any) -> dict[str, Any]:
    """A metric whose expression is the portable top-level ``sql``."""
    return {"name": name, "expression": {"sql": sql}, **extra}


def ds(name: str, *, source: str | None = None, fields: list[dict[str, Any]] | None = None,
       **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "source": source or name, **extra}
    if fields is not None:
        out["fields"] = fields
    return out


def flat(datasets: list[dict[str, Any]], *, relationships: list[dict[str, Any]] | None = None,
         metrics: list[dict[str, Any]] | None = None, name: str = "shop") -> dict[str, Any]:
    """A current-spec (flat, single-model) document mapping."""
    out: dict[str, Any] = {"version": "0.3.0.dev0", "name": name, "datasets": datasets}
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


def by_name(result: ConversionResult) -> dict[str, SlayerDataset]:
    return {model.name: model for model in result.datasets}


def cols(dataset: SlayerDataset) -> dict[str, DatasetField]:
    return {c.name: c for c in dataset.fields}
