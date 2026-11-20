"""Shared seeds, documents and accessors for the native Ossie semantic-model document tests.

The Appendix B seed is ``tests/_dev2277_fixtures.py``'s (datasource ``shop`` here). The document ``sales`` declares
``orders`` (metric ``total_revenue = SUM(amount)``) and ``customers``, the relationship ``orders_to_customers`` and
the model-scoped metric ``revenue_per_customer = AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)``
(650 / 4 = 162.5 on the seed).

New names are reached through the accessors below, so a name the implementation has not added yet fails the one
test that needs it rather than the whole module's collection.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import shutil
import subprocess
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core import errors
from slayer.core.models import DatasetField, DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2277_fixtures import SEED_TABLES
from tests._engine_helpers import owned_query_engine

DS = "shop"
STORAGES = ("yaml", "sqlite")
CORPUS = Path(__file__).parent / "fixtures" / "ossie_corpus"

SALES = """\
version: 0.3.0.dev0
name: sales
description: Orders and customers.
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: {sql: order_id}
        datatype: String
      - name: customer_id
        expression: {sql: customer_id}
        datatype: String
      - name: amount
        expression: {sql: amount}
        datatype: Float
      - name: status
        expression: {sql: status}
        datatype: String
    metrics:
      - name: total_revenue
        expression: {sql: SUM(amount)}
  # owned by finance
  - name: customers
    source: customers
    primary_key: [customer_id]
    fields:
      - name: customer_id
        expression: {sql: customer_id}
        datatype: String
      - name: region
        expression: {sql: region}
        datatype: String
relationships:
  - name: orders_to_customers
    from: orders
    to: customers
    from_columns: [customer_id]
    to_columns: [customer_id]
metrics:
  - name: revenue_per_customer
    expression: {sql: AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)}
    description: Revenue per customer.
    ai_context: {synonyms: [ARPU]}
"""


def document_text(*, name: str = "sales", datasets: list[dict[str, Any]], relationships: list[dict[str, Any]] | None = None,
                  metrics: list[dict[str, Any]] | None = None, **extra: Any) -> str:
    """A flat 0.3.0.dev0 document as YAML text."""
    doc: dict[str, Any] = {"version": "0.3.0.dev0", "name": name, **extra, "datasets": datasets}
    if relationships is not None:
        doc["relationships"] = relationships
    if metrics is not None:
        doc["metrics"] = metrics
    return yaml.safe_dump(doc, sort_keys=False)


def field(name: str, sql: str | None = None, *, datatype: str | None = "String", **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "expression": {"sql": sql or name}}
    if datatype is not None:
        out["datatype"] = datatype
    return {**out, **extra}


def metric(name: str, sql: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "expression": {"sql": sql}, **extra}


def dataset(name: str, *, fields: list[dict[str, Any]], source: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"name": name, "source": source or name, **extra, "fields": fields}


def relationship(name: str, *, src: str, to: str, from_columns: list[str], to_columns: list[str]) -> dict[str, Any]:
    return {"name": name, "from": src, "to": to, "from_columns": from_columns, "to_columns": to_columns}


# --------------------------------------------------------------------------- #
# Accessors for names this change adds
# --------------------------------------------------------------------------- #
def error(name: str) -> type[Exception]:
    """A new typed error of ``slayer.core.errors``."""
    return getattr(errors, name)


def semantic_model_module() -> Any:
    return importlib.import_module("slayer.core.semantic_model")


def importer_module() -> Any:
    return importlib.import_module("slayer.osi.importer")


def is_untyped(dataset_field: DatasetField) -> bool:
    """The decomposed field carries the unknown datatype."""
    return dataset_field.datatype is None


# --------------------------------------------------------------------------- #
# Storage and engines
# --------------------------------------------------------------------------- #
def make_storage(kind: str, base: Path) -> StorageBackend:
    if kind == "yaml":
        return YAMLStorage(base_dir=str(base / "store"))
    return SQLiteStorage(db_path=str(base / "store.db"))


def document_path(base: Path, *, name: str, data_source: str = DS) -> Path:
    """Where YAML storage keeps document ``name`` (the storage made by ``make_storage("yaml", base)``)."""
    return base / "store" / "semantic_models" / data_source / f"{name}.yaml"


def place_document(base: Path, text: str, *, name: str, data_source: str = DS) -> Path:
    """Write a document file by hand, as a user would."""
    path = document_path(base, name=name, data_source=data_source)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


async def store_document(storage: StorageBackend, text: str, *, data_source: str = DS) -> None:
    """Save ``text`` as a document through storage."""
    name = yaml.safe_load(text)["name"]
    await storage.save_semantic_model_text(data_source=data_source, name=name, text=text)


@contextlib.asynccontextmanager
async def document_engine(*, kind: str = "yaml", backend: str = "duckdb", documents: tuple[str, ...] = (SALES,),
                          tables: list[TableSpec] | None = None, base: Path | None = None,
                          ) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over the Appendix B seed with ``documents`` stored in datasource ``shop``."""
    with tempfile.TemporaryDirectory() as tmp:
        root = base or Path(tmp)
        db_path = os.path.join(tmp, f"seed.{'duckdb' if backend == 'duckdb' else 'db'}")
        seed_backend(backend, db_path, tables or SEED_TABLES)
        storage = make_storage(kind, root)
        ds = DatasourceConfig(name=DS, type=backend, database=db_path)
        async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
            for text in documents:
                await store_document(storage, text)
            yield engine


def floats(rows: list[dict[str, Any]], key: str) -> list[float | None]:
    return [None if r[key] is None else float(r[key]) for r in rows]


def upstream_validate(paths: list[Path], *, tmp: Path) -> subprocess.CompletedProcess[str]:
    """Run the vendored upstream ``validate.py`` over ``paths`` from a copy of the corpus under ``tmp``; skip without uv."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; the upstream Ossie validator runs through `uv run --script`")
    corpus = tmp / "ossie_corpus_copy"
    if not corpus.exists():
        for part in ("validation", "core-spec", "python"):
            shutil.copytree(CORPUS / part, corpus / part)
    return subprocess.run([uv, "run", "--script", str(corpus / "validation" / "validate.py"), *map(str, paths)],
                          capture_output=True, text=True, check=False, timeout=300)
