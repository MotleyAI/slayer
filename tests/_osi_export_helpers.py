"""Export, re-import, document-navigation and result-comparison helpers for the Ossie export tests."""

from __future__ import annotations

import contextlib
import io
import json
import math
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import jsonschema
import sqlalchemy as sa
import sqlglot
import yaml
from pydantic import BaseModel

from slayer.cli import _import_osi_workflow
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionResult, ConversionWarning
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.dialects import read, render, select_expression
from slayer.osi.exporter import OsiExportResult, export_osi
from slayer.osi.models import OSIDocument, OSIExpression
from slayer.osi.source import parse_source
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine
from tests._osi_export_fixtures import DATASOURCE, EXPORT_DDL
from tests._osi_helpers import SCHEMA_PATH, db_url, run_converter, seed_db

SLAYER = "SLAYER"


class Env(BaseModel):
    """A seeded database and a storage directory holding its datasource."""

    db: Path
    store: Path
    dialect: str
    datasource: str = DATASOURCE

    @property
    def config(self) -> DatasourceConfig:
        return DatasourceConfig(name=self.datasource, type=self.dialect, database=str(self.db))


async def make_env(directory: Path, *, dialect: str = "duckdb", statements: list[str] | None = None,
                   datasource: str = DATASOURCE, description: str | None = None) -> Env:
    directory.mkdir(parents=True, exist_ok=True)
    db = seed_db(directory=directory, dialect=dialect, statements=EXPORT_DDL if statements is None else statements)
    env = Env(db=db, store=directory / "store", dialect=dialect, datasource=datasource)
    await YAMLStorage(base_dir=str(env.store)).save_datasource(
        env.config.model_copy(update={"description": description}))
    return env


async def fresh_store(env: Env, name: str) -> Env:
    """``env``'s database with an empty storage directory ``name`` next to it."""
    other = env.model_copy(update={"store": env.store.parent / name})
    original = await YAMLStorage(base_dir=str(env.store)).get_datasource(env.datasource)
    await YAMLStorage(base_dir=str(other.store)).save_datasource(original or env.config)
    return other


def _engine(env: Env) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=YAMLStorage(base_dir=str(env.store)))


def _close(engine: SlayerQueryEngine, env: Env) -> None:
    engine.close()
    engine_factory.invalidate_engine(env.config)


async def save(env: Env, *models: SlayerModel) -> None:
    """Save ``models`` through the engine, validating each."""
    engine = _engine(env)
    try:
        for model in models:
            await engine.save_model(model.model_copy(update={"data_source": env.datasource}))
    finally:
        _close(engine, env)


async def export(env: Env, *, models: list[str] | None = None, extensions: bool = True) -> OsiExportResult:
    engine = _engine(env)
    try:
        return await export_osi(engine=engine, datasource=env.datasource, models=models, extensions=extensions)
    finally:
        _close(engine, env)


async def export_models(env: Env, *models: SlayerModel, extensions: bool = True,
                        subset: list[str] | None = None) -> OsiExportResult:
    await save(env, *models)
    return await export(env, models=subset, extensions=extensions)


async def reimport(env: Env, document: dict[str, Any], *, name: str = "reimported") -> tuple[Env, str]:
    """Run ``import-osi`` on ``document`` into a fresh store over ``env``'s database; return it and the output."""
    target = await fresh_store(env, name)
    path = target.store.parent / f"{name}.osi.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    args = SimpleNamespace(osi_path=str(path), datasource=env.datasource, dialect=None, storage=str(target.store),
                           models_dir=None)
    engine = _engine(target)
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            await _import_osi_workflow(args, engine)
    finally:
        _close(engine, target)
    return target, buffer.getvalue()


def convert_document(env: Env, document: dict[str, Any]) -> ConversionResult:
    """The pure importer over ``document`` against ``env``'s database."""
    return run_converter(
        OsiToSlayerConverter(documents=[OSIDocument.model_validate(document)], data_source=env.datasource,
                             target_dialect=env.dialect),
        datasource=env.config,
    )


async def stored(env: Env, name: str) -> SlayerModel:
    model = await YAMLStorage(base_dir=str(env.store)).get_model(name, data_source=env.datasource)
    assert model is not None, f"model {name!r} not in {env.store}"
    return model


async def rows(env: Env, payload: dict[str, Any] | str) -> list[dict[str, Any]]:
    engine = _engine(env)
    try:
        return (await engine.execute(payload)).data
    finally:
        _close(engine, env)


def run_sql(env: Env, sql: str) -> list[tuple[Any, ...]]:
    with disposable_engine(db_url(path=env.db, dialect=env.dialect)) as engine, engine.connect() as conn:
        return [tuple(r) for r in conn.execute(sa.text(sql))]


# ─────────────────────────── document navigation ───────────────────────────

def validate(document: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))).validate(document)


def dataset_names(document: dict[str, Any]) -> list[str]:
    return [d["name"] for d in document["datasets"]]


def dataset(document: dict[str, Any], name: str) -> dict[str, Any]:
    hits = [d for d in document["datasets"] if d["name"] == name]
    assert len(hits) == 1, f"dataset {name!r}: {dataset_names(document)}"
    return hits[0]


def field(ds: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((x for x in ds.get("fields") or [] if x["name"] == name), None)


def metric(container: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((x for x in container.get("metrics") or [] if x["name"] == name), None)


def all_metrics(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [*(m for d in document["datasets"] for m in d.get("metrics") or []), *(document.get("metrics") or [])]


def relationships(document: dict[str, Any]) -> list[dict[str, Any]]:
    return list(document.get("relationships") or [])


def entities(document: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Every dataset, field, metric and relationship of ``document``."""
    for ds in document["datasets"]:
        yield ds
        yield from ds.get("fields") or []
        yield from ds.get("metrics") or []
    yield from document.get("metrics") or []
    yield from relationships(document)


def slayer_entries(entity: dict[str, Any]) -> list[dict[str, Any]]:
    return [e for e in entity.get("custom_extensions") or [] if e["vendor_name"] == SLAYER]


def payload(entity: dict[str, Any]) -> Any:
    [entry] = slayer_entries(entity)
    return json.loads(entry["data"])


def carries(entity: dict[str, Any], value: str) -> bool:
    """Whether ``entity``'s SLAYER payload holds the string ``value`` anywhere."""
    def walk(node: Any) -> bool:
        if isinstance(node, dict):
            return any(walk(v) for v in node.values())
        if isinstance(node, list):
            return any(walk(v) for v in node)
        return node == value
    return walk(payload(entity))


def expression_sql(entity: dict[str, Any], *, target: str) -> str:
    """The importer's reading of ``entity``'s expression, rendered for ``target``."""
    selection = select_expression(OSIExpression.model_validate(entity["expression"]), override=None, target=target)
    assert selection.candidate is not None, selection.reason
    return render(selection.candidate, read(selection.candidate), target=target)


def same_sql(actual: str, expected: str, *, read_dialect: str = "snowflake") -> bool:
    def norm(text: str) -> str:
        return sqlglot.parse_one(text, read=read_dialect).sql(dialect=read_dialect)
    return norm(actual) == norm(expected)


def eval_field(env: Env, ds: dict[str, Any], name: str, *, order_by: str) -> list[Any]:
    """``ds``'s field ``name`` evaluated over its source, ordered by the source column ``order_by``."""
    entity = field(ds, name)
    assert entity is not None, f"no field {name!r} on {ds['name']!r}"
    target = sqlglot_target(env)
    parsed = parse_source(ds["source"])
    source = f"({parsed.query}) AS _src" if parsed.is_query else ds["source"]
    return [r[0] for r in run_sql(env, f"SELECT {expression_sql(entity, target=target)} AS v FROM {source} "
                                       f"ORDER BY {order_by}")]


def sqlglot_target(env: Env) -> str:
    return {"duckdb": "duckdb", "sqlite": "sqlite", "postgres": "postgres"}[env.dialect]


# ─────────────────────────────── report ────────────────────────────────────

def mentions(result: OsiExportResult | ConversionResult, name: str, *, severity: str | None = None,
             category: str | None = None) -> list[ConversionWarning]:
    """Report entries naming ``name`` (as model, metric, or in the message), optionally filtered."""
    return [
        e for e in result.entries
        if name in (e.model_name, e.metric_name) or name in e.message
        if severity is None or e.severity == severity
        if category is None or e.category == category
    ]


# ───────────────────────────── result comparison ───────────────────────────

def norm_value(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        return None if math.isnan(number) else round(number, 6)
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:19].replace("T", " ")
    return str(value)


def normalized(data: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    """Rows as sorted, value-normalized tuples, so equal results compare equal across stores and dialects."""
    return sorted((tuple(sorted((k, norm_value(v)) for k, v in row.items())) for row in data), key=repr)


def doc_text(document: dict[str, Any]) -> str:
    return yaml.safe_dump(document, sort_keys=False)
