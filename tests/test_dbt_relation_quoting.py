"""dbt relation names follow dbt's quoting policy and ``sources:`` declarations (spec: importers/dbt-import)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from sqlglot import exp

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.dbt.parser import parse_dbt_project
from slayer.ingest_report import ConversionResult
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import denoted_name
from slayer.sql.render.parse import parse_query, parse_table
from tests._dev2171_fixtures import column, entries, models_of, write_yaml
from tests._engine_helpers import _engine_generate, disposable_engine

DS = "ds"
ITEMS = {"name": "items", "model": "ref('OrderItems')", "dimensions": [{"name": "qty", "type": "categorical"}]}
SRC = {"name": "m", "model": "ref('src')", "dimensions": [{"name": "id", "type": "categorical"}]}
ORDERS_SOURCE = {"name": "raw", "database": "Analytics", "schema": "Raw",
                 "tables": [{"name": "orders", "identifier": "Orders"}]}
DEFAULT_QUOTING = {"postgres": True, "snowflake": False}


def _import(tmp_path: Path, *, dialect: str, semantic_models: list[dict[str, Any]], quoting: Any = None,
            sources: list[dict[str, Any]] | None = None, sql: dict[str, str] | None = None,
            schema_files: dict[str, dict[str, Any]] | None = None, metrics: list[dict[str, Any]] | None = None,
            ) -> ConversionResult:
    root = tmp_path / "dbt"
    write_yaml(root / "dbt_project.yml", {"name": "shop", "version": "1.0", "config-version": 2,
                                          **({"quoting": quoting} if quoting is not None else {})})
    write_yaml(root / "models" / "schema.yml", {"version": 2, "semantic_models": semantic_models,
                                                "sources": sources or [], "metrics": metrics or []})
    for name, body in (schema_files or {}).items():
        write_yaml(root / "models" / name, body)
    for name, body in (sql or {}).items():
        (root / "models" / f"{name}.sql").write_text(body, encoding="utf-8")
    return DbtToSlayerConverter(project=parse_dbt_project(str(root)), data_source=DS, target_dialect=dialect).convert()


def _relation(model: SlayerModel, *, dialect: str) -> list[str]:
    """The catalog names of the one relation ``model`` reads, as SLayer reads its stored text."""
    sql_dialect = get_dialect(dialect)
    if model.sql_table is not None:
        table = parse_table(name=model.sql_table, dialect=sql_dialect)
    else:
        assert model.sql is not None
        (table,) = parse_query(sql=model.sql, target_dialect=sql_dialect).find_all(exp.Table)
    return [denoted_name(p, dialect=dialect) for p in cast(list[exp.Identifier], table.parts)]


async def _generated_relation(model: SlayerModel, *, dialect: str) -> list[tuple[str, bool]]:
    sql = await _engine_generate(query=SlayerQuery.model_validate({"source_model": model.name, "dimensions": ["qty"]}),
                                 model=model, dialect=dialect)
    (table,) = parse_query(sql=sql, target_dialect=get_dialect(dialect)).find_all(exp.Table)
    return [(p.name, p.quoted) for p in cast(list[exp.Identifier], table.parts)]


# ── quoting policy ────────────────────────────────────────────────────────

def test_postgres_default_keeps_the_case(tmp_path: Path) -> None:
    result = _import(tmp_path, dialect="postgres", semantic_models=[ITEMS])
    assert _relation(models_of(result)["items"], dialect="postgres") == ["OrderItems"]


def test_project_turns_identifier_quoting_off(tmp_path: Path) -> None:
    result = _import(tmp_path, dialect="postgres", semantic_models=[ITEMS], quoting={"identifier": False})
    assert _relation(models_of(result)["items"], dialect="postgres") == ["orderitems"]


def test_models_directory_reads_the_project_quoting(tmp_path: Path) -> None:
    _import(tmp_path, dialect="postgres", semantic_models=[ITEMS], quoting={"identifier": False})
    project = parse_dbt_project(str(tmp_path / "dbt" / "models"))
    assert project.quoting == {"identifier": False}


def test_a_nested_model_path_reads_the_project_quoting(tmp_path: Path) -> None:
    root = tmp_path / "dbt"
    write_yaml(root / "dbt_project.yml", {"name": "shop", "version": "1.0", "config-version": 2,
                                          "model-paths": ["src/models"], "quoting": {"identifier": False}})
    write_yaml(root / "src" / "models" / "schema.yml", {"version": 2, "semantic_models": [ITEMS]})
    assert parse_dbt_project(str(root / "src" / "models")).quoting == {"identifier": False}


def test_a_symlinked_model_path_reads_the_project_quoting(tmp_path: Path) -> None:
    root, external = tmp_path / "dbt", tmp_path / "elsewhere" / "models"
    write_yaml(root / "dbt_project.yml", {"name": "shop", "version": "1.0", "config-version": 2,
                                          "model-paths": ["linked"], "quoting": {"identifier": False}})
    write_yaml(external / "schema.yml", {"version": 2, "semantic_models": [ITEMS]})
    (root / "linked").symlink_to(external, target_is_directory=True)
    assert parse_dbt_project(str(root / "linked")).quoting == {"identifier": False}


def test_a_directory_outside_the_model_paths_ignores_the_parent_project(tmp_path: Path) -> None:
    _import(tmp_path, dialect="postgres", semantic_models=[ITEMS], quoting={"identifier": False})
    write_yaml(tmp_path / "dbt" / "other" / "schema.yml", {"version": 2, "semantic_models": [ITEMS]})
    assert parse_dbt_project(str(tmp_path / "dbt" / "other")).quoting == {}


async def test_snowflake_default_folds(tmp_path: Path) -> None:
    items = models_of(_import(tmp_path, dialect="snowflake", semantic_models=[ITEMS]))["items"]
    assert await _generated_relation(items, dialect="snowflake") == [("orderitems", False)]


async def test_snowflake_with_identifier_quoting_on(tmp_path: Path) -> None:
    items = models_of(_import(tmp_path, dialect="snowflake", semantic_models=[ITEMS],
                              quoting={"identifier": True}))["items"]
    assert await _generated_relation(items, dialect="snowflake") == [("OrderItems", True)]


@pytest.mark.parametrize("dialect", ["postgres", "snowflake"])
@pytest.mark.parametrize("quoting", [
    {}, {"database": False}, {"schema": False}, {"identifier": False},
    {"database": True}, {"schema": True}, {"identifier": True}, {"database": True, "identifier": False},
])
def test_flags_apply_part_by_part(tmp_path: Path, dialect: str, quoting: dict[str, bool]) -> None:
    result = _import(tmp_path, dialect=dialect, semantic_models=[SRC], quoting=quoting, sources=[ORDERS_SOURCE],
                     sql={"src": "SELECT * FROM {{ source('raw', 'orders') }}"})
    fold = str.lower if dialect == "postgres" else str.upper
    expected = [name if quoting.get(part, DEFAULT_QUOTING[dialect]) else fold(name)
                for part, name in (("database", "Analytics"), ("schema", "Raw"), ("identifier", "Orders"))]
    assert _relation(models_of(result)["m"], dialect=dialect) == expected


def test_schema_quoting_off_on_postgres(tmp_path: Path) -> None:
    source = {"name": "raw", "schema": "Raw", "tables": [{"name": "orders", "identifier": "Orders"}]}
    result = _import(tmp_path, dialect="postgres", semantic_models=[SRC], quoting={"schema": False}, sources=[source],
                     sql={"src": "SELECT * FROM {{ source('raw', 'orders') }}"})
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw", "Orders"]


def test_per_model_override_is_reported(tmp_path: Path) -> None:
    source = {"name": "raw", "tables": [{"name": "orders", "identifier": "Orders"}]}
    result = _import(tmp_path, dialect="postgres", semantic_models=[SRC], sources=[source], sql={
        "src": "{{ config(quoting={'identifier': false}) }}\nSELECT * FROM {{ source('raw', 'orders') }}"})
    assert [e for e in entries(result, "src") if "quoting" in e.message.lower()]
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw", "Orders"]


def test_per_source_override_is_reported(tmp_path: Path) -> None:
    source = {"name": "raw", "quoting": {"identifier": False}, "tables": [{"name": "orders", "identifier": "Orders"}]}
    result = _import(tmp_path, dialect="postgres", semantic_models=[SRC], sources=[source],
                     sql={"src": "SELECT * FROM {{ source('raw', 'orders') }}"})
    assert [e for e in entries(result, "raw") if "quoting" in e.message.lower()]
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw", "Orders"]


def test_non_boolean_flag_is_reported(tmp_path: Path) -> None:
    result = _import(tmp_path, dialect="postgres", semantic_models=[ITEMS], quoting={"identifier": "{{ var('q') }}"})
    assert [e for e in entries(result, "identifier") if "quoting" in e.message.lower()]
    assert _relation(models_of(result)["items"], dialect="postgres") == ["OrderItems"]


# ── source() names the declared relation ──────────────────────────────────

def _sourced(tmp_path: Path, source_call: str, sources: list[dict[str, Any]],
             schema_files: dict[str, dict[str, Any]] | None = None) -> ConversionResult:
    return _import(tmp_path, dialect="postgres", semantic_models=[SRC], sources=sources, schema_files=schema_files,
                   sql={"src": f"SELECT * FROM {{{{ {source_call} }}}}"})


def test_declared_schema_and_identifier(tmp_path: Path) -> None:
    result = _sourced(tmp_path, "source('raw', 'orders')",
                      [{"name": "raw", "schema": "raw_v2", "tables": [{"name": "orders", "identifier": "orders_tbl"}]}])
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw_v2", "orders_tbl"]


def test_declared_defaults(tmp_path: Path) -> None:
    result = _sourced(tmp_path, "source('raw', 'orders')", [{"name": "raw", "tables": [{"name": "orders"}]}])
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw", "orders"]


def test_undeclared_source_is_reported(tmp_path: Path) -> None:
    result = _sourced(tmp_path, "source('legacy', 'events')", [{"name": "raw", "tables": [{"name": "orders"}]}])
    assert entries(result, "legacy.events")
    assert _relation(models_of(result)["m"], dialect="postgres") == ["legacy", "events"]


def test_duplicate_source_declaration_is_reported_and_the_first_wins(tmp_path: Path) -> None:
    second = {"version": 2, "sources": [{"name": "raw", "schema": "raw_late", "tables": [{"name": "orders"}]}]}
    result = _sourced(tmp_path, "source('raw', 'orders')",
                      [{"name": "raw", "schema": "raw_first", "tables": [{"name": "orders"}]}],
                      schema_files={"zz_more.yml": second})
    assert entries(result, "raw")
    assert _relation(models_of(result)["m"], dialect="postgres") == ["raw_first", "orders"]


def test_package_qualified_ref_follows_the_policy(tmp_path: Path) -> None:
    result = _import(tmp_path, dialect="snowflake", semantic_models=[SRC],
                     sql={"src": "SELECT * FROM {{ ref('shop', 'Other') }}"})
    assert _relation(models_of(result)["m"], dialect="snowflake") == ["OTHER"]


# ── logical references stay; physical entity expressions fold ─────────────

ORDERS = {"name": "orders", "model": "ref('orders')",
          "entities": [{"name": "order_id", "type": "primary", "expr": "OrderId"}],
          "dimensions": [{"name": "Status", "type": "categorical", "expr": "status"}],
          "measures": [{"name": "revenue", "agg": "sum", "expr": "amount"}]}


def test_entity_expr_reads_the_folded_column(tmp_path: Path) -> None:
    orders = models_of(_import(tmp_path, dialect="postgres", semantic_models=[ORDERS]))["orders"]
    keys = [c for c in orders.columns if c.primary_key]
    assert [c.physical_name for c in keys] == ["orderid"]


def test_metric_filter_with_a_mixed_case_logical_dimension_is_unchanged(tmp_path: Path) -> None:
    metric = {"name": "paid_revenue", "type": "simple", "type_params": {"measure": {
        "name": "revenue", "filter": "{{ Dimension('order_id__Status') }} = 'paid'"}}}
    orders = models_of(_import(tmp_path, dialect="postgres", semantic_models=[ORDERS], metrics=[metric]))["orders"]
    measure = orders.get_measure("paid_revenue")
    assert measure is not None
    assert measure.formula == "sum(revenue_filtered)"
    assert column(orders, "revenue_filtered").filter == "Status = 'paid'"


def test_member_without_an_expr_reads_its_folded_name(tmp_path: Path) -> None:
    sm = {"name": "orders", "model": "ref('orders')",
          "dimensions": [{"name": "OrderDate", "type": "time", "type_params": {"time_granularity": "day"}}]}
    orders = models_of(_import(tmp_path, dialect="postgres", semantic_models=[sm]))["orders"]
    assert column(orders, "OrderDate").physical_name == "orderdate"


def test_an_engine_alone_gives_the_dialect(tmp_path: Path) -> None:
    project = DbtProject.model_validate({"semantic_models": [ITEMS | {"model": "OrderItems"}]})
    with disposable_engine(f"duckdb:///{tmp_path / 'shop.duckdb'}") as engine:
        result = DbtToSlayerConverter(project=project, data_source=DS, sa_engine=engine).convert()
    assert models_of(result)["items"].sql_table == "OrderItems"
