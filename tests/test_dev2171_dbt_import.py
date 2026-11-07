"""dbt import preserves dbt's meaning (specs: importers/meaning-preservation, importers/dbt-import)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.dbt.parser import parse_dbt_project
from slayer.sql import engine_factory
from tests._dev2171_fixtures import (
    DS, MASKED, column, counts_by, deletes, drift_entries, entries, has_column, has_measure, import_dbt, models_of,
    run_cli, save_datasource, seed, source_column, stored_model, unreachable_engine, value, write_yaml,
)

ORDER = {"name": "order", "type": "primary", "expr": "id"}
CUSTOMER_FK = {"name": "customer", "type": "foreign", "expr": "customer_id"}
CUSTOMERS = {"name": "customers", "model": "customers",
             "entities": [{"name": "customer", "type": "primary", "expr": "id"}],
             "dimensions": [{"name": "name", "type": "categorical"}]}
RENAMED_STATUS = {"name": "status", "type": "categorical", "expr": "state"}


def dim(name: str, expr: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"name": name, "type": "categorical", **({"expr": expr} if expr is not None else {}), **extra}


def meas(name: str, agg: str, expr: str | None = None) -> dict[str, Any]:
    return {"name": name, "agg": agg, **({"expr": expr} if expr is not None else {})}


def sm(*, dimensions: list[dict[str, Any]] | None = None, measures: list[dict[str, Any]] | None = None,
       entities: list[dict[str, Any]] | None = None, name: str = "orders") -> dict[str, Any]:
    return {"name": name, "model": "orders", "entities": entities or [ORDER],
            "dimensions": dimensions or [], "measures": measures or []}


def typed(*columns: tuple[str, str]) -> list[dict[str, Any]]:
    """The ``orders`` regular model declaring ``(column, data_type)`` pairs."""
    return [{"name": "orders", "columns": [{"name": c, "data_type": t} for c, t in columns]}]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


class TestPhysicalReferences:
    @MASKED
    async def test_collision_in_a_measure_expr(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[RENAMED_STATUS], measures=[
            meas("paid_cnt", "sum", "CASE WHEN status = 'paid' THEN 1 ELSE 0 END")])], db=db)
        assert await value(models=result.models, db=db, model="orders", formula="paid_cnt") == 2

    @MASKED
    async def test_collision_in_sum_boolean(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[RENAMED_STATUS], measures=[
            meas("paid_cnt", "sum_boolean", "status = 'paid'")])], db=db)
        assert await value(models=result.models, db=db, model="orders", formula="paid_cnt") == 2

    @MASKED
    async def test_collision_in_a_dimension_expr(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[RENAMED_STATUS, dim("status_upper", "UPPER(status)")])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status_upper") == {
            "PAID": 2, "OPEN": 1}

    @MASKED
    async def test_entity_key_masked_by_a_dimension(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[dim("customer_id", "legacy_ref")], entities=[ORDER, CUSTOMER_FK]),
                             CUSTOMERS], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="customers.name") == {
            "Ann": 2, "Bo": 1}

    @MASKED
    async def test_self_named_dimension(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[dim("status", "UPPER(status)")])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == {
            "PAID": 2, "OPEN": 1}

    def test_case_only_ambiguity_is_dropped(self) -> None:
        result = import_dbt([sm(dimensions=[dim("s_lower", "status"), dim("s_upper", '"Status"'),
                                            dim("s_len", "LENGTH(Status)"), dim("s_qlen", 'LENGTH("Status")')])],
                            target_dialect="duckdb")
        assert not has_column(result, "orders", "s_len")
        assert all(has_column(result, "orders", c) for c in ("s_lower", "s_upper", "s_qlen"))
        assert [e for e in entries(result, "Status") if "ambiguous" in e.message.lower()], result.warnings


class TestDeclaredDataType:
    @pytest.mark.parametrize(("data_type", "element"), [
        ("array<string>", DataType.TEXT), ("text[]", DataType.TEXT), ("ARRAY<INT64>", DataType.INT),
        ("array<array<string>>", DataType.UNKNOWN), ("array<geography>", DataType.UNKNOWN),
    ])
    def test_list_data_types(self, data_type: str, element: DataType) -> None:
        result = import_dbt([sm(dimensions=[dim("tags")])], regular_models=typed(("tags", data_type)))
        col = column(models_of(result)["orders"], "tags")
        assert (col.type, col.element_type) == (DataType.ARRAY, element)

    @pytest.mark.parametrize(("data_type", "expected"), [
        ("bigint", DataType.INT), ("numeric(10,2)", DataType.DOUBLE), ("varchar(255)", DataType.TEXT),
        ("boolean", DataType.BOOLEAN), ("date", DataType.DATE), ("timestamp with time zone", DataType.TIMESTAMP),
    ])
    def test_scalar_data_types(self, data_type: str, expected: DataType) -> None:
        result = import_dbt([sm(dimensions=[dim("note")])], regular_models=typed(("note", data_type)))
        assert column(models_of(result)["orders"], "note").type == expected

    def test_declared_type_of_a_time_dimension(self) -> None:
        created = {"name": "created_at", "type": "time", "type_params": {"time_granularity": "day"}}
        result = import_dbt([sm(dimensions=[created])], regular_models=typed(("created_at", "date")))
        assert column(models_of(result)["orders"], "created_at").type == DataType.DATE

    def test_declared_types_reach_keys_and_measure_sources(self) -> None:
        result = import_dbt([sm(measures=[meas("total", "sum", "amount"), meas("tag_cnt", "count", "tags")])],
                            regular_models=typed(("id", "bigint"), ("amount", "numeric(10,2)"), ("tags", "text[]")))
        model = models_of(result)["orders"]
        assert column(model, "id").type == DataType.INT
        assert source_column(model, "total").type == DataType.DOUBLE
        tags = source_column(model, "tag_cnt")
        assert (tags.type, tags.element_type) == (DataType.ARRAY, DataType.TEXT)

    def test_unrecognised_data_type(self) -> None:
        result = import_dbt([sm(measures=[meas("geo_sum", "sum", "geo")])], regular_models=typed(("geo", "geography")))
        assert source_column(models_of(result)["orders"], "geo_sum").type == DataType.DOUBLE
        assert entries(result, "geography", severity="info"), result.warnings

    @pytest.mark.parametrize(("data_type", "expected", "element"), [
        ("INT64", DataType.INT, None), ("STRING", DataType.TEXT, None),
        ("ARRAY<STRING>", DataType.ARRAY, DataType.TEXT),
    ])
    def test_read_in_the_datasource_dialect(self, data_type: str, expected: DataType,
                                            element: DataType | None) -> None:
        result = import_dbt([sm(dimensions=[dim("note")])], regular_models=typed(("note", data_type)),
                            target_dialect="bigquery")
        col = column(models_of(result)["orders"], "note")
        assert (col.type, col.element_type) == (expected, element)

    def test_read_from_schema_yml_without_hidden_models(self, tmp_path: Path) -> None:
        write_yaml(tmp_path / "models" / "schema.yml", {
            "models": [{"name": "orders", "columns": [{"name": "tags", "data_type": "array<string>"}]}],
            "semantic_models": [{**sm(dimensions=[dim("tags")]), "model": "ref('orders')"}],
        })
        project = parse_dbt_project(str(tmp_path))
        result = DbtToSlayerConverter(project=project, data_source=DS).convert()
        col = column(models_of(result)["orders"], "tags")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)

    async def test_declared_type_contradicted_by_the_database(self, db: Path) -> None:
        result = import_dbt([sm(dimensions=[dim("amount")])], regular_models=typed(("amount", "text")), db=db)
        assert column(models_of(result)["orders"], "amount").type == DataType.INT
        assert entries(result, "amount", severity="info"), result.warnings


class TestTypingOrder:
    def test_offline_implied_types(self) -> None:
        result = import_dbt([sm(measures=[meas("total", "sum", "amount"), meas("n_notes", "count", "note"),
                                          meas("paid", "sum_boolean", "status = 'paid'")])])
        model = models_of(result)["orders"]
        assert source_column(model, "total").type == DataType.DOUBLE
        assert source_column(model, "n_notes").type == DataType.TEXT
        assert source_column(model, "paid").type == DataType.INT

    @pytest.mark.parametrize("target", ["mysql", "mssql"])
    def test_median_refused_by_the_target_is_dropped(self, target: str) -> None:
        result = import_dbt([sm(measures=[meas("mid", "median", "amount")])], target_dialect=target)
        assert not has_measure(result, "orders", "mid")
        assert [e for e in entries(result, "mid") if "not supported" in e.message], result.warnings

    def test_unreachable_datasource_is_tried_once(self, tmp_path: Path) -> None:
        project = DbtProject.model_validate({"semantic_models": [sm(), sm(name="orders_b")]})
        with unreachable_engine(tmp_path) as (engine, attempts):
            result = DbtToSlayerConverter(project=project, data_source=DS, sa_engine=engine,
                                          target_dialect="duckdb").convert()
        assert len(attempts) == 1
        (note,) = [w.message for w in result.warnings if w.category == "typing"]
        assert "could not be reached" in note

    def test_offline_max_over_an_untyped_source_is_dropped(self) -> None:
        result = import_dbt([sm(measures=[meas("latest", "max", "created_at")])])
        assert not has_measure(result, "orders", "latest")
        found = entries(result, "latest")
        assert [e for e in found if "datasource" in e.message.lower() and "type" in e.message.lower()], found

    def test_filtered_metric_over_a_dropped_measure_is_dropped(self) -> None:
        metric = {"name": "late_paid", "type": "simple", "type_params": {"measure": "latest"},
                  "filter": "{{ Dimension('order__status') }} = 'paid'"}
        result = import_dbt([sm(dimensions=[dim("status")], measures=[meas("latest", "max", "created_at")])],
                            metrics=[metric])
        assert not has_measure(result, "orders", "late_paid")
        assert [e for e in entries(result, "late_paid") if "was dropped" in e.message], result.unconverted_metrics

    def test_filtered_metric_column_keeps_the_declared_type(self) -> None:
        metric = {"name": "late_paid", "type": "simple", "type_params": {"measure": "latest"},
                  "filter": "{{ Dimension('order__status') }} = 'paid'"}
        result = import_dbt([sm(dimensions=[dim("status")], measures=[meas("latest", "max", "created_at")])],
                            regular_models=typed(("created_at", "timestamp")), metrics=[metric])
        assert source_column(models_of(result)["orders"], "late_paid").type is DataType.TIMESTAMP

    def test_model_sql_with_a_trailing_semicolon_is_typed_live(self, db: Path) -> None:
        orders_q = {**sm(measures=[meas("tag_cnt", "count", "tags")]), "model": "orders_q"}
        result = import_dbt([orders_q], regular_models=[{"name": "orders_q", "raw_code": "SELECT * FROM orders;"}],
                            db=db)
        col = source_column(models_of(result)["orders"], "tag_cnt")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)

    async def test_list_measure_source_typed_from_the_database(self, db: Path) -> None:
        result = import_dbt([sm(measures=[meas("tag_cnt", "count", "tags")])], db=db)
        col = source_column(models_of(result)["orders"], "tag_cnt")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)
        assert await value(models=result.models, db=db, model="orders", formula="tag_cnt") == 2

    @pytest.mark.parametrize("agg", ["count_distinct", "min", "max", "sum", "average"])
    def test_aggregation_refused_over_a_list_is_dropped(self, db: Path, agg: str) -> None:
        result = import_dbt([sm(measures=[meas("tag_agg", agg, "tags")])], db=db)
        assert not has_measure(result, "orders", "tag_agg")
        found = entries(result, "tag_agg")
        assert [e for e in found if "list" in e.message.lower() or "ARRAY" in e.message], found

    async def test_validate_models_proposes_no_deletion(self, db: Path, tmp_path: Path) -> None:
        result = import_dbt([sm(dimensions=[dim("tags")], measures=[meas("tag_cnt", "count", "tags")])], db=db)
        found = await drift_entries(models=result.models, db=db, base_dir=tmp_path / "store")
        assert deletes(found, model="orders") == [], found


def test_cli_types_without_hidden_models(db: Path, tmp_path: Path) -> None:
    project = tmp_path / "dbt_project"
    write_yaml(project / "models" / "schema.yml", {
        "semantic_models": [{**sm(measures=[meas("tag_cnt", "count", "tags")]), "model": "ref('orders')"}]})
    store = tmp_path / "store"
    save_datasource(store, database=db)
    try:
        code = run_cli("import-dbt", str(project), "--datasource", DS, "--storage", str(store))
    finally:
        engine_factory.invalidate_engine(DatasourceConfig(name=DS, type="duckdb", database=str(db)))
    assert code == 0
    col = source_column(stored_model(store, "orders"), "tag_cnt")
    assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)
