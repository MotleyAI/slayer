"""BigQuery: SLayer's TIMESTAMP is naive DATETIME; a zoned TIMESTAMP column compares through a cast of the other side."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine import ingestion
from slayer.engine.introspect_utils import _get_columns_fallback
from tests._dev1840_fixtures import dev1840_models, signup_month_td
from tests._dev1919_fixtures import WINDOWED_PARAM
from tests._dev2015_fixtures import JAN_MAR, TWO_FACTS, spine_models, spine_td
from tests._dev2120_fixtures import emit_lib, lib_model, lib_query
from tests._engine_helpers import _engine_generate


def _zoned_lib() -> SlayerModel:
    base = lib_model()
    return base.model_copy(update={"columns": [
        c.model_copy(update={"db_type": "TIMESTAMP"}) if c.name == "ts" else c for c in base.columns]})


async def _sql(expr: str, *, dialect: str = "bigquery", filtered: bool = False, zoned: bool = False) -> str:
    query = lib_query(filters=[expr]) if filtered else lib_query(expr)
    sql = await emit_lib(query, dialect=dialect, models=[_zoned_lib()] if zoned else None)
    return " ".join(sql.upper().split())


class TestNaiveValues:
    async def test_now_is_current_datetime(self) -> None:
        sql = await _sql("now()")
        assert "CURRENT_DATETIME()" in sql, sql
        assert "CURRENT_TIMESTAMP" not in sql, sql

    @pytest.mark.parametrize("expr", ["TRY_CAST(code AS TIMESTAMP)", "json_value(payload, '$.t' RETURNING TIMESTAMP)"])
    async def test_portable_timestamp_casts_read_as_datetime(self, expr: str) -> None:
        sql = await _sql(expr)
        assert "AS DATETIME)" in sql, sql
        assert "AS TIMESTAMP)" not in sql, sql

    async def test_author_cast_is_the_databases_own(self) -> None:
        assert "CAST(LIB.CODE AS TIMESTAMP)" in await _sql("CAST(code AS TIMESTAMP)")

    @pytest.mark.parametrize("unit", ["hour", "day", "week", "month"])
    async def test_date_add_stays_datetime(self, unit: str) -> None:
        sql = await _sql(f"date_add(ts, 1, '{unit}')")
        assert "DATETIME_ADD(CAST(LIB.TS AS DATETIME)" in sql, sql
        assert "TIMESTAMP" not in sql, sql

    async def test_sub_day_date_diff_uses_datetime_functions(self) -> None:
        sql = await _sql("date_diff('hour', ts, now())")
        assert "DATETIME_DIFF(" in sql, sql
        assert "DATETIME_TRUNC(" in sql, sql
        assert "TIMESTAMP_" not in sql, sql

    async def test_naive_column_compares_with_now_directly(self) -> None:
        assert "WHERE LIB.TS < CURRENT_DATETIME()" in await _sql("ts < now()", filtered=True)


class TestZonedColumn:
    @pytest.mark.parametrize(("expr", "where"), [
        ("ts < now()", "LIB.TS < CAST(CURRENT_DATETIME() AS TIMESTAMP)"),
        ("now() > ts", "CAST(CURRENT_DATETIME() AS TIMESTAMP) > LIB.TS"),
        ("ts < date_add(now(), -1, 'day')", "LIB.TS < CAST(DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY) AS TIMESTAMP)"),
    ])
    async def test_other_side_converts_and_the_column_stays_bare(self, expr: str, where: str) -> None:
        assert f"WHERE {where}" in await _sql(expr, filtered=True, zoned=True)

    async def test_temporal_literal_keeps_its_iso_text(self) -> None:
        sql = await _sql("ts > TIMESTAMP '2024-01-01 10:00'", filtered=True, zoned=True)
        assert "WHERE LIB.TS > '2024-01-01 10:00:00'" in sql, sql

    async def test_column_against_itself_is_untouched(self) -> None:
        assert "WHERE LIB.TS = LIB.TS" in await _sql("ts = ts", filtered=True, zoned=True)

    async def test_a_stage_reads_the_zoned_column_naive(self) -> None:
        s1 = SlayerQuery.model_validate({"name": "s1", "source_model": "lib", "dimensions": ["id", "ts"]})
        root = SlayerQuery.model_validate({"source_model": "s1", "dimensions": ["id"], "filters": ["ts < now()"]})
        sql = " ".join((await _engine_generate(query=[s1, root], model=_zoned_lib(), dialect="bigquery")).upper().split())
        assert "CAST(LIB.TS AS DATETIME) AS `LIB___TS`" in sql, sql
        assert "WHERE S1.TS < CURRENT_DATETIME()" in sql, sql

    async def test_other_dialects_ignore_the_db_type(self) -> None:
        zoned = await _sql("ts < now()", dialect="postgres", filtered=True, zoned=True)
        assert zoned == await _sql("ts < now()", dialect="postgres", filtered=True)


class TestZonedColumnReadsNaive:
    @pytest.mark.parametrize(("expr", "fragment"), [
        ("ts", "CAST(LIB.TS AS DATETIME) AS `LIB___V`"),
        ("coalesce(ts, now())", "COALESCE(CAST(LIB.TS AS DATETIME), CURRENT_DATETIME())"),
        ("date_trunc('month', ts)", "DATE_TRUNC(CAST(LIB.TS AS DATETIME), MONTH)"),
    ])
    async def test_value_positions(self, expr: str, fragment: str) -> None:
        assert fragment in await _sql(expr, zoned=True)

    async def test_time_dimension_bucket_and_date_range(self) -> None:
        query = SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": "*:count"}], "time_dimensions": [
            {"dimension": "ts", "granularity": "month", "date_range": ["2024-01-01", "2024-03-31"]}]})
        sql = " ".join((await _engine_generate(query=query, model=_zoned_lib(), dialect="bigquery")).upper().split())
        assert "DATE_TRUNC(CAST(LIB.TS AS DATETIME), MONTH)" in sql, sql
        assert "LIB.TS >= '2024-01-01 00:00:00'" in sql, sql

    @pytest.mark.parametrize(("formula", "fragment"), [
        ("max(ts)", "MAX(CAST(LIB.TS AS DATETIME))"), ("count_distinct(ts)", "COUNT(DISTINCT CAST(LIB.TS AS DATETIME))"),
    ])
    async def test_aggregate_input_reads_naive(self, formula: str, fragment: str) -> None:
        query = SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": formula, "name": "agg_v"}]})
        sql = " ".join((await _engine_generate(query=query, model=_zoned_lib(), dialect="bigquery")).upper().split())
        assert fragment in sql, sql

    async def test_derived_aggregate_source_reads_naive_once(self) -> None:
        base = _zoned_lib()
        model = base.model_copy(update={"columns": [*base.columns, Column(
            name="ts_shifted", sql="TIMESTAMP_ADD(ts, INTERVAL 1 HOUR)", type=DataType.TIMESTAMP, db_type="TIMESTAMP")]})
        query = SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": "max(ts_shifted)",
                                                                                 "name": "agg_v"}]})
        sql = " ".join((await _engine_generate(query=query, model=model, dialect="bigquery")).upper().split())
        assert "MAX(CAST(TIMESTAMP_ADD(LIB.TS, INTERVAL '1' HOUR) AS DATETIME))" in sql, sql
        assert "AS DATETIME) AS DATETIME))" not in sql, sql

    @pytest.mark.parametrize("agg", ["first", "last"])
    async def test_ranked_pick_reads_naive(self, agg: str) -> None:
        query = SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": f"{agg}(ts)", "name": "agg_v"}],
                                            "time_dimensions": [{"dimension": "ts", "granularity": "month"}]})
        sql = " ".join((await _engine_generate(query=query, model=_zoned_lib(), dialect="bigquery")).upper().split())
        assert "CAST(LIB.TS AS DATETIME) AS _VAL_1" in sql, sql

    async def test_derived_time_column_reads_naive(self) -> None:
        base = _zoned_lib()
        model = base.model_copy(update={"columns": [*base.columns, Column(
            name="ts_shifted", sql="TIMESTAMP_ADD(ts, INTERVAL 1 HOUR)", type=DataType.TIMESTAMP, db_type="TIMESTAMP")]})
        query = SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": "*:count"}],
                                            "time_dimensions": [{"dimension": "ts_shifted", "granularity": "month"}]})
        sql = " ".join((await _engine_generate(query=query, model=model, dialect="bigquery")).upper().split())
        assert "DATE_TRUNC(CAST(TIMESTAMP_ADD(LIB.TS, INTERVAL '1' HOUR) AS DATETIME), MONTH)" in sql, sql

    @pytest.mark.parametrize(("target_db_type", "on"), [
        (None, "ON CAST(a.at_raw AS DATETIME) = b.bt"), ("TIMESTAMP", "ON a.at_raw = b.bt"),
    ])
    async def test_join_keys_meet_in_one_type(self, target_db_type: str | None, on: str) -> None:
        # The zoned key's logical name differs from its physical one.
        a = SlayerModel(name="a", sql_table="a", data_source="test", joins=[
            ModelJoin(target_model="b", join_pairs=[["at", "bt"]])], columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="at", sql="at_raw", type=DataType.TIMESTAMP, db_type="TIMESTAMP"),
            Column(name="ts_shifted", sql="TIMESTAMP_ADD(at_raw, INTERVAL 1 HOUR)", type=DataType.TIMESTAMP)])
        b = SlayerModel(name="b", sql_table="b", data_source="test", columns=[
            Column(name="bt", type=DataType.TIMESTAMP, db_type=target_db_type, primary_key=True),
            Column(name="label", type=DataType.TEXT)])
        query = SlayerQuery.model_validate({"source_model": "a", "dimensions": ["b.label"]})
        sql = await _engine_generate(query=query, model=a, extra_models=[b], dialect="bigquery")
        assert on in " ".join(sql.split()), sql

    async def test_window_time_reads_naive(self) -> None:
        models = _zoned(dev1840_models(), {("customers", "signup_at")})
        query = SlayerQuery.model_validate({"source_model": "orders", "time_dimensions": signup_month_td(),
                                            "measures": [{"formula": WINDOWED_PARAM, "name": "w"}]})
        sql = await _engine_generate(query=query, model=models[0], extra_models=models[1:], dialect="bigquery",
                                     validate=False)
        assert "CAST(customers.signup_at AS DATETIME) AS _w_time" in " ".join(sql.split()), sql

    async def test_spine_fact_bucket_reads_naive(self) -> None:
        models = spine_models()
        models = _zoned(models, {(m.name, c.name) for m in models for c in m.columns
                                 if m.name in ("orders", "returns") and c.type in (DataType.DATE, DataType.TIMESTAMP)})
        query = SlayerQuery.model_validate({"source_model": "customers", "measures": TWO_FACTS,
                                            "dimensions": ["customers.region"],
                                            "time_dimensions": [spine_td(date_range=JAN_MAR)]})
        sql = " ".join((await _engine_generate(query=query, model=models[0], extra_models=models[1:],
                                               dialect="bigquery", validate=False)).split())
        assert "DATE_TRUNC(CAST(orders.order_date AS DATETIME), MONTH)" in sql, sql
        assert "orders.order_date >= '2025-01-01 00:00:00'" in sql, sql


def _zoned(models: list[SlayerModel], columns: set[tuple[str, str]]) -> list[SlayerModel]:
    return [m.model_copy(update={"columns": [
        c.model_copy(update={"db_type": "TIMESTAMP", "type": DataType.TIMESTAMP}) if (m.name, c.name) in columns else c
        for c in m.columns]}) for m in models]


class TestIngestionRecordsZonedType:
    @pytest.mark.parametrize(("sa_type", "zoned", "db_type"), [
        (sa.TIMESTAMP(), frozenset({"TIMESTAMP"}), "TIMESTAMP"),
        (sa.DATETIME(), frozenset({"TIMESTAMP"}), None),
        (sa.TIMESTAMP(), frozenset(), None),
    ])
    def test_inspector_column(self, sa_type: sa.types.TypeEngine, zoned: frozenset[str], db_type: str | None) -> None:
        col = ingestion._introspected_column({"type": sa_type}, name="ts", primary_key=False, zoned_types=zoned)
        assert col.type is DataType.TIMESTAMP
        assert col.db_type == db_type

    @pytest.mark.parametrize(("dialect", "db_type"), [("bigquery", "TIMESTAMP"), ("postgresql", None)])
    def test_inspector_path_reads_the_engine_dialect(self, dialect: str, db_type: str | None) -> None:
        engine = MagicMock()
        engine.dialect.name = dialect
        with patch.object(ingestion, "_safe_get_pk_constraint", return_value={}), \
                patch.object(ingestion, "_safe_get_columns", return_value=[{"name": "ts", "type": sa.TIMESTAMP()}]):
            cols = ingestion._introspect_query_columns_via_inspector(engine, MagicMock(), "t", None)
        assert [c.db_type for c in cols] == [db_type]

    @pytest.mark.parametrize(("dialect", "expected"), [
        ("bigquery", {"ts": "TIMESTAMP", "dt": None}), ("postgresql", {"ts": None, "dt": None}),
    ])
    def test_information_schema_fallback(self, dialect: str, expected: dict[str, str | None]) -> None:
        engine = MagicMock()
        engine.dialect.name = dialect
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = [("ts", "TIMESTAMP"), ("dt", "DATETIME")]
        result = _get_columns_fallback(sa_engine=engine, table_name="t", ref=None)
        assert {c["name"]: c["db_type"] for c in result} == expected
