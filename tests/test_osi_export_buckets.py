"""Ossie export: time buckets, Sunday weeks, date addition and custom granularities inside measures
(spec: importers/osi-export)."""

from datetime import date, datetime
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp

from slayer.core.models import DatasourceConfig, DatasetMetric, SlayerDataset
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import EXPORT_DDL, PG_DDL, customers, orders, slayer_metric
from tests._osi_export_helpers import (
    Env, carries, dataset, export_datasets, expression_sql, make_env, mentions, metric, reimport,
    rows, run_sql, same_sql,
)
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

_DIALECTS = ["duckdb", "sqlite"]
_EXTENSIONS = pytest.mark.parametrize("extensions", [True, False])
_DAY_OR_COARSER = ["day", "week", "month", "quarter", "year"]
_SUB_DAY = ["second", "minute", "hour"]
# An April order puts the rows in two fiscal years.
_FISCAL_DDL = [*EXPORT_DDL, "INSERT INTO orders VALUES (6, 103, 3.00, 3, 1, 3, 0, 'open', 'US', TRUE, '2026-04-02', "
                            "'2026-04-02 07:00:00', NULL, 2026, NULL, 'llc')"]
_FISCAL_YEAR = {"name": "fiscal_year", "base": "month", "multiple": 12, "origin": "2024-04-01"}
_FISCAL_FORMULA = "count_distinct(date_trunc('fiscal_year', ordered_on))"


def _sql(entity: dict[str, Any] | None) -> str:
    assert entity is not None
    return entity["expression"]["sql"]


def _tree(text: str) -> exp.Expr:
    return sqlglot.parse_one(text, read="snowflake")


def _no_interval(text: str) -> None:
    assert _tree(text).find(exp.Interval) is None, text
    assert "INTERVAL" not in text.upper(), text


async def _export_orders(env: Env, *metrics: DatasetMetric, extensions: bool = True) -> dict[str, Any]:
    return (await export_datasets(env, customers(), orders(metrics=list(metrics)), extensions=extensions)).document


async def _value(env: Env, dataset_name: str, measure: str, *, grouped: bool) -> list[tuple[Any, ...]]:
    payload: dict[str, Any] = {"population": dataset_name, "metrics": [{"metric": measure, "alias": "v"}]}
    if grouped:
        payload["dimensions"] = [f"{dataset_name}.region"]
    data = await rows(env, payload)
    return sorted((str(r.get(f"{dataset_name}.region")), _norm(r["v"])) for r in data)


def _norm(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:19].replace("T", " ")
    if isinstance(value, str):
        return value[:19].replace("T", " ")
    return float(value) if value is not None else None


async def _assert_round_trip(env: Env, document: dict[str, Any], *names: str) -> None:
    """Each measure of ``orders`` re-imported from ``document`` returns its original value, grouped and ungrouped."""
    imported, _ = await reimport(env, document)
    for name in names:
        for grouped in (False, True):
            assert await _value(imported, "orders", name, grouped=grouped) == \
                await _value(env, "orders", name, grouped=grouped), (name, grouped)


# ───────────────────────────── built-in buckets ─────────────────────────────

@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_month_bucket_of_a_timestamp(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("months", "count_distinct(date_trunc('month', ordered_at))"),
                                    extensions=extensions)
    text = _sql(metric(dataset(document, "orders"), "months"))
    assert same_sql(text, "COUNT(DISTINCT DATE_TRUNC('MONTH', CAST(ordered_at AS TIMESTAMP)))"), text
    assert "DATE_TRUNC('MONTH'" in text
    await _assert_round_trip(env, document, "months")


async def test_day_or_coarser_bucket_of_a_date(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    document = await _export_orders(env, slayer_metric("last_month", "max(date_trunc('month', ordered_on))"))
    text = _sql(metric(dataset(document, "orders"), "last_month"))
    assert same_sql(text, "MAX(CAST(DATE_TRUNC('MONTH', CAST(ordered_on AS TIMESTAMP)) AS DATE))"), text


@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
@pytest.mark.parametrize("unit", [*_SUB_DAY, *_DAY_OR_COARSER])
@pytest.mark.parametrize("column", ["ordered_at", "ordered_on"])
async def test_every_built_in_unit(tmp_path: Path, unit: str, column: str, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("m", f"max(date_trunc('{unit}', {column}))"),
                                    extensions=extensions)
    text = _sql(metric(dataset(document, "orders"), "m"))
    bucket = f"DATE_TRUNC('{unit.upper()}', CAST({column} AS TIMESTAMP))"
    if column == "ordered_on" and unit in _DAY_OR_COARSER:
        bucket = f"CAST({bucket} AS DATE)"
    assert same_sql(text, f"MAX({bucket})"), text
    _no_interval(text)
    await _assert_round_trip(env, document, "m")


@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_bucket_under_filter_and_inside_a_scalar_call(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(
        env,
        slayer_metric("january", "count(*) FILTER (WHERE date_trunc('month', ordered_at) = '2026-01-01')"),
        slayer_metric("last_day", "max(coalesce(date_trunc('day', ordered_at), ordered_at))"),
        extensions=extensions,
    )
    orders_ds = dataset(document, "orders")
    for name, unit in (("january", "MONTH"), ("last_day", "DAY")):
        text = _sql(metric(orders_ds, name))
        truncs = list(_tree(text).find_all(exp.TimestampTrunc, exp.DateTrunc))
        assert [t.text("unit").upper() for t in truncs] == [unit], text
        _no_interval(text)
    await _assert_round_trip(env, document, "january", "last_day")


_PER_CUSTOMER = "count_distinct(date_trunc('month', ordered_at)) / count_distinct(customers.id)"


@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_bucket_in_a_model_scoped_metric(tmp_path: Path, dialect: str) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("months_per_customer", _PER_CUSTOMER))
    text = _sql(metric(document, "months_per_customer"))
    assert same_sql(text, "COUNT(DISTINCT DATE_TRUNC('MONTH', CAST(orders.ordered_at AS TIMESTAMP))) "
                          "/ COUNT(DISTINCT customers.id)"), text
    await _assert_round_trip(env, document, "months_per_customer")


@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_bucket_in_a_model_scoped_metric_without_extensions(tmp_path: Path, dialect: str) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("months_per_customer", _PER_CUSTOMER), extensions=False)
    imported, _ = await reimport(env, document)
    # Re-imported, it is the document's model-scoped metric, answering by bare name.
    assert await _value(imported, "orders", "months_per_customer", grouped=False) == \
        await _value(env, "orders", "months_per_customer", grouped=False)


# ─────────────────────────────── logical types ──────────────────────────────

_TYPED = [("day", date), ("month", date), ("hour", datetime)]
_LATEST = date(2026, 3, 2)


async def _typed_metric_sql(env: Env, *, target: str) -> dict[str, str]:
    document = await _export_orders(env, *(slayer_metric(f"b_{unit}", f"max(date_trunc('{unit}', ordered_on))")
                                           for unit, _ in _TYPED))
    orders_ds = dataset(document, "orders")
    return {unit: expression_sql(metric(orders_ds, f"b_{unit}") or {}, target=target) for unit, _ in _TYPED}


def _assert_typed(unit: str, kind: type, value: Any) -> None:
    if kind is date:
        assert type(value) is date, (unit, value)
        assert value == (_LATEST if unit == "day" else _LATEST.replace(day=1)), (unit, value)
    else:
        assert isinstance(value, datetime) and value.tzinfo is None, (unit, value)
        assert value == datetime(2026, 3, 2), (unit, value)


async def test_exported_buckets_keep_their_type_on_duckdb(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    sql_by_unit = await _typed_metric_sql(env, target="duckdb")
    for unit, kind in _TYPED:
        [(value,)] = run_sql(env, f"SELECT {sql_by_unit[unit]} FROM orders")
        _assert_typed(unit, kind, value)


async def test_exported_buckets_keep_their_type_on_postgres_in_a_non_utc_session(
    tmp_path: Path, postgresql_proc: Any,
) -> None:
    env = await make_env(tmp_path)
    sql_by_unit = await _typed_metric_sql(env, target="postgres")
    with pg_database(postgresql_proc, statements=PG_DDL, name="osi_bucket_types") as pg:
        with psycopg.connect(host=pg.host, port=pg.port, user=pg.username, dbname=pg.database) as conn:
            conn.execute("SET TimeZone = 'Asia/Kolkata'")
            for unit, kind in _TYPED:
                row = conn.execute(cast(LiteralString, f"SELECT {sql_by_unit[unit]} FROM orders")).fetchone()
                assert row is not None
                _assert_typed(unit, kind, row[0])


# ─────────────────────────── Sunday weeks, date_add ─────────────────────────

@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_sunday_week(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("weeks", "count_distinct(date_trunc('week_sunday', ordered_at))"),
                                    extensions=extensions)
    text = _sql(metric(dataset(document, "orders"), "weeks"))
    tree = _tree(text)
    assert [t.text("unit").upper() for t in tree.find_all(exp.TimestampTrunc, exp.DateTrunc)] == ["WEEK"], text
    adds = list(tree.find_all(exp.DateAdd))
    assert len(adds) == 2 and all(a.text("unit").upper() == "DAY" for a in adds), text
    _no_interval(text)
    await _assert_round_trip(env, document, "weeks")


@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_date_addition(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("next_day", "max(date_add(ordered_at, 1, 'day'))"),
                                    extensions=extensions)
    text = _sql(metric(dataset(document, "orders"), "next_day"))
    [add] = list(_tree(text).find_all(exp.DateAdd))
    assert add.text("unit").upper() == "DAY" and add.expression.name == "1", text
    assert "DATEADD(DAY, 1," in text.upper()
    _no_interval(text)
    await _assert_round_trip(env, document, "next_day")


@_EXTENSIONS
@pytest.mark.parametrize("dialect", _DIALECTS)
async def test_extraction_keeps_exporting_as_date_part(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    document = await _export_orders(env, slayer_metric("month_numbers", "count_distinct(month(ordered_at))"),
                                    extensions=extensions)
    text = _sql(metric(dataset(document, "orders"), "month_numbers"))
    assert same_sql(text, "COUNT(DISTINCT CAST(DATE_PART(MONTH, ordered_at) AS INT))"), text
    await _assert_round_trip(env, document, "month_numbers")


# ──────────────────────────── custom granularities ──────────────────────────

async def _fiscal_env(tmp_path: Path, dialect: str) -> Env:
    env = await make_env(tmp_path, dialect=dialect, statements=_FISCAL_DDL)
    config = DatasourceConfig.model_validate({**env.config.model_dump(), "granularities": [_FISCAL_YEAR]})
    await YAMLStorage(base_dir=str(env.store)).save_datasource(config)
    return env


def _fiscal_orders() -> SlayerDataset:
    return orders(metrics=[slayer_metric("fiscal_years", _FISCAL_FORMULA)])


@pytest.mark.parametrize("extensions", [True, False])
async def test_custom_granularity_on_duckdb(tmp_path: Path, extensions: bool) -> None:
    env = await _fiscal_env(tmp_path, "duckdb")
    document = (await export_datasets(env, customers(), _fiscal_orders(), extensions=extensions)).document
    entity = metric(dataset(document, "orders"), "fiscal_years")
    assert entity is not None
    assert "sql" not in entity["expression"]
    [entry] = entity["expression"]["dialects"]
    assert entry["dialect"] == "DUCKDB"
    assert await _value(env, "orders", "fiscal_years", grouped=False) == [("None", 2.0)]
    await _assert_round_trip(env, document, "fiscal_years")


async def test_custom_granularity_on_sqlite(tmp_path: Path) -> None:
    env = await _fiscal_env(tmp_path, "sqlite")
    result = await export_datasets(env, customers(), _fiscal_orders())
    assert metric(dataset(result.document, "orders"), "fiscal_years") is None
    assert carries(dataset(result.document, "orders"), _FISCAL_FORMULA)
    [entry] = mentions(result, "fiscal_years", category="extension_only")
    assert "does not bind" not in entry.message, entry.message
