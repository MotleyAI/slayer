"""Bucket emission returns the bucket's logical type; a DATE meeting a TIMESTAMP is promoted to midnight."""

from __future__ import annotations

import re
from typing import Any, Optional

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression
from sqlglot.expressions.datatypes import DType

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.granularity import CustomGranularity, Granularity
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import get_dialect
from slayer.sql.render.joins import build_grain_joinback_condition

from tests._dev2018_fixtures import DS_GRANULARITIES, GRANULARITIES, bucket_type, mx_model, px_model
from tests._engine_helpers import _engine_generate
from tests._time_points_fixtures import PinnedClock

TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery", "trino"]
TIER2 = ["redshift"]
PROMOTING = [d for d in TIER1 if d != "sqlite"] + TIER2
UNITS = ["second", "hour", "day", "week", "week_sunday", "month", "quarter", "year", "fiscal_year", "quarter_hour"]
OPERANDS = [DataType.DATE, DataType.TIMESTAMP]
ZONED = {exp.DataType.Type.TIMESTAMPTZ, exp.DataType.Type.TIMESTAMPLTZ}
_CUSTOM = {g["name"]: CustomGranularity.model_validate(g) for g in GRANULARITIES}
SQLITE_CANONICAL = "STRFTIME('%Y-%m-%d %H:%M:%f',"


def _unit(name: str) -> Granularity:
    return _CUSTOM[name] if name in _CUSTOM else TimeGranularity(name)


def _bucket(dialect: str, unit: str, operand: DataType, column: str = "t") -> Expression:
    return get_dialect(dialect).build_bucket(col_expr=exp.column(column), granularity=_unit(unit), operand=operand)


def _render(dialect: str, node: Expression) -> str:
    return node.sql(dialect=get_dialect(dialect).sqlglot_name)


def _flat(sql: str) -> str:
    return " ".join(sql.split())


def _promoted(dialect: str, column: Expression) -> str:
    """``column`` as the dialect's midnight-TIMESTAMP reading of a DATE, rendered."""
    return _render(dialect, get_dialect(dialect).promote_to_timestamp(column))


def _promote_pattern(dialect: str) -> re.Pattern[str]:
    """Any operand wrapped in the dialect's DATE→TIMESTAMP promotion."""
    template = _promoted(dialect, exp.column("zz_operand"))
    return re.compile(re.escape(template).replace("zz_operand", ".+?"))


# ---------------------------------------------------------------------------
# build_bucket(operand=…): every dialect, unit and source type.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", TIER1 + TIER2)
@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_bucket_is_valid_and_never_zoned(dialect, unit, operand) -> None:
    node = _bucket(dialect, unit, operand)
    sqlglot.parse_one(_render(dialect, node), read=get_dialect(dialect).sqlglot_name)
    assert not [dt for dt in node.find_all(exp.DataType) if dt.this in ZONED], _render(dialect, node)


def _is_cast_to(node: Expression, kind: DType) -> bool:
    return isinstance(node, exp.Cast) and node.to.this is kind


@pytest.mark.parametrize("dialect", ["postgres", "duckdb", *TIER2])
@pytest.mark.parametrize("unit", [u for u in UNITS if u not in _CUSTOM])
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_base_family_truncates_a_timestamp_and_casts_dates_back(dialect, unit, operand) -> None:
    node = _bucket(dialect, unit, operand)
    truncs = list(node.find_all(exp.DateTrunc, exp.TimestampTrunc))
    assert truncs, _render(dialect, node)
    if unit != "week_sunday":  # truncates the shifted value, not the source
        assert all(_is_cast_to(t.this, exp.DataType.Type.TIMESTAMP) for t in truncs), _render(dialect, node)
    assert _is_cast_to(node, exp.DataType.Type.DATE) is (bucket_type(operand, unit) is DataType.DATE), (
        _render(dialect, node))


@pytest.mark.parametrize("dialect", TIER1 + TIER2)
@pytest.mark.parametrize("unit", list(_CUSTOM))
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_custom_bucket_demotes_exactly_when_the_rule_says_date(dialect, unit, operand, monkeypatch) -> None:
    cls = type(get_dialect(dialect))
    original = cls.demote_to_date
    calls: list[Expression] = []

    def spy(self: Any, expr: Expression) -> Expression:
        calls.append(expr)
        return original(self, expr)

    monkeypatch.setattr(cls, "demote_to_date", spy)
    _bucket(dialect, unit, operand)
    assert len(calls) == (1 if bucket_type(operand, unit) is DataType.DATE else 0)


@pytest.mark.parametrize("unit", ["day", "week", "week_sunday", "month", "quarter", "year"])
def test_mysql_day_or_coarser_bucket_of_a_datetime_is_a_datetime(unit) -> None:
    node = _bucket("mysql", unit, DataType.TIMESTAMP)
    assert isinstance(node, exp.Cast), _render("mysql", node)
    assert node.to.sql(dialect="mysql").upper().startswith("DATETIME"), _render("mysql", node)


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_clickhouse_wraps_the_bucket_to_its_type(unit, operand) -> None:
    sql = _render("clickhouse", _bucket("clickhouse", unit, operand))
    wrapper = r"toDate\(" if bucket_type(operand, unit) is DataType.DATE else r"toDateTime64\("
    assert re.match(wrapper, sql), sql


def _bq_datetime_cast(node: Optional[Expr]) -> bool:
    return isinstance(node, exp.Cast) and node.to.sql(dialect="bigquery").upper() == "DATETIME"


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_bigquery_bucket_has_its_logical_type(unit, operand) -> None:
    node = _bucket("bigquery", unit, operand)
    datetime_casts = [c for c in node.find_all(exp.Cast) if _bq_datetime_cast(c)]
    if bucket_type(operand, unit) is DataType.DATE:
        assert _is_cast_to(node, exp.DataType.Type.DATE) or not datetime_casts, _render("bigquery", node)
    else:
        assert not _is_cast_to(node, exp.DataType.Type.DATE), _render("bigquery", node)
        if operand is DataType.DATE:
            assert datetime_casts, _render("bigquery", node)


@pytest.mark.parametrize("dialect", ["snowflake", "tsql", "trino", "bigquery"])
@pytest.mark.parametrize("unit", ["second", "minute", "hour"])
def test_sub_day_bucket_of_a_date_promotes_the_source(dialect, unit) -> None:
    sql = _render(dialect, _bucket(dialect, unit, DataType.DATE))
    assert _promoted(dialect, exp.column("t")) in sql, sql


# ---------------------------------------------------------------------------
# DATE meeting TIMESTAMP in generated SQL: comparisons, null-safe equality, conditionals, nullif.
# ---------------------------------------------------------------------------

async def _sql(dialect: str, query: dict[str, Any], *, model: Optional[SlayerModel] = None,
               extra: Optional[list[SlayerModel]] = None) -> str:
    return await _engine_generate(
        query=SlayerQuery.model_validate(query), model=model or mx_model(), extra_models=extra, dialect=dialect,
        validate=False, clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
    )


MIXED_FILTERS = [
    "d = ts",
    "d is not distinct from ts",
    "date_trunc('day', ts) = d",
    "coalesce(shipped_at, d) = ts",
    "iif(shipped_at is null, d, shipped_at) = ts",
    "greatest(d, ts) = ts",
]


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("filt", MIXED_FILTERS)
async def test_mixed_filter_promotes_the_date(dialect, filt) -> None:
    sql = _flat(await _sql(dialect, {"source_model": "mx", "dimensions": ["id"], "filters": [filt]}))
    assert _promoted(dialect, exp.column("d", table="mx")) in sql, sql


@pytest.mark.parametrize("dialect", PROMOTING)
async def test_mixed_nullif_compares_at_midnight_and_returns_the_date(dialect) -> None:
    sql = _flat(await _sql(dialect, {
        "source_model": "mx", "dimensions": ["id", {"expression": "nullif(d, ts)", "name": "v"}],
    }))
    assert _promoted(dialect, exp.column("d", table="mx")) in sql, sql
    assert "NULLIF(" not in sql.upper(), sql


@pytest.mark.parametrize("filt", ["d = ts", "ts = shipped_at", "d is not distinct from ts", "date_trunc('day', ts) = ts"])
async def test_sqlite_temporal_pair_compares_in_canonical_form(filt) -> None:
    sql = _flat(await _sql("sqlite", {"source_model": "mx", "dimensions": ["id"], "filters": [filt]}))
    where = sql.split(" WHERE ", 1)[1]
    assert where.count(SQLITE_CANONICAL) >= 2, sql


# ---------------------------------------------------------------------------
# Join-backs: typed pairs, DATE side promoted, unknown temporal pair refused.
# ---------------------------------------------------------------------------

def _joinback(dialect: str, *pairs: tuple[DataType | None, DataType | None]) -> str:
    built = build_grain_joinback_condition(
        pairs=[(exp.column(f"l{i}"), lt, exp.column(f"r{i}"), rt) for i, (lt, rt) in enumerate(pairs)],
        dialect=get_dialect(dialect),
    )
    assert built is not None
    return _render(dialect, built)


@pytest.mark.parametrize("dialect", PROMOTING)
def test_joinback_promotes_the_date_side(dialect) -> None:
    sql = _joinback(dialect, (DataType.DATE, DataType.TIMESTAMP), (DataType.TIMESTAMP, DataType.DATE))
    assert _promoted(dialect, exp.column("l0")) in sql, sql
    assert _promoted(dialect, exp.column("r1")) in sql, sql
    assert _promoted(dialect, exp.column("r0")) not in sql, sql
    assert _promoted(dialect, exp.column("l1")) not in sql, sql


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("pair", [(DataType.DATE, DataType.DATE), (DataType.TIMESTAMP, DataType.TIMESTAMP),
                                  (DataType.INT, DataType.INT), (None, None)])
def test_joinback_same_types_are_untouched(dialect, pair) -> None:
    assert not _promote_pattern(dialect).search(_joinback(dialect, pair))


@pytest.mark.parametrize("pair", [(DataType.DATE, DataType.TIMESTAMP), (DataType.TIMESTAMP, DataType.TIMESTAMP)])
def test_sqlite_joinback_compares_in_canonical_form(pair) -> None:
    assert _joinback("sqlite", pair).count(SQLITE_CANONICAL) == 2


@pytest.mark.parametrize("dialect", TIER1 + TIER2)
@pytest.mark.parametrize("pair", [(DataType.DATE, None), (None, DataType.TIMESTAMP)])
def test_joinback_temporal_pair_of_unknown_type_fails_closed(dialect, pair) -> None:
    with pytest.raises(SlayerError):
        _joinback(dialect, pair)


# ---------------------------------------------------------------------------
# The time spine over a DATE axis on every dialect.
# ---------------------------------------------------------------------------

def _spine(granularity: str, date_range: list[str]) -> dict[str, Any]:
    return {
        "measures": [{"formula": "count(px.id)", "name": "n"}],
        "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": granularity,
                             "date_range": date_range}],
    }


def _axis_comparisons(tree: Expr) -> list[Expression]:
    """WHERE comparisons whose one side is the bare DATE axis ``px.d``."""
    out = []
    for where in tree.find_all(exp.Where):
        for cmp in where.find_all(exp.GTE, exp.GT, exp.LT, exp.LTE):
            sides = (cmp.this, cmp.expression)
            if any(isinstance(s, exp.Column) and s.name == "d" for s in sides):
                out.append(cmp)
    return out


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("granularity", ["month", "hour"])
async def test_spine_over_a_date_axis(dialect, granularity) -> None:
    sql = await _sql(dialect, _spine(granularity, ["2024-05-15 06:00:00", "2024-08-01"]), model=px_model())
    read = get_dialect(dialect).sqlglot_name
    tree = sqlglot.parse_one(sql, read=read)
    ons = [_flat(j.args["on"].sql(dialect=read)) for j in tree.find_all(exp.Join) if j.args.get("on") is not None]
    assert ons, sql
    if dialect == "sqlite":
        assert all(on.count(SQLITE_CANONICAL) >= 2 for on in ons), ons
    else:
        assert all(_promote_pattern(dialect).search(on) for on in ons), ons
    bounds = _axis_comparisons(tree)
    assert bounds, sql
    for cmp in bounds:
        other = cmp.expression if isinstance(cmp.this, exp.Column) else cmp.this
        assert ":" not in other.sql(dialect=read), cmp.sql(dialect=read)
    assert "2024-05-16" in sql, sql


# ---------------------------------------------------------------------------
# BigQuery zoned TIMESTAMP: every bucket reads the column once.
# ---------------------------------------------------------------------------

def _zoned(model: SlayerModel) -> SlayerModel:
    return model.model_copy(update={"columns": [
        c.model_copy(update={"db_type": "TIMESTAMP"}) if c.name == "ts" else c for c in model.columns]})


@pytest.mark.parametrize("query", [
    {"source_model": "mx", "measures": [{"formula": "count(*)", "name": "n"}],
     "time_dimensions": [{"dimension": "ts", "granularity": "month"}]},
    {"source_model": "mx", "measures": [{"formula": "count(*)", "name": "n"}],
     "time_dimensions": [{"dimension": "ts", "granularity": "quarter_hour"}]},
    {"source_model": "mx", "dimensions": [{"expression": "date_trunc('hour', ts)", "name": "h"}]},
    {"measures": [{"formula": "count(mx.id)", "name": "n"}],
     "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "day",
                          "date_range": ["2025-03-01", "2025-03-03"]}]},
], ids=["built_in", "custom", "row_expression", "spine"])
async def test_bigquery_zoned_bucket_reads_the_column_once(query) -> None:
    sql = await _sql("bigquery", query, model=_zoned(mx_model()))
    tree = sqlglot.parse_one(sql, read="bigquery")
    reads = [c for c in tree.find_all(exp.Column)
             if c.name == "ts" and c.table == "mx" and c.find_ancestor(exp.Where) is None]
    assert reads, sql
    for column in reads:
        read = column.parent
        assert _bq_datetime_cast(read), sql
        assert read is not None and not _bq_datetime_cast(read.parent), sql


async def test_bigquery_zoned_column_meets_a_date_as_midnight() -> None:
    sql = _flat(await _sql("bigquery", {"source_model": "mx", "dimensions": ["id"], "filters": ["d = ts"]},
                           model=_zoned(mx_model())))
    where = sql.split(" WHERE ", 1)[1].upper()
    assert "MX.D = CAST(MX.TS AS DATETIME)" not in where, sql
    assert "CAST(MX.D AS " in where, sql
