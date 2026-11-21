"""Bucket emission returns the bucket's logical type; a DATE meeting a TIMESTAMP is promoted to midnight."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression
from sqlglot.expressions.datatypes import DType

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.granularity import CustomGranularity, Granularity
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import get_dialect
from slayer.sql.render.joins import build_grain_joinback_condition

from tests._dev2018_fixtures import DS_GRANULARITIES, GRANULARITIES, bucket_type, mx_dataset, px_dataset
from tests._engine_helpers import _engine_generate
from tests._time_points_fixtures import PinnedClock

TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery", "trino"]
TIER2 = ["redshift"]
PROMOTING = [d for d in TIER1 if d != "sqlite"] + TIER2
UNITS = ["second", "hour", "day", "week", "week_sunday", "month", "quarter", "year", "fiscal_year", "quarter_hour"]
OPERANDS = [DataType.DATE, DataType.DATETIME]
ZONED = {exp.DataType.Type.TIMESTAMPTZ, exp.DataType.Type.TIMESTAMPLTZ}
_CUSTOM = {g["name"]: CustomGranularity.model_validate(g) for g in GRANULARITIES}
SQLITE_CANONICAL = "SLAYER_TIMESTAMP_TEXT("


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
    node = _bucket("mysql", unit, DataType.DATETIME)
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


@pytest.mark.parametrize("unit", ["day", "hour"])
def test_tsql_computed_timestamp_bucket_truncates_a_datetime2(unit) -> None:
    computed = exp.Add(this=exp.column("a"), expression=exp.column("b"))
    node = get_dialect("tsql").build_bucket(col_expr=computed, granularity=TimeGranularity(unit), operand=DataType.DATETIME)
    sql = _render("tsql", node)
    assert "AS DATETIME2)" in sql, sql


@pytest.mark.parametrize("unit", ["second", "hour", "day", "month", "fiscal_year"])
@pytest.mark.parametrize("operand", OPERANDS, ids=lambda o: o.value)
def test_snowflake_bucket_timestamps_are_ntz(unit, operand) -> None:
    sql = _render("snowflake", _bucket("snowflake", unit, operand))
    assert "AS TIMESTAMP)" not in sql, sql


def test_snowflake_timestamp_literal_is_ntz() -> None:
    node = get_dialect("snowflake").build_temporal_literal(value=datetime(2025, 3, 1, 6), dt=DataType.DATETIME)
    assert _render("snowflake", node) == "CAST('2025-03-01 06:00:00' AS TIMESTAMPNTZ)"


@pytest.mark.parametrize("dialect", ["snowflake", "tsql", "trino", "bigquery"])
@pytest.mark.parametrize("unit", ["second", "minute", "hour"])
def test_sub_day_bucket_of_a_date_promotes_the_source(dialect, unit) -> None:
    sql = _render(dialect, _bucket(dialect, unit, DataType.DATE))
    assert _promoted(dialect, exp.column("t")) in sql, sql


# ---------------------------------------------------------------------------
# DATE meeting TIMESTAMP in generated SQL: comparisons, null-safe equality, conditionals, nullif.
# ---------------------------------------------------------------------------

async def _sql(dialect: str, query: dict[str, Any], *, model: Optional[SlayerDataset] = None,
               extra: Optional[list[SlayerDataset]] = None) -> str:
    return await _engine_generate(
        query=SlayerQuery.model_validate(query), dataset=model or mx_dataset(), extra_datasets=extra, dialect=dialect,
        validate=False, clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
    )


MIXED_FILTERS = [
    "mx.d = mx.ts",
    "mx.d is not distinct from mx.ts",
    "date_trunc('day', mx.ts) = mx.d",
    "coalesce(mx.shipped_at, mx.d) = mx.ts",
    "iif(mx.shipped_at is null, mx.d, mx.shipped_at) = mx.ts",
    "greatest(mx.d, mx.ts) = mx.ts",
]


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("filt", MIXED_FILTERS)
async def test_mixed_filter_promotes_the_date(dialect, filt) -> None:
    sql = _flat(await _sql(dialect, {"population": "mx", "dimensions": ["mx.id"], "filter": [filt]}))
    assert _promoted(dialect, exp.column("d", table="mx")) in sql, sql


@pytest.mark.parametrize("dialect", PROMOTING)
async def test_mixed_nullif_compares_at_midnight_and_returns_the_date(dialect) -> None:
    sql = _flat(await _sql(dialect, {
        "population": "mx", "dimensions": ["mx.id", {"field": "nullif(mx.d, mx.ts)", "alias": "v"}],
    }))
    assert _promoted(dialect, exp.column("d", table="mx")) in sql, sql
    assert "NULLIF(" not in sql.upper(), sql


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("filt", [
    "mx.d = timestamp '2024-05-15 06:00:00'",
    "mx.d != timestamp '2024-05-15 06:00:00'",
    "mx.d in ('2024-05-16', '2024-05-15 06:00:00')",
])
async def test_date_meeting_an_off_midnight_literal_is_promoted(dialect, filt) -> None:
    sql = _flat(await _sql(dialect, {"population": "mx", "dimensions": ["mx.id"], "filter": [filt]}))
    assert _promoted(dialect, exp.column("d", table="mx")) in sql, sql


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("filt", [
    "mx.d in ('2024-05-16', '2024-05-15 06:00:00')",
    "mx.ts in ('2024-05-15T06:00:00')",
    "mx.ts is not distinct from '2024-05-15 06:00:00'",
])
async def test_temporal_members_are_typed_where_dates_are_not_text(dialect, filt) -> None:
    where = _flat(await _sql(dialect, {"population": "mx", "dimensions": ["mx.id"], "filter": [filt]})).split(" WHERE ")[1]
    assert not re.search(r"(?:IN \(|,|FROM)\s*'2024-05-1[56]", where), where


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("filt", [
    "mx.d = timestamp '2024-05-15 00:00:00'",
    "mx.d >= timestamp '2024-05-15 06:00:00'",
    "mx.d in ('2024-05-16', '2024-05-15 00:00:00')",
])
async def test_date_meeting_an_exact_date_literal_stays_bare(dialect, filt) -> None:
    sql = _flat(await _sql(dialect, {"population": "mx", "dimensions": ["mx.id"], "filter": [filt]}))
    assert _promoted(dialect, exp.column("d", table="mx")) not in sql, sql


@pytest.mark.parametrize("filt", ["mx.d = mx.ts", "mx.ts = mx.shipped_at", "mx.d is not distinct from mx.ts", "date_trunc('day', mx.ts) = mx.ts"])
async def test_sqlite_temporal_pair_compares_in_canonical_form(filt) -> None:
    sql = _flat(await _sql("sqlite", {"population": "mx", "dimensions": ["mx.id"], "filter": [filt]}))
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
    sql = _joinback(dialect, (DataType.DATE, DataType.DATETIME), (DataType.DATETIME, DataType.DATE))
    assert _promoted(dialect, exp.column("l0")) in sql, sql
    assert _promoted(dialect, exp.column("r1")) in sql, sql
    assert _promoted(dialect, exp.column("r0")) not in sql, sql
    assert _promoted(dialect, exp.column("l1")) not in sql, sql


@pytest.mark.parametrize("dialect", PROMOTING)
@pytest.mark.parametrize("pair", [(DataType.DATE, DataType.DATE), (DataType.DATETIME, DataType.DATETIME),
                                  (DataType.INTEGER, DataType.INTEGER), (None, None)])
def test_joinback_same_types_are_untouched(dialect, pair) -> None:
    assert not _promote_pattern(dialect).search(_joinback(dialect, pair))


@pytest.mark.parametrize("pair", [(DataType.DATE, DataType.DATETIME), (DataType.DATETIME, DataType.DATETIME)])
def test_sqlite_joinback_compares_in_canonical_form(pair) -> None:
    assert _joinback("sqlite", pair).count(SQLITE_CANONICAL) == 2


@pytest.mark.parametrize("dialect", TIER1 + TIER2)
@pytest.mark.parametrize("pair", [(DataType.DATE, None), (None, DataType.DATETIME)])
def test_joinback_temporal_pair_of_unknown_type_fails_closed(dialect, pair) -> None:
    with pytest.raises(SlayerError):
        _joinback(dialect, pair)


# ---------------------------------------------------------------------------
# The time spine over a DATE axis on every dialect.
# ---------------------------------------------------------------------------

def _spine(granularity: str, date_range: list[str]) -> dict[str, Any]:
    return {
        "metrics": [{"metric": "count(px.id)", "alias": "n"}],
        "time_dimensions": [{"field": "time_spine.timestamp", "granularity": granularity,
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
    sql = await _sql(dialect, _spine(granularity, ["2024-05-15 06:00:00", "2024-08-01"]), model=px_dataset())
    read = get_dialect(dialect).sqlglot_name
    tree = sqlglot.parse_one(sql, read=read)
    ons = [_flat(j.args["on"].sql(dialect=read)) for j in tree.find_all(exp.Join) if j.args.get("on") is not None]
    assert ons, sql
    if dialect == "sqlite":
        assert all(on.count(SQLITE_CANONICAL) >= 2 for on in ons), ons
    elif granularity == "month":  # a DATE month bucket meets the TIMESTAMP spine
        assert all(_promote_pattern(dialect).search(on) for on in ons), ons
    else:  # an hour bucket of a DATE is already a TIMESTAMP
        assert not any(_promote_pattern(dialect).search(on) for on in ons), ons
    bounds = _axis_comparisons(tree)
    assert bounds, sql
    for cmp in bounds:
        other = cmp.expression if isinstance(cmp.this, exp.Column) else cmp.this
        assert ":" not in other.sql(dialect=read), cmp.sql(dialect=read)
    assert "2024-05-16" in sql, sql


# ---------------------------------------------------------------------------
# BigQuery zoned TIMESTAMP: every bucket reads the column once.
# ---------------------------------------------------------------------------

def _zoned(model: SlayerDataset) -> SlayerDataset:
    return model.model_copy(update={"fields": [
        c.model_copy(update={"db_type": "TIMESTAMP"}) if c.name == "ts" else c for c in model.fields]})


@pytest.mark.parametrize("query", [
    {"population": "mx", "metrics": [{"metric": "count(mx.*)", "alias": "n"}],
     "time_dimensions": [{"field": "mx.ts", "granularity": "month"}]},
    {"population": "mx", "metrics": [{"metric": "count(mx.*)", "alias": "n"}],
     "time_dimensions": [{"field": "mx.ts", "granularity": "quarter_hour"}]},
    {"population": "mx", "dimensions": [{"field": "date_trunc('hour', mx.ts)", "alias": "h"}]},
    {"metrics": [{"metric": "count(mx.id)", "alias": "n"}],
     "time_dimensions": [{"field": "time_spine.timestamp", "granularity": "day",
                          "date_range": ["2025-03-01", "2025-03-03"]}]},
], ids=["built_in", "custom", "row_expression", "spine"])
async def test_bigquery_zoned_bucket_reads_the_column_once(query) -> None:
    sql = await _sql("bigquery", query, model=_zoned(mx_dataset()))
    tree = sqlglot.parse_one(sql, read="bigquery")
    reads = [c for c in tree.find_all(exp.Column)
             if c.name == "ts" and c.table == "mx" and c.find_ancestor(exp.Where) is None]
    assert reads, sql
    for column in reads:
        read = column.parent
        assert _bq_datetime_cast(read), sql
        assert read is not None, sql
        assert not _bq_datetime_cast(read.parent), sql


async def test_bigquery_zoned_column_meets_a_date_as_midnight() -> None:
    sql = _flat(await _sql("bigquery", {"population": "mx", "dimensions": ["mx.id"], "filter": ["mx.d = mx.ts"]},
                           model=_zoned(mx_dataset())))
    where = sql.split(" WHERE ", 1)[1].upper()
    assert "MX.D = CAST(MX.TS AS DATETIME)" not in where, sql
    assert "CAST(MX.D AS " in where, sql
