"""Date-function emission per Tier-1 dialect: exact golden SQL for every part/unit/operand type, plus hook invariants."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.sql.dialects import get_dialect
from slayer.sql.render.value_expr import render_scalar_call
from tests._dev1737_fixtures import PARTS, dt_model, matrix_cases
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent.parent / "golden" / "dev1737_date_functions.json"
TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]
SUB_DAY = [TimeGranularity.SECOND, TimeGranularity.MINUTE, TimeGranularity.HOUR]
DAY_OR_COARSER = [g for g in TimeGranularity if g not in SUB_DAY]
OPERANDS = [DataType.DATE, DataType.TIMESTAMP]


def _cases() -> dict:
    cases = {c.case_id: c.expr for c in matrix_cases()}
    cases.update({
        "clock|current_date": "current_date()",
        "clock|now": "now()",
        "literal|date": "date_diff('day', '2024-01-01', t1)",
        "literal|timestamp": "date_add('2024-01-01 10:00:00', 1, 'day')",
        "literal|in_coalesce": "date_part('year', coalesce(t2, '2024-01-01'))",
    })
    return cases


async def _generate_one(expr: str, dialect: str):
    query = {"source_model": "dt", "dimensions": ["id", {"expression": expr, "name": "v"}]}
    try:
        return await _engine_generate(query=[query], model=dt_model(), dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=TIER1,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_every_case_generates_sql(baseline) -> None:
    raised = {k: v for k, v in baseline.items() if isinstance(v, dict)}
    assert not raised, raised


# ---------------------------------------------------------------------------
# Hook-level invariants (typed operands only, sql P1).
# ---------------------------------------------------------------------------

def _col(name: str = "ts") -> exp.Column:
    return exp.column(name, table="t")


def _sql(node: Expression, dialect: str) -> str:
    return node.sql(dialect=get_dialect(dialect).sqlglot_name)


def _assert_well_formed(node: Expression, dialect: str, *inputs: Expression) -> str:
    assert isinstance(node, Expression)
    seen: set[int] = set()
    for n in node.walk():
        assert id(n) not in seen, f"node shared within the tree: {n!r}"
        seen.add(id(n))
        for child in n.iter_expressions():
            assert child.parent is n, f"stale parent pointer under {n.key}"
    for i in inputs:
        assert i.parent is None, "hook adopted its input operand"
    sql = _sql(node, dialect)
    sqlglot.parse_one(sql, read=get_dialect(dialect).sqlglot_name)
    return sql


def test_date_part_enum() -> None:
    assert {p.value for p in DatePart} == set(PARTS)


@pytest.mark.parametrize("dialect", TIER1)
class TestHooks:
    @pytest.mark.parametrize("part", list(DatePart))
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_date_part(self, dialect: str, part: DatePart, operand: DataType) -> None:
        col = _col()
        _assert_well_formed(get_dialect(dialect).build_date_part(part=part, expr=col, operand=operand), dialect, col)

    @pytest.mark.parametrize("unit", list(TimeGranularity))
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_date_diff(self, dialect: str, unit: TimeGranularity, operand: DataType) -> None:
        start, end = _col("s"), _col("e")
        node = get_dialect(dialect).build_date_diff(unit=unit, start=start, end=end, operand=operand)
        _assert_well_formed(node, dialect, start, end)

    @pytest.mark.parametrize("unit", list(TimeGranularity))
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_date_add(self, dialect: str, unit: TimeGranularity, operand: DataType) -> None:
        col, count = _col(), exp.Literal.number(-2)
        node = get_dialect(dialect).build_date_add(expr=col, count=count, unit=unit, operand=operand)
        _assert_well_formed(node, dialect, col, count)

    def test_clock(self, dialect: str) -> None:
        d = get_dialect(dialect)
        _assert_well_formed(d.build_current_date(), dialect)
        _assert_well_formed(d.build_current_timestamp(), dialect)

    @pytest.mark.parametrize("value,dt", [
        (date(2024, 2, 29), DataType.DATE),
        (datetime(2024, 2, 29, 10, 15, 30), DataType.TIMESTAMP),
    ])
    def test_temporal_literal(self, dialect: str, value, dt: DataType) -> None:
        sql = _assert_well_formed(get_dialect(dialect).build_temporal_literal(value=value, dt=dt), dialect)
        assert "2024-02-29" in sql
        assert ("10:15:30" in sql) is (dt is DataType.TIMESTAMP)


class TestDialectBranches:
    def test_tsql_weekday_ignores_datefirst(self) -> None:
        for operand in OPERANDS:
            sql = _sql(get_dialect("tsql").build_date_part(
                part=DatePart.DAY_OF_WEEK, expr=_col(), operand=operand), "tsql").upper()
            assert "DATEFIRST" not in sql
            assert not re.search(r"DATEPART\(\s*(WEEKDAY|DW)\b", sql), sql
            assert "DATENAME" not in sql

    @pytest.mark.parametrize("unit", DAY_OR_COARSER)
    def test_bigquery_date_add_on_date_stays_date(self, unit: TimeGranularity) -> None:
        sql = _sql(get_dialect("bigquery").build_date_add(
            expr=_col(), count=exp.Literal.number(1), unit=unit, operand=DataType.DATE), "bigquery")
        assert re.search(r"\bDATE_(ADD|SUB)\(", sql), sql
        assert "TIMESTAMP" not in sql, sql

    @pytest.mark.parametrize("unit", SUB_DAY)
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_bigquery_sub_day_add_is_timestamp(self, unit: TimeGranularity, operand: DataType) -> None:
        sql = _sql(get_dialect("bigquery").build_date_add(
            expr=_col(), count=exp.Literal.number(1), unit=unit, operand=operand), "bigquery")
        assert "TIMESTAMP" in sql, sql
        assert not re.search(r"\bDATE_(ADD|SUB)\(", sql), sql

    @pytest.mark.parametrize("unit", list(TimeGranularity))
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_bigquery_never_emits_unsupported_timestamp_parts(self, unit: TimeGranularity, operand: DataType) -> None:
        d = get_dialect("bigquery")
        for node in (
            d.build_date_add(expr=_col(), count=exp.Literal.number(1), unit=unit, operand=operand),
            d.build_date_diff(unit=unit, start=_col("s"), end=_col("e"), operand=operand),
        ):
            sql = _sql(node, "bigquery").upper()
            assert not re.search(r"TIMESTAMP_(ADD|SUB|DIFF)\([^()]*\b(WEEK|MONTH|QUARTER|YEAR)\b", sql), sql

    @pytest.mark.parametrize("unit", DAY_OR_COARSER)
    def test_bigquery_date_diff_on_date_uses_date_functions(self, unit: TimeGranularity) -> None:
        sql = _sql(get_dialect("bigquery").build_date_diff(
            unit=unit, start=_col("s"), end=_col("e"), operand=DataType.DATE), "bigquery")
        assert "TIMESTAMP" not in sql, sql

    @pytest.mark.parametrize("unit", SUB_DAY)
    def test_bigquery_sub_day_diff_uses_timestamp_functions(self, unit: TimeGranularity) -> None:
        sql = _sql(get_dialect("bigquery").build_date_diff(
            unit=unit, start=_col("s"), end=_col("e"), operand=DataType.TIMESTAMP), "bigquery")
        assert "TIMESTAMP_" in sql, sql
        assert "DATE_DIFF(" not in sql, sql

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
    @pytest.mark.parametrize("unit", DAY_OR_COARSER)
    def test_date_add_on_date_casts_back_to_date(self, dialect: str, unit: TimeGranularity) -> None:
        node = get_dialect(dialect).build_date_add(
            expr=_col(), count=exp.Literal.number(1), unit=unit, operand=DataType.DATE)
        assert isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.DATE), _sql(node, dialect)

    @pytest.mark.parametrize("unit", SUB_DAY)
    def test_tsql_sub_day_add_promotes_date(self, unit: TimeGranularity) -> None:
        sql = _sql(get_dialect("tsql").build_date_add(
            expr=_col(), count=exp.Literal.number(1), unit=unit, operand=DataType.DATE), "tsql")
        assert "DATETIME2" in sql.upper(), sql

    @pytest.mark.parametrize("unit", list(TimeGranularity))
    @pytest.mark.parametrize("operand", OPERANDS)
    def test_sqlite_date_add_uses_the_udf(self, unit: TimeGranularity, operand: DataType) -> None:
        sql = _sql(get_dialect("sqlite").build_date_add(
            expr=_col(), count=exp.Literal.number(1), unit=unit, operand=operand), "sqlite")
        assert "slayer_date_add(" in sql.lower(), sql

    @pytest.mark.parametrize("part", list(DatePart))
    def test_sqlite_avoids_unsupported_strftime_codes(self, part: DatePart) -> None:
        for operand in OPERANDS:
            sql = _sql(get_dialect("sqlite").build_date_part(part=part, expr=_col(), operand=operand), "sqlite")
            assert not re.search(r"%[VuGg]", sql), sql


def _trunc_function(dialect: str) -> str:
    snippet = _sql(render_scalar_call(name="trunc", args=[exp.column("n")], dialect=get_dialect(dialect)), dialect)
    return snippet.split("(", 1)[0]


@pytest.mark.parametrize("dialect", TIER1)
async def test_computed_count_truncates_toward_zero(dialect: str) -> None:
    computed = await _generate_one("date_add(d1, n, 'day')", dialect)
    literal = await _generate_one("date_add(d1, 3, 'day')", dialect)
    assert isinstance(computed, str) and isinstance(literal, str)
    fn = _trunc_function(dialect)
    assert f"{fn}(" in computed, computed
    assert f"{fn}(" not in literal, literal
