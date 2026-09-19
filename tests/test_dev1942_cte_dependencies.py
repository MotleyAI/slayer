"""DEV-1942 task 1.5 — CTE dependencies are declared (sql.arc42.md P6).

The forward-reference validator (``assert_dependency_ordered_ctes``) flags any CTE
that reads a sibling declared later in the same ``WITH`` — the DuckDB-rejected shape
SQLite tolerates — while leaving physical tables, schema-qualified names, derived-table
aliases and nested-``WITH`` scopes alone. The observable pins prove the emitted
statement lists a nested producer before every consumer at every hoisting depth: a
producer attached at two phases (row + combined) and two distinct nested hoists both
order their carriers first, on every Tier-1 dialect, and a multi-stage query carrying
the shape executes on strict engines.

Red today: ``assert_dependency_ordered_ctes`` / ``CteOrderError`` do not yet exist
(ImportError), and the dual-phase shape still fails closed in the checker.

Spec: openspec …/specs/queries/partitioned-aggregates — "Nested producers compose to
arbitrary depth" (the two-phase emission-order scenario).
"""

from __future__ import annotations

import re

import pytest

from slayer.sql.scope_check import (
    CteOrderError,
    assert_dependency_ordered_ctes,
    assert_scope_closed,
)

from tests._dev1832_fixtures import (
    ModelMeasure,
    gen,
    make_exec_engine,
    month_td,
    monthly_q,
)

DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_X = "amount:sum(partition_by=[region, ordered_at])"
_Y = "amount:max(partition_by=[region, ordered_at])"
REAGG_STANDALONE = f"min({_X}, partition_by=region)"
HANDWRITTEN_MIN = f"sum(amount * min({_X}, partition_by=region))"
REAGG_MAX_STANDALONE = f"max({_Y}, partition_by=region)"
HANDWRITTEN_MAX = f"sum(amount * max({_Y}, partition_by=region))"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _cm_ctes_in_order(sql: str) -> list[str]:
    """The ``_cm_*`` producer CTE names in declaration order."""
    return re.findall(r"(_cm_\w+) AS \(", sql)


# --------------------------------------------------------------------------- #
# The forward-reference validator (pure-SQL unit battery).
# --------------------------------------------------------------------------- #
class TestForwardReferenceValidator:
    def test_forward_reference_raises(self) -> None:
        sql = "WITH a AS (SELECT x FROM b), b AS (SELECT 1 AS x) SELECT x FROM a"
        with pytest.raises(CteOrderError):
            assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_ordered_chain_passes(self) -> None:
        sql = "WITH b AS (SELECT 1 AS x), a AS (SELECT x FROM b) SELECT x FROM a"
        assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_physical_table_not_flagged(self) -> None:
        sql = "WITH a AS (SELECT y FROM warehouse) SELECT y FROM a"
        assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_schema_qualified_same_basename_not_flagged(self) -> None:
        # `a` reads sch.b (schema-qualified → physical), a CTE `b` is declared later;
        # the schema qualifier means it is not the forward CTE.
        sql = "WITH a AS (SELECT v FROM sch.b), b AS (SELECT 1 AS v) SELECT v FROM a"
        assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_derived_table_alias_shadowing_not_flagged(self) -> None:
        # `b` reads a derived table aliased `c`; a CTE `c` follows, but the local
        # alias binds the name, so it is not a forward reference.
        sql = ("WITH b AS (SELECT n FROM (SELECT 1 AS n) AS c), "
               "c AS (SELECT 2 AS n) SELECT n FROM b")
        assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_nested_with_scope_resolved(self) -> None:
        sql = ("WITH outer1 AS (WITH inner1 AS (SELECT 1 AS x) SELECT x FROM inner1) "
               "SELECT x FROM outer1")
        assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_case_folded_forward_reference_raises(self) -> None:
        sql = 'WITH a AS (SELECT x FROM "B"), "B" AS (SELECT 1 AS x) SELECT x FROM a'
        with pytest.raises(CteOrderError):
            assert_dependency_ordered_ctes(sql, dialect="postgres")

    def test_case_folded_backward_reference_passes(self) -> None:
        sql = 'WITH "B" AS (SELECT 1 AS x), a AS (SELECT x FROM b) SELECT x FROM a'
        assert_dependency_ordered_ctes(sql, dialect="postgres")


# --------------------------------------------------------------------------- #
# The emitted dual-phase statement declares its carrier before every consumer.
# --------------------------------------------------------------------------- #
class TestDualPhaseEmissionOrder:
    def _dual_q(self):
        return monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b")],
            time_dimensions=month_td())

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_carrier_precedes_reaggregation_on_every_dialect(self, dialect) -> None:
        sql = await gen(self._dual_q(), dialect=dialect)
        ctes = _cm_ctes_in_order(sql)
        assert len(ctes) == 2, ctes
        carrier, reagg = next(c for c in ctes if "min" not in c), next(
            c for c in ctes if "min" in c)
        assert ctes.index(carrier) < ctes.index(reagg)  # nested producer first
        assert_dependency_ordered_ctes(sql, dialect=dialect)
        assert_scope_closed(sql, dialect=dialect)

    async def test_two_distinct_nested_hoists_each_order_their_carrier(self) -> None:
        # Two re-aggregations (min over amount:sum, max over amount:max) each dual-phase:
        # four `_cm_` producers; EACH carrier is declared before its OWN consumer (two
        # distinct re-keyed edges, not one global position).
        sql = await gen(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b"),
                      ModelMeasure(formula=REAGG_MAX_STANDALONE, name="c"),
                      ModelMeasure(formula=HANDWRITTEN_MAX, name="d")],
            time_dimensions=month_td()), dialect="duckdb")
        ctes = _cm_ctes_in_order(sql)
        assert len(ctes) == 4, ctes

        def pos(prefix: str) -> int:
            return next(i for i, c in enumerate(ctes) if c.startswith(prefix))

        assert pos("_cm_amount_sum") < pos("_cm_min_amount_sum")  # min carrier first
        assert pos("_cm_amount_max") < pos("_cm_max_amount_max")  # max carrier first
        assert_dependency_ordered_ctes(sql, dialect="duckdb")
        assert_scope_closed(sql, dialect="duckdb")

    async def test_multi_stage_query_carrying_the_shape_executes(self, exec_backend) -> None:
        # An unrelated cumsum(amount:sum) forces the multi-stage steps prelude; the
        # non-root stage carries the dual-phase re-aggregation.
        _, engine = exec_backend
        query = monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b"),
                      ModelMeasure(formula="cumsum(amount:sum)", name="c")],
            time_dimensions=month_td())
        resp = await engine.execute(query)
        assert resp.data
        dry = await engine.execute(query, dry_run=True)
        assert_dependency_ordered_ctes(dry.sql, dialect="duckdb")
        assert_scope_closed(dry.sql, dialect="duckdb")
