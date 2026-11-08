"""DEV-1839 executed ground truth (SQLite + DuckDB) for mixed-grain transforms
used as computed dimensions — the ``queries/computed-dimensions`` delta
scenarios. Expectations hand-computed in ``tests/_dev1839_fixtures.py``.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  Different grains in one transform union and broadcast .. TestMixedGrainRank
  Keyless grain in a mixed transform ..................... TestKeylessMixed
  A subset grain computes at its own grain ............... TestSubsetGrain
  Nested transform evaluates at its own grain ............ TestNestedTransform
  Explicit transform partition over union rows ........... TestExplicitUnionPartition
  Union attach is cardinality-neutral (D8 structure) ..... TestUnionAttachStructure
  Same mixed-grain transform as dimension and measure .... TestDualRole
  Duplicate producers across roles (accepted trade-off) .. TestDuplicateProducerRoles
(Guard scenarios → tests/test_dev1839_guards.py.)
"""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1839_fixtures import (
    CITY_TOTAL,
    DUAL_MEASURE_RANK_OF,
    EXPLICIT_PART_RANK,
    EXPLICIT_PART_RANK_OF,
    KEYLESS_RANK,
    KEYLESS_RANK_OF,
    MIXED_RANK,
    MIXED_RANK_OF,
    ModelMeasure,
    NESTED_RANK,
    NESTED_RANK_OF,
    RCC_TOTAL,
    RCM_TOTAL,
    REGION_TOTAL,
    SUBSET_RANK,
    SUBSET_RANK_OF,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


async def _dry_scope_closed(engine, query, dialect: str) -> str:
    dry = await engine.execute(query, dry_run=True)
    assert dry.sql is not None
    assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
    assert_scope_closed(dry.sql, dialect=dialect)
    return dry.sql


def _last_part(column: exp.Column) -> str:
    return column.name.split(".")[-1]


def _cte_group_by_sets(sql: str, dialect: str) -> list:
    """The GROUP BY column-name set of every CTE-level SELECT."""
    tree = sqlglot.parse_one(sql, read=dialect)
    out = []
    for with_node in tree.find_all(exp.With):
        for cte in with_node.expressions:
            for select in cte.this.find_all(exp.Select):
                group = select.args.get("group")
                if group is not None:
                    out.append(frozenset(
                        _last_part(c)
                        for g in group.expressions
                        for c in g.find_all(exp.Column)
                    ))
    return out


def _row_attach_joins(sql: str, dialect: str) -> list:
    """``(alias, on-column-name set)`` of every top-level ``_cm_*`` join."""
    tree = sqlglot.parse_one(sql, read=dialect)
    out = []
    for join in tree.args.get("joins") or []:
        name = join.this.alias_or_name
        if re.fullmatch(r"_cm_\w+", name):
            out.append((name, {
                _last_part(c) for c in join.args["on"].find_all(exp.Column)
            }))
    return out


class TestMixedGrainRank:
    async def test_union_broadcast_executes_with_oracle_values(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city", {"expression": MIXED_RANK, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_RANK_OF)
        assert len(resp.data) == len(MIXED_RANK_OF)
        for key, r in by.items():
            assert int(r["orders.rr"]) == MIXED_RANK_OF[key], f"{key}"
            assert float(r["orders.s"]) == pytest.approx(CITY_TOTAL[key]), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_banding_by_mixed_rank_is_legal(self, exec_backend) -> None:
        _, engine = exec_backend
        tier = f"CASE WHEN {MIXED_RANK} <= 1 THEN 'top' ELSE 'rest' END"
        resp = await engine.execute(q(
            dimensions=[{"expression": tier, "name": "tier"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {r["orders.tier"]: float(r["orders.s"]) for r in resp.data}
        # rank<=1 groups: (N,CityA)=30 + (N,NULL)=30; the rest total 150.
        assert got == {"top": pytest.approx(60.0), "rest": pytest.approx(150.0)}


class TestKeylessMixed:
    async def test_share_of_total_ranks_regions(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", {"expression": KEYLESS_RANK, "name": "kr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region")
        assert len(resp.data) == len(KEYLESS_RANK_OF)
        got = {key[0]: int(r["orders.kr"]) for key, r in by.items()}
        # A keyless divisor misgrained to region grain would tie every rank at 1.
        assert got == KEYLESS_RANK_OF
        for key, r in by.items():
            assert float(r["orders.s"]) == pytest.approx(REGION_TOTAL[key[0]])
        await _dry_scope_closed(engine, query, dialect)


class TestSubsetGrain:
    async def test_union_grain_inline_and_coarser_broadcast(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city", {"expression": SUBSET_RANK, "name": "sr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(SUBSET_RANK_OF)
        assert len(resp.data) == len(SUBSET_RANK_OF)
        for key, r in by.items():
            assert int(r["orders.sr"]) == SUBSET_RANK_OF[key], f"{key}"
            assert float(r["orders.s"]) == pytest.approx(CITY_TOTAL[key]), f"{key}"
        await _dry_scope_closed(engine, query, dialect)


class TestNestedTransform:
    async def test_inner_cumsum_evaluates_at_its_own_grain(
        self, exec_backend,
    ) -> None:
        # The inner cumsum accumulates months WITHIN region at its own
        # (region, month) grain; accumulating over union rows instead would
        # shift (S,CityC,Mar) and (N,·,Feb) values and reorder the ranks.
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city", {"expression": NESTED_RANK, "name": "nr"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], r["orders.city"], month_key(r["orders.ordered_at"])):
                (int(r["orders.nr"]), float(r["orders.s"]))
            for r in resp.data
        }
        assert set(got) == set(NESTED_RANK_OF)
        assert len(resp.data) == len(NESTED_RANK_OF)
        for key, (nr, s) in got.items():
            assert nr == NESTED_RANK_OF[key], f"{key}"
            assert s == pytest.approx(RCM_TOTAL[key]), f"{key}"
        await _dry_scope_closed(engine, query, dialect)


class TestExplicitUnionPartition:
    async def test_partition_by_union_key_partitions_union_rows(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=[
                "region", "city", {"expression": EXPLICIT_PART_RANK, "name": "er"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(EXPLICIT_PART_RANK_OF)
        assert len(resp.data) == len(EXPLICIT_PART_RANK_OF)
        for key, r in by.items():
            assert int(r["orders.er"]) == EXPLICIT_PART_RANK_OF[key], f"{key}"
        await _dry_scope_closed(engine, query, dialect)


class TestUnionAttachStructure:
    """D8 — cardinality neutrality plus plan structure: one union-grain row
    attach whose producer computes each strict-subset aggregate at its own
    grain, so a degenerate all-at-union implementation cannot pass."""

    async def test_mixed_dim_is_cardinality_neutral(self, exec_backend) -> None:
        _, engine = exec_backend
        base = await engine.execute(q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        plus = await engine.execute(q(
            dimensions=["region", "city", {"expression": MIXED_RANK, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        base_by = rows_by(base, "orders.region", "orders.city")
        plus_by = rows_by(plus, "orders.region", "orders.city")
        assert set(base_by) == set(plus_by)
        assert len(plus.data) == len(base.data)
        for key, r in plus_by.items():
            assert float(r["orders.s"]) == pytest.approx(
                float(base_by[key]["orders.s"])
            ), f"{key}"

    async def test_one_union_attach_with_own_grain_producers(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        sql = await _dry_scope_closed(engine, q(
            dimensions=["region", "city", {"expression": MIXED_RANK, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ), dialect)
        # ``_cm_`` is the pinned attach-CTE naming contract (``cm_cte_bodies``
        # in the DEV-1739+ suites relies on it), not renderer-private detail.
        attaches = _row_attach_joins(sql, dialect)
        assert len(attaches) == 1, sql
        _, on_cols = attaches[0]
        assert {"region", "city"} <= on_cols, sql  # complete union grain
        # A degenerate all-at-union plan computes both aggregates at
        # (region, city) and has neither own-grain producer below.
        group_sets = _cte_group_by_sets(sql, dialect)
        assert frozenset({"region"}) in group_sets, sql   # region producer
        assert frozenset({"city"}) in group_sets, sql     # city producer


class TestDuplicateProducerRoles:
    async def test_same_coarse_aggregate_in_dimension_and_measure_roles(
        self, exec_backend,
    ) -> None:
        # The region aggregate feeds BOTH the mixed dim's nested attach and a
        # combined measure attach. Duplicate producers are accepted (DEV-1824
        # design D10; stage-2 dedup) — values and cardinality must hold.
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city", {"expression": MIXED_RANK, "name": "rr"}],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
            ],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_RANK_OF)
        assert len(resp.data) == len(MIXED_RANK_OF)
        for key, r in by.items():
            assert int(r["orders.rr"]) == MIXED_RANK_OF[key], f"{key}"
            assert float(r["orders.s"]) == pytest.approx(CITY_TOTAL[key]), f"{key}"
            assert float(r["orders.rt"]) == pytest.approx(
                REGION_TOTAL[key[0]]
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)


class TestDualRole:
    async def test_same_expression_as_dimension_and_measure(
        self, exec_backend,
    ) -> None:
        # The channel dimension makes the query grain strictly finer than the
        # union, so the two roles produce visibly different rank vectors.
        dialect, engine = exec_backend
        query = q(
            dimensions=[
                "region", "city", "channel",
                {"expression": MIXED_RANK, "name": "rr"},
            ],
            measures=[
                ModelMeasure(formula=MIXED_RANK, name="rm"),
                ModelMeasure(formula="amount:sum", name="s"),
            ],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city", "orders.channel")
        assert set(by) == set(RCC_TOTAL)
        assert len(resp.data) == len(RCC_TOTAL)
        for key, r in by.items():
            region, city, _channel = key
            assert int(r["orders.rr"]) == MIXED_RANK_OF[(region, city)], f"{key}"
            assert int(r["orders.rm"]) == DUAL_MEASURE_RANK_OF[key], f"{key}"
            assert float(r["orders.s"]) == pytest.approx(RCC_TOTAL[key]), f"{key}"
        await _dry_scope_closed(engine, query, dialect)
