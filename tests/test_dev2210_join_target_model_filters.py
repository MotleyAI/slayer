"""A model's ``filters`` define its dataset: every read of the model sees only its passing rows, restricted before any join."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2120_fixtures import contractors_engine, contractors_model
from tests._dev2210_fixtures import BACKENDS, by_id, filtered_engine, ids, render, seeded_file
from tests._law_harness import raw_rows

postgresql_proc = factories.postgresql_proc(port=None)

JAN_MAR = ["2024-01-01", "2024-03-31"]


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async with filtered_engine(request.param, request) as eng:
        yield eng


async def _run(engine: SlayerQueryEngine, **body: Any) -> SlayerResponse:
    return await engine.execute(SlayerQuery.model_validate(body))


async def _ids(engine: SlayerQueryEngine, *, source: str, filters: list[str] | None = None) -> set[int]:
    return ids(await _run(engine, source_model=source, dimensions=["id"], filters=filters or []), source)


def _month(value: Any) -> str:
    return str(value)[:7]


def _per_month(resp: SlayerResponse, *names: str, source: str = "done_orders") -> dict[str, tuple]:
    got = {_month(r[f"{source}.ordered_at"]): tuple(r[f"{source}.{n}"] for n in names) for r in resp.data}
    assert len(got) == len(resp.data), resp.data
    return got


class TestJoinTargetReadsFilteredRows:
    async def test_pushed_down_filter(self, engine):
        assert await _ids(engine, source="orders", filters=["late_returns.id is not null"]) == {1}

    async def test_pushed_down_anti_join(self, engine):
        assert await _ids(engine, source="orders", filters=["late_returns.id is null"]) == {2, 3, 4}

    async def test_null_extended_pushdown(self, engine):
        assert await _ids(engine, source="orders", filters=["late_returns.id is null or amount > 350"]) == {2, 3, 4}

    async def test_pushdown_under_aggregation(self, engine):
        resp = await _run(engine, source_model="orders", dimensions=["status"],
                          measures=[{"formula": "count(*)", "name": "n"}], filters=["late_returns.id is null"])
        assert {r["orders.status"]: int(r["orders.n"]) for r in resp.data} == {"done": 2, "open": 1}

    async def test_stage_source(self, engine):
        resp = await engine.execute([
            {"name": "s", "source_model": "done_orders", "dimensions": ["id"]},
            {"source_model": "s", "dimensions": ["id"]},
        ])
        assert ids(resp, "s") == {1, 2, 3}

    async def test_to_one_dimension_null_extends(self, engine):
        resp = await _run(engine, source_model="returns", dimensions=["id", "done_orders.status"])
        assert by_id(resp, "returns", "done_orders.status") == {10: "done", 11: "done", 12: "done", 13: None}

    async def test_to_one_null_test_keeps_outer_rows(self, engine):
        assert await _ids(engine, source="returns", filters=["done_orders.id is null"]) == {13}

    async def test_cross_model_aggregate(self, engine):
        resp = await _run(engine, source_model="orders", dimensions=["id"], measures=["count(late_returns.*)"])
        assert {k: int(v) for k, v in by_id(resp, "orders", "late_returns._count").items()} == {1: 1, 2: 0, 3: 0, 4: 0}

    async def test_inner_join_drops_filtered_out_target(self, engine):
        resp = await _run(engine, source_model="returns_inner", dimensions=["id", "done_orders.status"])
        assert by_id(resp, "returns_inner", "done_orders.status") == {10: "done", 11: "done", 12: "done"}

    @pytest.mark.parametrize("target", ["done_orders_sql", "done_orders_qb"])
    async def test_sql_and_query_backed_targets(self, engine, target):
        source = {"done_orders_sql": "returns_sql", "done_orders_qb": "returns_qb"}[target]
        resp = await _run(engine, source_model=source, dimensions=["id", f"{target}.status"])
        assert by_id(resp, source, f"{target}.status") == {10: "done", 11: "done", 12: "done", 13: None}

    async def test_filter_reads_joined_models_filtered_rows(self, engine):
        assert await _ids(engine, source="returns_of_done") == {10, 11, 12}

    async def test_derived_column_reads_joined_models_filtered_rows(self, engine):
        resp = await _run(engine, source_model="returns", dimensions=["id", "done_status"])
        assert by_id(resp, "returns", "done_status") == {10: "done", 11: "done", 12: "done", 13: None}


class TestUnrelatedDatasetReadsFilteredRows:
    async def test_filtered_model_as_the_unrelated_side(self, engine):
        resp = await _run(engine, source_model="shipments", measures=[{"formula": "count(done_orders.id)", "name": "n"}])
        assert [int(r["shipments.n"]) for r in resp.data] == [3]

    async def test_filtered_model_as_the_population_side(self, engine):
        resp = await _run(engine, source_model="done_orders", measures=[
            {"formula": "count(*)", "name": "n"}, {"formula": "count(shipments.id)", "name": "s"},
        ])
        assert [(int(r["done_orders.n"]), int(r["done_orders.s"])) for r in resp.data] == [(3, 4)]


class TestFilteredRoot:
    async def test_dimension(self, engine):
        assert await _ids(engine, source="done_orders") == {1, 2, 3}

    async def test_measure(self, engine):
        resp = await _run(engine, source_model="done_orders", measures=[{"formula": "sum(amount)", "name": "s"}])
        assert [int(r["done_orders.s"]) for r in resp.data] == [600]

    async def test_isolated_aggregate(self, engine):
        resp = await _run(engine, source_model="done_orders", measures=[{"formula": "count(returns.*)", "name": "n"}])
        assert [int(r["done_orders.n"]) for r in resp.data] == [3]

    async def test_windowed_aggregate(self, engine):
        resp = await _run(
            engine, source_model="done_orders",
            time_dimensions=[{"dimension": "ordered_at", "granularity": "month"}],
            measures=[{"formula": "sum(amount, window='366d')", "name": "w"}],
        )
        assert {k: int(v) for (k, (v,)) in _per_month(resp, "w").items()} == {
            "2024-01": 100, "2024-02": 300, "2024-03": 600,
        }

    async def test_time_shifted_aggregate(self, engine):
        resp = await _run(
            engine, source_model="done_orders",
            time_dimensions=[{"dimension": "ordered_at", "granularity": "month"}],
            measures=[{"formula": "sum(amount)", "name": "s"}, {"formula": "time_shift(sum(amount), -1)", "name": "p"}],
        )
        got = {k: tuple(None if x is None else int(x) for x in v) for k, v in _per_month(resp, "s", "p").items()}
        assert got == {"2024-01": (100, None), "2024-02": (200, 100), "2024-03": (300, 200)}

    async def test_date_range(self, engine):
        resp = await _run(
            engine, source_model="done_orders",
            time_dimensions=[{"dimension": "ordered_at", "granularity": "month", "date_range": ["2024-02-01", "2024-02-29"]}],
            measures=[{"formula": "sum(amount)", "name": "s"}],
        )
        assert {k: int(v) for (k, (v,)) in _per_month(resp, "s").items()} == {"2024-02": 200}

    async def test_spine_cells_are_filtered_rows_only(self, engine):
        resp = await _run(
            engine, source_model="done_orders", dimensions=["id"],
            time_dimensions=[{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": JAN_MAR}],
            measures=[{"formula": "count(*)", "name": "n"}],
        )
        cells = {(int(r["done_orders.id"]), _month(r["time_spine.timestamp"])): int(r["done_orders.n"])
                 for r in resp.data}
        assert {i for i, _ in cells} == {1, 2, 3}
        assert {k for k, n in cells.items() if n} == {(1, "2024-01"), (2, "2024-02"), (3, "2024-03")}


class TestListParentReadsFilteredRows:
    @pytest.fixture
    async def team_a(self) -> AsyncIterator[SlayerQueryEngine]:
        model = contractors_model().model_copy(update={"filters": ["team = 'A'"]})
        async with contractors_engine("duckdb", models=[model]) as eng:
            yield eng

    async def test_element_count(self, team_a):
        resp = await team_a.execute(SlayerQuery.model_validate({
            "source_model": "contractors", "measures": [{"formula": "count(skills.*)", "name": "n"}],
        }))
        assert [int(r["contractors.n"]) for r in resp.data] == [4]

    async def test_element_values(self, team_a):
        resp = await team_a.execute(SlayerQuery.model_validate({
            "source_model": "contractors", "dimensions": ["skills.value"],
            "measures": [{"formula": "count(skills.*)", "name": "n"}],
        }))
        assert {r["contractors.skills.value"]: int(r["contractors.n"]) for r in resp.data} == {"go": 2, "sql": 2}


class TestAssociation:
    async def test_unproven_hop_counts_each_row_once(self, engine):
        resp = await _run(engine, source_model="ups_orders", measures=[{"formula": "count(*)", "name": "n"}])
        assert [int(r["ups_orders.n"]) for r in resp.data] == [2]

    async def test_join_target_across_unproven_hop_never_multiplies(self, engine):
        resp = await _run(engine, source_model="returns", dimensions=["id", "ups_orders.id"])
        assert len(resp.data) == 4
        assert by_id(resp, "returns", "ups_orders.id") == {10: 1, 11: 1, 12: None, 13: None}

    async def test_missing_joined_row_reads_as_null(self, engine):
        assert await _ids(engine, source="ups_or_big_orders") == {1, 3, 4}


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
class TestInlinedAndAssociationFormsAgree:
    """Lowering soundness: inlining a proven to-one filter read equals its association form."""

    QUERY = {"source_model": "returns_of_done", "dimensions": ["id"]}

    def test_same_rows(self, backend, tmp_path):
        if backend == "duckdb":
            pytest.importorskip("duckdb")
        inlined = render(self.QUERY, dialect=backend)
        association = render(self.QUERY, dialect=backend, force_association=True)
        assert not list(sqlglot.parse_one(inlined, read=backend).find_all(exp.Exists)), inlined
        assert list(sqlglot.parse_one(association, read=backend).find_all(exp.Exists)), association
        db = seeded_file(backend, str(tmp_path))
        inlined_rows = sorted(raw_rows(dialect=backend, db_path=db, sql=inlined))
        association_rows = sorted(raw_rows(dialect=backend, db_path=db, sql=association))
        assert inlined_rows == association_rows == [(10,), (11,), (12,)]
