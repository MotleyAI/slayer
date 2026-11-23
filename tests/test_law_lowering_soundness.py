"""LAW lowering soundness (semantics.arc42.md §3 law 6): emission tricks are
pure optimizations. Assoc-inline half: an association-restricting filter on a
proven to-one hop equals a raw inline-WHERE JOIN oracle on the same database
file. Fusion half: fused vs forced-unfused renders of one planned query are
value-identical — the claim is scoped to the no-transform branch, the only
point where a fusion choice exists (transform/combined paths have no fused
alternative). Untrusted-hop half: the distinct projection equals association
on data violating declared cardinality, and trusting a hop changes nothing on
clean data."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.sql.generator import SQLGenerator

from slayer.core.query import QueryMetric

from slayer.core.enums import RelationshipCardinality

from tests._dev1739_fixtures import dev1739_datasets
from tests._dev2437_fixtures import (
    CLEAN,
    DUP_C1,
    REGION,
    REVENUE,
    SPLIT_C1,
    customers_dataset,
    orders_dataset,
    seed_datasets,
    to_customers,
)
from tests._dev2437_fixtures import engine as dev2437_engine
from tests._law_harness import (
    make_law_engine,
    month_key,
    month_td,
    q,
    raw_rows,
)

_MONTH_EXPR = {
    "sqlite": "substr(o.ordered_at, 1, 7)",
    "duckdb": "strftime(date_trunc('month', o.ordered_at), '%Y-%m')",
}


@pytest.fixture(params=["sqlite", "duckdb"])
async def law_backend(request):
    async for engine, db_path in make_law_engine(request.param):
        yield request.param, engine, db_path


def _keyed_engine(resp, *, with_month: bool) -> dict:
    out = {}
    for row in resp.data:
        key: tuple = (row["orders.region"],)
        if with_month:
            key += (month_key(row["orders.ordered_at"]),)
        out[key] = float(row["m"])
    assert len(out) == len(resp.data), "duplicate group keys"
    return out


class TestAssocInline:
    async def _assert_matches_oracle(self, law_backend, *, with_month: bool):
        dialect, engine, db_path = law_backend
        kwargs: dict = {"dimensions": ["orders.region"]}
        if with_month:
            kwargs["time_dimensions"] = month_td()
        resp = await engine.execute(q(
            **kwargs,
            filter=["customers.tier = 'gold'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="m")],
        ))
        month_col = f", {_MONTH_EXPR[dialect]}" if with_month else ""
        oracle = raw_rows(dialect=dialect, db_path=db_path, sql=(
            f"SELECT o.region{month_col}, SUM(o.amount) FROM orders AS o "
            f"JOIN customers AS c ON o.customer_id = c.id "
            f"WHERE c.tier = 'gold' GROUP BY o.region{month_col}"
        ))
        expected = {tuple(row[:-1]): float(row[-1]) for row in oracle}
        got = _keyed_engine(resp, with_month=with_month)
        assert got == pytest.approx(expected), (
            f"LAW lowering soundness violated — association filter != inline "
            f"WHERE on the to-one hop: {got} != {expected}"
        )

    async def test_association_filter_equals_inline_where_region_grain(
        self, law_backend,
    ) -> None:
        await self._assert_matches_oracle(law_backend, with_month=False)

    async def test_association_filter_equals_inline_where_month_grain(
        self, law_backend,
    ) -> None:
        await self._assert_matches_oracle(law_backend, with_month=True)


class TestFusionParity:
    @staticmethod
    def _planned():
        models = dev1739_datasets()
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=models[0], referenced_datasets=list(models[1:]),
        )
        query = q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="m")],
        )
        return plan_query(query=query, bundle=bundle), bundle

    @staticmethod
    def _render_twice(generator, planned, bundle) -> str:
        first = generator.generate_from_planned(planned, bundle=bundle)
        second = generator.generate_from_planned(planned, bundle=bundle)
        assert first == second, "render is not stable across two runs"
        return first

    async def test_fused_and_forced_unfused_values_agree(self, law_backend):
        dialect, _engine, db_path = law_backend
        planned, bundle = self._planned()
        fused = self._render_twice(
            SQLGenerator(dialect=dialect), planned, bundle,
        )
        unfused = self._render_twice(
            SQLGenerator(dialect=dialect, force_unfused=True), planned, bundle,
        )
        fused_tree = sqlglot.parse_one(fused, read=dialect)
        assert not list(fused_tree.find_all(exp.With)), fused
        assert len(list(fused_tree.find_all(exp.Select))) == 1, fused
        unfused_tree = sqlglot.parse_one(unfused, read=dialect)
        assert len(list(unfused_tree.find_all(exp.Select))) > 1, (
            f"forced-unfused render did not take the wrap:\n{unfused}"
        )
        fused_raw = raw_rows(dialect=dialect, db_path=db_path, sql=fused)
        unfused_raw = raw_rows(dialect=dialect, db_path=db_path, sql=unfused)
        fused_rows = {r[0]: float(r[1]) for r in fused_raw}
        unfused_rows = {r[0]: float(r[1]) for r in unfused_raw}
        assert len(fused_rows) == len(fused_raw), "duplicate group keys (fused)"
        assert len(unfused_rows) == len(unfused_raw), (
            "duplicate group keys (unfused)"
        )
        assert fused_rows == pytest.approx(unfused_rows), (
            f"LAW lowering soundness violated — fusion changed values: "
            f"{fused_rows} != {unfused_rows}"
        )


_UNTRUSTED_QUERIES = [
    pytest.param(DUP_C1, {"dimensions": [REGION]}, id="dimension"),
    pytest.param(DUP_C1, {"filter": ["customers.region = 'EAST'"]}, id="filter"),
    pytest.param(SPLIT_C1, {"dimensions": [REGION], "filter": ["customers.tier = 'gold'"]},
                 id="same-branch-filter-and-dimension"),
]


def _values(resp) -> dict:
    return {row.get(REGION): float(row[REVENUE]) for row in resp.data}


class TestUntrustedHop:
    @pytest.mark.parametrize(("customers", "query"), _UNTRUSTED_QUERIES)
    @pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
    async def test_distinct_projection_equals_association_on_dirty_data(
        self, backend: str, customers: list, query: dict,
    ) -> None:
        # Keyless and many-to-many: no proof of to-one, so the oracle associates.
        associated = seed_datasets(customers=customers_dataset(keyed=False), orders=orders_dataset(
            relationship=to_customers(cardinality=RelationshipCardinality.MANY_TO_MANY)))
        async with dev2437_engine(backend, customers) as eng:
            projected = await eng.execute(q(**query, metrics=[REVENUE]))
        async with dev2437_engine(backend, customers, associated) as eng:
            oracle = await eng.execute(q(**query, metrics=[REVENUE]))
        assert _values(projected) == pytest.approx(_values(oracle)), (
            "LAW lowering soundness violated — the untrusted to-one lowering != association on dirty data"
        )

    @pytest.mark.parametrize("query", [p.values[1] for p in _UNTRUSTED_QUERIES],
                             ids=[str(p.id) for p in _UNTRUSTED_QUERIES])
    @pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
    async def test_trusted_equals_untrusted_on_clean_data(self, backend: str, query: dict) -> None:
        async with dev2437_engine(backend, CLEAN, trusted=True) as eng:
            trusted = await eng.execute(q(**query, metrics=[REVENUE]))
        async with dev2437_engine(backend, CLEAN) as eng:
            untrusted = await eng.execute(q(**query, metrics=[REVENUE]))
        assert _values(trusted) == pytest.approx(_values(untrusted)), (
            "LAW lowering soundness violated — trusting a hop changed values on clean data"
        )
