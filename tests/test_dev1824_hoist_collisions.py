"""DEV-1824 producer CTE-hoist (design D2) — producers that carry their own
internal relations render hoisted into ONE flat WITH, with globally unique
names, across sibling producers, three-level nesting, case-folding dialects,
and identifier-truncating dialects.

Covers the delta scenario "Two complex producers in one query"
(queries/partitioned-aggregates → Producers may require their own intermediate
relations) and tasks.md 1.4.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1824_fixtures import (
    ModelMeasure,
    REGION_LAST,
    SlayerQuery,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)
from tests._engine_helpers import _assert_valid_sql, _engine_generate


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _cte_names(sql: str, *, dialect: str) -> list:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [cte.alias_or_name for cte in tree.find_all(exp.CTE)]


def _assert_flat_and_unique(
    sql: str, *, dialect: str, fold: bool = False, max_ident_bytes: int = 0,
) -> None:
    _assert_valid_sql(sql, dialect=dialect)  # parses; no nested WITH
    assert_scope_closed(sql, dialect=dialect)
    names = _cte_names(sql, dialect=dialect)
    keys = [n.lower() for n in names] if fold else names
    if max_ident_bytes:
        # Over-limit names truncate SERVER-side, so uniqueness must hold on
        # the fitted prefix and no name may exceed the dialect limit (Codex).
        assert all(len(k.encode("utf-8")) <= max_ident_bytes for k in keys), (
            f"CTE name over {max_ident_bytes} bytes in {names}"
        )
        keys = [k.encode("utf-8")[:max_ident_bytes] for k in keys]
    assert len(keys) == len(set(keys)), f"colliding CTE names {names} in:\n{sql}"
    assert "__regroup__" not in sql


#: Two ranked producers (partition sets {region} and {city}) — identical
#: internal CTE shapes that must hoist under distinct names.
TWO_PRODUCERS = dict(
    dimensions=["region", "city"],
    measures=[
        ModelMeasure(formula="amount:last(partition_by=region)", name="a"),
        ModelMeasure(formula="amount:last(partition_by=city)", name="b"),
    ],
)


class TestSiblingProducers:
    @pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
    async def test_two_identical_shape_producers_hoist(self, dialect: str) -> None:
        sql = await gen(q(**TWO_PRODUCERS), dialect=dialect)
        _assert_flat_and_unique(sql, dialect=dialect)

    async def test_two_producers_execute_with_correct_values(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(**TWO_PRODUCERS))
        by = rows_by(resp, "orders.region", "orders.city")
        city_last = {
            "CityA": 20.0, "CityB": 40.0, None: 30.0, "CityC": 25.0, "CityD": 60.0,
        }
        assert len(by) == 5
        for (region, city), r in by.items():
            assert float(r["orders.a"]) == pytest.approx(REGION_LAST[region])
            assert float(r["orders.b"]) == pytest.approx(city_last[city])


class TestThreeLevelNesting:
    #: Root WITH ← producers ← producers' own internal step relations: a
    #: transform-bearing dimension producer, a windowed producer, and a ranked
    #: producer in one query.
    CBAND = (
        "CASE WHEN cumsum(amount:sum(partition_by=[region, ordered_at])) > 50 "
        "THEN 1 ELSE 0 END"
    )

    def _query(self):
        return q(
            dimensions=["region", {"expression": self.CBAND, "name": "cband"}],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(
                    formula="amount:sum(window='90d', partition_by=region)", name="w",
                ),
                ModelMeasure(formula="amount:last(partition_by=region)", name="l"),
            ],
        )

    @pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
    async def test_nested_producers_hoist_flat(self, dialect: str) -> None:
        sql = await gen(self._query(), dialect=dialect)
        _assert_flat_and_unique(sql, dialect=dialect)

    async def test_nested_producers_execute(self, exec_engine) -> None:
        # cband: cumsum of region-month totals AT region grain (dimension
        # context): (N,01)=30→0 (N,02)=100→1 (S,01)=25→0 (S,03)=50→0
        # (NULL,03)=60→1.
        resp = await exec_engine.execute(self._query())
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"])):
                (int(r["orders.cband"]), float(r["orders.s"]))
            for r in resp.data
        }
        assert got == {
            ("North", "2024-01"): (0, pytest.approx(30.0)),
            ("North", "2024-02"): (1, pytest.approx(70.0)),
            ("South", "2024-01"): (0, pytest.approx(25.0)),
            ("South", "2024-03"): (0, pytest.approx(25.0)),
            (None, "2024-03"): (1, pytest.approx(60.0)),
        }


class TestCaseFoldingDialect:
    async def test_producer_names_unique_after_case_folding(self) -> None:
        sql = await gen(q(**TWO_PRODUCERS), dialect="snowflake")
        _assert_flat_and_unique(sql, dialect="snowflake", fold=True)


#: >63-byte shared prefix: fitted Postgres identifiers must not collide.
LONG_PREFIX = "metric_" + "x" * 60


def _long_names_model() -> SlayerModel:
    return SlayerModel(
        name="t", data_source="test", sql_table="t",
        default_time_dimension="at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="grp", type=DataType.TEXT),
            Column(name="at", type=DataType.TIMESTAMP),
            Column(name=f"{LONG_PREFIX}_alpha", type=DataType.DOUBLE),
            Column(name=f"{LONG_PREFIX}_beta", type=DataType.DOUBLE),
        ],
    )


class TestTruncatingDialect:
    async def test_fitted_producer_names_do_not_collide(self) -> None:
        model = _long_names_model()
        query = SlayerQuery(
            source_model=model.name,
            dimensions=["grp"],
            measures=[
                ModelMeasure(
                    formula=f"{LONG_PREFIX}_alpha:last(partition_by=grp)", name="a",
                ),
                ModelMeasure(
                    formula=f"{LONG_PREFIX}_beta:last(partition_by=[])", name="b",
                ),
            ],
        )
        sql = await _engine_generate(query=query, model=model, dialect="postgres")
        _assert_flat_and_unique(sql, dialect="postgres", max_ident_bytes=63)
