"""Executed ground truth (SQLite + DuckDB) for the lifted
``partition_by=`` metric shapes — the ``queries/partitioned-aggregates`` delta
scenarios. Every expectation is hand-computed in ``tests/_dev1824_fixtures.py``.

Scenario coverage map (spec: openspec …/specs/queries/partitioned-aggregates):
  Rolling total at a coarser grain .............. TestWindowPlusPartition
  Requires a resolvable time dimension .......... TestWindowPlusPartition
  Region-wide latest value on city rows ......... TestFirstLastPartition
  Temporal partition keys do not hijack ......... TestFirstLastPartition
  Running total of partition-grain values ....... TestTransformOverPartitioned
  Ranking result rows by an attached total ...... TestTransformOverPartitioned
  Keep rows whose partition total qualifies ..... TestFilterOnPartitioned
  Conjunction splits by scope ................... TestFilterOnPartitioned
  Mixing with a plain aggregate is legal ........ TestFilterOnPartitioned
  No common scope fails closed .................. TestFilterOnPartitioned
  Dimension banding and a partitioned measure ... TestRowCombinedCoexistence
  Same aggregate in both roles .................. TestRowCombinedCoexistence
  ORDER BY the raw aggregate alongside …  ....... TestRowCombinedCoexistence
  Partitioned plus windowed measure ............. TestCoexistenceAcrossFamilies
  Partitioned plus first/last, cross-model, … ... TestCoexistenceAcrossFamilies
  Adding a partitioned measure is neutral ....... TestCardinalityNeutral
  Empty partition set attaches overall total .... TestCardinalityNeutral
(Two complex producers → tests/test_dev1824_hoist_collisions.py;
 Golden baselines hold → tests/test_dev1824_golden_sql.py.)
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1824_fixtures import (
    BAND35,
    BAND35_OF,
    CITY_TOTAL,
    GRAND_TOTAL,
    RCM_GROUPS,
    RC_GROUPS,
    REGION_FIRST,
    REGION_LAST,
    REGION_MONTH_LAST,
    REGION_TOTAL,
    TRAILING_45D_REGION,
    TRAILING_90D_REGION,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _rcm_rows(resp) -> dict:
    """Rows keyed (region, city, month) with backend-stable month keys."""
    return {
        (r["orders.region"], r["orders.city"], month_key(r["orders.ordered_at"])): r
        for r in resp.data
    }


class TestWindowPlusPartition:
    async def test_rolling_total_at_coarser_grain(self, exec_engine) -> None:
        base = q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with_w = base.model_copy(update={"metrics": [
            *(base.metrics or []),
            QueryMetric(metric="sum(orders.amount, window='90d', partition_by=orders.region)", alias="w"),
        ]})
        plain = _rcm_rows(await exec_engine.execute(base))
        rows = _rcm_rows(await exec_engine.execute(with_w))
        assert set(rows) == RCM_GROUPS == set(plain)
        for (region, city, month), r in rows.items():
            assert float(r["w"]) == pytest.approx(
                TRAILING_90D_REGION[(region, month)]
            )
            # Adding the metric changes no other column's value.
            assert float(r["s"]) == pytest.approx(
                float(plain[(region, city, month)]["s"])
            )
        # Identical across cities of the same (region, month).
        assert rows[("North", "CityB", "2024-02")]["w"] == (
            rows[("North", None, "2024-02")]["w"]
        )

    async def test_window_duration_bounds_the_trailing_interval(
        self, exec_engine,
    ) -> None:
        # 45d ≠ running total on this dataset (Codex): (N,Feb) drops the
        # Jan 10 row → 90; (S,Mar) drops the Jan 25 row → 25.
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="sum(orders.amount, window='45d', partition_by=orders.region)", alias="w",
            )],
        ))
        rows = _rcm_rows(resp)
        assert set(rows) == RCM_GROUPS
        for (region, _city, month), r in rows.items():
            assert float(r["w"]) == pytest.approx(
                TRAILING_45D_REGION[(region, month)]
            )

    async def test_requires_resolvable_time_dimension(self, exec_engine) -> None:
        query = q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(
                metric="sum(orders.amount, window='1y', partition_by=orders.region)", alias="w",
            )],
        )
        with pytest.raises(ValueError, match=r"resolve its time dimension"):
            await exec_engine.execute(query)


class TestFirstLastPartition:
    async def test_region_last_on_city_rows(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="s"),
                QueryMetric(metric="last(orders.amount, partition_by=orders.region)", alias="l"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == RC_GROUPS
        for (region, city), r in by.items():
            assert float(r["l"]) == pytest.approx(REGION_LAST[region])
            assert float(r["s"]) == pytest.approx(CITY_TOTAL[(region, city)])

    async def test_region_first_on_city_rows(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="first(orders.amount, partition_by=orders.region)", alias="f")],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == RC_GROUPS
        for (region, _city), r in by.items():
            assert float(r["f"]) == pytest.approx(REGION_FIRST[region])

    async def test_temporal_partition_key_does_not_hijack_ranking(
        self, exec_engine,
    ) -> None:
        # (North, Jan) holds two rows in one month bucket (Jan 10 → 10,
        # Jan 20 → 20): ranking by the truncated partition bucket instead of the
        # raw ordered_at would tie and pick either; the pinned 20 is only
        # deterministic when ranking stays on the dataset's resolved time field.
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="last(orders.amount, partition_by=[orders.region, orders.ordered_at])", alias="l",
            )],
        ))
        rows = _rcm_rows(resp)
        assert set(rows) == RCM_GROUPS
        for (region, _city, month), r in rows.items():
            assert float(r["l"]) == pytest.approx(
                REGION_MONTH_LAST[(region, month)]
            )


class TestTransformOverPartitioned:
    async def test_cumsum_of_region_month_totals(self, exec_engine) -> None:
        # cumsum over the operand's (region, month) cells, broadcast onto each city row.
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]))",
                alias="c",
            )],
        ))
        rows = _rcm_rows(resp)
        expected = {
            ("North", "CityA", "2024-01"): 30.0,
            ("North", "CityB", "2024-02"): 100.0,
            ("North", None, "2024-02"): 100.0,
            ("South", "CityC", "2024-01"): 25.0,
            ("South", "CityC", "2024-03"): 50.0,
            (None, "CityD", "2024-03"): 60.0,
        }
        assert set(rows) == set(expected)
        for key, value in expected.items():
            assert float(rows[key]["c"]) == pytest.approx(value)

    async def test_rank_rows_by_attached_region_total(self, exec_engine) -> None:
        # The region cells rank by their totals 100/60/50, broadcast onto their cities.
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(
                metric="rank(sum(orders.amount, partition_by=orders.region), direction='desc')", alias="r",
            )],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        got = {key: int(r["r"]) for key, r in by.items()}
        assert got == {
            ("North", "CityA"): 1, ("North", "CityB"): 1, ("North", None): 1,
            (None, "CityD"): 2, ("South", "CityC"): 3,
        }


class TestFilterOnPartitioned:
    async def test_keep_rows_whose_partition_total_qualifies(self, exec_engine) -> None:
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == {k for k in RC_GROUPS if REGION_TOTAL[k[0]] > 50}
        for key, r in by.items():
            assert float(r["s"]) == pytest.approx(
                float(unfiltered[key]["s"])
            )

    async def test_conjunction_splits_by_scope(self, exec_engine) -> None:
        combined = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 and orders.city != 'CityB'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        separate = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50", "orders.city != 'CityB'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        key = lambda resp: {  # noqa: E731
            (r["orders.region"], r["orders.city"]): float(r["s"])
            for r in resp.data
        }
        assert key(combined) == key(separate)
        # Hand-computed: the row conjunct excludes CityB AND the NULL city
        # (NULL != 'CityB' is NULL), so filtered region totals are North=30,
        # South=50, NULL=60 — only the NULL region clears > 50.
        assert key(combined) == {(None, "CityD"): pytest.approx(60.0)}

    async def test_mixed_with_plain_aggregate_is_legal(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 and sum(orders.amount) > 25"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == {
            k for k in RC_GROUPS
            if REGION_TOTAL[k[0]] > 50 and CITY_TOTAL[k] > 25
        }
        for key, r in by.items():
            assert float(r["s"]) == pytest.approx(CITY_TOTAL[key])

    async def test_no_common_scope_fails_closed(self, exec_engine) -> None:
        # `status` resolves only before aggregation; the partitioned aggregate
        # only after attachment — an OR spanning both has no home.
        query = q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 or orders.status == 'ok'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with pytest.raises((ValueError, NotImplementedError)) as ei:
            await exec_engine.execute(query)
        message = str(ei.value).lower()
        assert "split" in message
        # A permanent semantic error (design D7), not a deferred-shape guard.
        assert "not yet supported" not in message
        assert "__regroup__" not in message

    async def test_empty_partition_set_filter_is_keyless_attach(
        self, exec_engine,
    ) -> None:
        keep_all = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=[]) > 100"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        assert len(keep_all.data) == len(RC_GROUPS)
        keep_none = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=[]) > 300"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        assert keep_none.data == []


class TestRowCombinedCoexistence:
    async def test_band_dimension_and_partitioned_measure(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", {"field": BAND35, "alias": "band"}],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="s"),
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
            ],
        ))
        by = {
            (r["orders.region"], int(r["band"])):
                (float(r["s"]), float(r["rt"]))
            for r in resp.data
        }
        assert by == {
            ("North", 0): (pytest.approx(60.0), pytest.approx(100.0)),
            ("North", 1): (pytest.approx(40.0), pytest.approx(100.0)),
            ("South", 1): (pytest.approx(50.0), pytest.approx(50.0)),
            (None, 1): (pytest.approx(60.0), pytest.approx(60.0)),
        }

    async def test_same_aggregate_in_both_roles(self, exec_engine) -> None:
        band = "CASE WHEN sum(orders.amount, partition_by=orders.region) > 55 THEN 1 ELSE 0 END"
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", {"field": band, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt")],
        ))
        got = {
            r["orders.region"]: (int(r["band"]), float(r["rt"]))
            for r in resp.data
        }
        assert got == {
            "North": (1, pytest.approx(100.0)),
            "South": (0, pytest.approx(50.0)),
            None: (1, pytest.approx(60.0)),
        }
        for band_value, rt in got.values():
            assert band_value == int(rt > 55)

    async def test_order_by_raw_aggregate_sorts_by_partition_value(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city", {"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{"field": "sum(orders.amount, partition_by=orders.city)", "direction": "asc"}],
        ))
        totals = [
            CITY_TOTAL[(r["orders.region"], r["orders.city"])] for r in resp.data
        ]
        assert len(totals) == len(RC_GROUPS)
        assert totals == sorted(totals)

    async def test_order_by_dimension_name_sorts_by_banded_value(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city", {"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{"field": "band", "direction": "asc"}],
        ))
        bands = [int(r["band"]) for r in resp.data]
        assert len(bands) == len(RC_GROUPS)
        assert bands == sorted(bands)
        assert bands == sorted(BAND35_OF.values())

    async def test_order_by_raw_finer_grain_raises_clean_error(
        self, exec_engine,
    ) -> None:
        # The issue-comment shape: city is NOT a query dimension, so the
        # combined attach for the order target has no host slot — a clean grain
        # error, never the leaked-placeholder ValueError.
        query = q(
            dimensions=["orders.region", {"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{
                "field": "sum(orders.amount, partition_by=orders.city)", "direction": "asc",
            }],
        )
        with pytest.raises(ValueError, match=r"not a query dimension") as ei:
            await exec_engine.execute(query)
        assert "__regroup__" not in str(ei.value)


class TestCoexistenceAcrossFamilies:
    async def _assert_solo_equality(self, exec_engine, *, dims, tds, metrics) -> None:
        """Each metric's combined-query values equal its solo-query values."""
        def _mk(pairs):
            return [QueryMetric(metric=f, alias=n) for f, n in pairs]

        def _index(resp):
            keys = list(dims)
            if tds:
                keys.append("orders.ordered_at")
            return {tuple(r[k] for k in keys): r for r in resp.data}

        combined = _index(await exec_engine.execute(q(
            dimensions=dims, time_dimensions=tds, metrics=_mk(metrics),
        )))
        for formula, name in metrics:
            solo = _index(await exec_engine.execute(q(
                dimensions=dims, time_dimensions=tds, metrics=_mk([(formula, name)]),
            )))
            assert set(solo) == set(combined), name
            for key, row in solo.items():
                got, want = combined[key][name], row[name]
                if want is None:
                    assert got is None, (name, key)
                else:
                    assert float(got) == pytest.approx(float(want)), (name, key)

    async def test_partitioned_plus_windowed(self, exec_engine) -> None:
        await self._assert_solo_equality(
            exec_engine, dims=["orders.region", "orders.city"], tds=month_td(),
            metrics=[
                ("sum(orders.amount, partition_by=orders.region)", "rt"),
                ("sum(orders.amount, window='1y')", "w"),
            ],
        )

    async def test_partitioned_plus_last(self, exec_engine) -> None:
        await self._assert_solo_equality(
            exec_engine, dims=["orders.region", "orders.city"], tds=None,
            metrics=[
                ("sum(orders.amount, partition_by=orders.region)", "rt"),
                ("last(orders.amount)", "l"),
            ],
        )

    async def test_partitioned_plus_cross_model(self, exec_engine) -> None:
        await self._assert_solo_equality(
            exec_engine, dims=["orders.region", "orders.city"], tds=None,
            metrics=[
                ("sum(orders.amount, partition_by=orders.region)", "rt"),
                ("sum(customers.spend)", "cm"),
            ],
        )

    async def test_partitioned_plus_transform(self, exec_engine) -> None:
        await self._assert_solo_equality(
            exec_engine, dims=["orders.region", "orders.city"], tds=month_td(),
            metrics=[
                ("sum(orders.amount, partition_by=orders.region)", "rt"),
                ("cumsum(sum(orders.amount))", "c"),
            ],
        )


class TestCardinalityNeutral:
    @pytest.mark.parametrize("dims,tds", [
        (["orders.region", "orders.city"], None),
        (["orders.region", {"field": BAND35, "alias": "band"}], None),
        (["orders.region", "orders.city"], "month"),
    ])
    async def test_adding_partitioned_measure_is_neutral(
        self, exec_engine: SlayerQueryEngine, dims, tds,
    ) -> None:
        tds = month_td() if tds else None
        base = q(
            dimensions=dims, time_dimensions=tds,
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with_rt = base.model_copy(update={"metrics": [
            *(base.metrics or []),
            QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
        ]})
        plain = await exec_engine.execute(base)
        extended = await exec_engine.execute(with_rt)
        assert len(extended.data) == len(plain.data)
        shared = set(plain.columns)
        strip = lambda r: {k: r[k] for k in shared}  # noqa: E731
        canon = lambda rows: sorted(  # noqa: E731
            (sorted(strip(r).items(), key=lambda kv: kv[0]) for r in rows),
            key=repr,
        )
        assert canon(extended.data) == canon(plain.data)

    async def test_empty_partition_set_attaches_overall_total(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount, partition_by=[])", alias="g")],
        ))
        assert len(resp.data) == len(RC_GROUPS)
        for r in resp.data:
            assert float(r["g"]) == pytest.approx(GRAND_TOTAL)
