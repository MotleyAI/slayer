"""DEV-1837 — the dimension-family × measure-family compatibility matrix,
executed on SQLite + DuckDB. The matrix is the migration's tracked frontier:
supported cells assert hand-computed oracles (``tests/_dev1837_fixtures.py``);
still-guarded cells strict-xfail pointing at their stage issue, so a lift
XPASSes and forces the cell flip (design D6). It shrinking to empty is the
migration's definition of done.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  Banded dimension with a time-shift measure ......... band-time_shift cell
  Banded dimension with change and change_pct ........ band-change / band-change_pct
  Banded dimension with a running total .............. band-cumsum
  Bare partitioned aggregate as dim + transform ...... bare-* transform cells
  Transform-root dimension with a transform measure .. rank-* transform cells
  Union-grain (mixed) dimension family (DEV-1839) .... mixed-* cells
  Alongside a partitioned measure .................... TestCoexistenceTriples
  Adding a transform measure is cardinality-neutral .. TestCardinalityNeutrality
  Running total partitions by a computed dimension ... expr-cumsum
  Attached value never widens the grain .............. TestPartitionedShiftFix
  Explicit partition_by on the transform wins ........ TestExplicitTransformPartition
(…/specs/queries/partitioned-aggregates):
  Partitioned measure with a time-shift executes ..... TestPartitionedShiftFix
  Shifted re-aggregation grain excludes the value .... TestPartitionedShiftFix
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1837_fixtures import (
    CM_TOTAL,
    COL_WM,
    DIM_FAMILY_DIMS,
    GROUP_M,
    GROUP_M_MONTH,
    ModelMeasure,
    REGION_LAST,
    REGION_TOTAL,
    TD_TRANSFORM_OPS,
    TRAILING_90D_REGION,
    TRANSFORM_FORMULAS,
    TRANSFORM_X,
    dim_key,
    make_exec_engine,
    month_td,
    q,
    with_nulls,
)
from tests._engine_helpers import _extract_cte_body


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


ATTACH_MEASURES = {
    "plain": "amount:sum",
    "part": "amount:sum(partition_by=region)",
    "win_part": "amount:sum(window='90d', partition_by=region)",
    "last_part": "amount:last(partition_by=region)",
    "wm": "amount:sum(window='1y')",
    "rk": "amount:last",
    "cm": "customers.spend:sum",
}
TD_ATTACH = frozenset({"win_part", "wm"})

#: Still-guarded cells → the stage issue their strict-xfail points at.
XFAIL_CELLS = {
    ("band", "wm"): "DEV-1835: row attach × bare windowed measure",
    ("band", "rk"): "DEV-1835: row attach × bare first/last measure",
    ("band", "cm"): "DEV-1836: row attach × cross-model measure",
    ("bare", "wm"): "DEV-1835: row attach × bare windowed measure",
    ("bare", "rk"): "DEV-1835: row attach × bare first/last measure",
    ("bare", "cm"): "DEV-1836: row attach × cross-model measure",
    ("rank", "wm"): "DEV-1835: row attach × bare windowed measure",
    ("rank", "rk"): "DEV-1835: row attach × bare first/last measure",
    ("rank", "cm"): "DEV-1836: row attach × cross-model measure",
    ("expr", "wm"): "DEV-1835: ScalarCallKey in the windowed grain anchoring",
    ("expr", "rk"): "DEV-1835: ScalarCallKey in the ranked-CTE anchoring",
    ("mixed", "wm"): "DEV-1835: row attach × bare windowed measure",
    ("mixed", "rk"): "DEV-1835: row attach × bare first/last measure",
    ("mixed", "cm"): "DEV-1836: row attach × cross-model measure",
}

DIM_FAMILIES = tuple(DIM_FAMILY_DIMS)
MEASURE_KEYS = tuple(ATTACH_MEASURES) + tuple(TRANSFORM_FORMULAS)


def _formula(meas: str) -> str:
    return ATTACH_MEASURES.get(meas) or TRANSFORM_FORMULAS[meas]


def _needs_td(meas: str) -> bool:
    return meas in TD_ATTACH or meas in TD_TRANSFORM_OPS


def _cell_query(family: str, meas: str, *, include_x: bool = True):
    measures = [ModelMeasure(formula="amount:sum", name="m")]
    if include_x:
        measures.append(ModelMeasure(formula=_formula(meas), name="x"))
    kwargs = {"dimensions": DIM_FAMILY_DIMS[family], "measures": measures}
    if _needs_td(meas):
        kwargs["time_dimensions"] = month_td()
    return q(**kwargs)


def _expected_maps(family: str, meas: str):
    """``(m oracle, x oracle)`` keyed by the cell's group key."""
    m_map = GROUP_M_MONTH[family] if _needs_td(meas) else GROUP_M[family]
    keys = m_map.keys()
    if meas in TRANSFORM_FORMULAS:
        x_map = with_nulls(keys, TRANSFORM_X[(family, meas)])
    elif meas == "plain":
        x_map = dict(m_map)
    elif meas == "part":
        x_map = {k: REGION_TOTAL[k[0]] for k in keys}
    elif meas == "win_part":
        x_map = {k: TRAILING_90D_REGION[(k[0], k[-1])] for k in keys}
    elif meas == "last_part":
        x_map = {k: REGION_LAST[k[0]] for k in keys}
    elif meas == "wm":
        assert family == "col", "bare-window oracle derived for D-col only"
        x_map = dict(COL_WM)
    elif meas == "rk":
        assert family == "col", "bare-last oracle derived for D-col only"
        x_map = {k: REGION_LAST[k[0]] for k in keys}
    else:
        assert meas == "cm"
        x_map = {k: CM_TOTAL for k in keys}
    return m_map, x_map


def _keyed(resp, *, family: str, with_month: bool, cols=("m", "x")) -> dict:
    out = {
        dim_key(r, family=family, with_month=with_month):
            tuple(r[f"orders.{c}"] for c in cols)
        for r in resp.data
    }
    # Codex F5 — a dict build silently collapses duplicate group keys; the
    # cardinality invariant needs the row count pinned too.
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def _assert_x(actual, expected, *, key) -> None:
    if expected is None:
        assert actual is None, f"{key}: expected NULL, got {actual!r}"
    elif isinstance(expected, int):
        assert int(actual) == expected, f"{key}: expected {expected}, got {actual!r}"
    else:
        assert actual is not None, f"{key}: expected {expected}, got NULL"
        assert float(actual) == pytest.approx(expected), f"{key}"


_SUPPORTED = [
    (f, m) for f in DIM_FAMILIES for m in MEASURE_KEYS if (f, m) not in XFAIL_CELLS
]
_GUARDED = [
    pytest.param(
        f, m,
        marks=pytest.mark.xfail(
            strict=True, raises=NotImplementedError, reason=XFAIL_CELLS[(f, m)],
        ),
        id=f"{f}-{m}",
    )
    for (f, m) in XFAIL_CELLS
]


class TestMatrix:
    @pytest.mark.parametrize(
        ("family", "meas"), _SUPPORTED, ids=[f"{f}-{m}" for f, m in _SUPPORTED],
    )
    async def test_supported_cell_executes_with_oracle_values(
        self, exec_backend, family: str, meas: str,
    ) -> None:
        dialect, engine = exec_backend
        query = _cell_query(family, meas)
        with_month = _needs_td(meas)
        m_map, x_map = _expected_maps(family, meas)

        resp = await engine.execute(query)
        got = _keyed(resp, family=family, with_month=with_month)
        assert set(got) == set(m_map), f"result grain moved: {sorted(map(str, got))}"
        for key, (m_actual, x_actual) in got.items():
            assert float(m_actual) == pytest.approx(m_map[key]), f"{key}: m"
            _assert_x(x_actual, x_map[key], key=key)

        dry = await engine.execute(query, dry_run=True)
        assert dry.sql is not None
        assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
        assert_scope_closed(dry.sql, dialect=dialect)

    @pytest.mark.parametrize(("family", "meas"), _GUARDED)
    async def test_guarded_cell_solo_equality(
        self, exec_backend, family: str, meas: str,
    ) -> None:
        """D6 hybrid: raises NotImplementedError while guarded (strict xfail);
        on a lift this XPASSes and the cell must move to the supported table
        with a real oracle."""
        _, engine = exec_backend
        combined = await engine.execute(_cell_query(family, meas))
        solo = await engine.execute(_cell_query(family, meas, include_x=False))
        with_month = _needs_td(meas)
        got = _keyed(combined, family=family, with_month=with_month)
        solo_m = _keyed(solo, family=family, with_month=with_month, cols=("m",))
        assert set(got) == set(solo_m)
        for key, (m_actual, _) in got.items():
            assert float(m_actual) == pytest.approx(float(solo_m[key][0]))


class TestCoexistenceTriples:
    """Row attach + combined attach + transform in ONE query (the
    ``_render_with_cross_model_plans`` chain, task 2.5). Each value must also
    equal its value when queried alone (Codex F4 — direct solo runs, not just
    the shared oracle tables)."""

    @staticmethod
    def _triple_query(family: str, *, measures):
        return q(
            dimensions=DIM_FAMILY_DIMS[family],
            time_dimensions=month_td(),
            measures=measures,
        )

    async def _assert_triple(self, exec_backend, *, family: str, op: str) -> None:
        dialect, engine = exec_backend
        m = ModelMeasure(formula="amount:sum", name="m")
        rt = ModelMeasure(formula="amount:sum(partition_by=region)", name="rt")
        x = ModelMeasure(formula=TRANSFORM_FORMULAS[op], name="x")
        query = self._triple_query(family, measures=[m, rt, x])
        m_map = GROUP_M_MONTH[family]
        x_map = with_nulls(m_map.keys(), TRANSFORM_X[(family, op)])

        resp = await engine.execute(query)
        got = _keyed(resp, family=family, with_month=True, cols=("m", "rt", "x"))
        assert set(got) == set(m_map)
        for key, (m_actual, rt_actual, x_actual) in got.items():
            assert float(m_actual) == pytest.approx(m_map[key]), f"{key}: m"
            assert float(rt_actual) == pytest.approx(REGION_TOTAL[key[0]]), f"{key}: rt"
            _assert_x(x_actual, x_map[key], key=key)

        # Solo runs: each measure alone at the same grain yields the same value.
        for name, measure in (("m", m), ("rt", rt), ("x", x)):
            solo = await engine.execute(
                self._triple_query(family, measures=[measure]),
            )
            solo_map = _keyed(solo, family=family, with_month=True, cols=(name,))
            assert set(solo_map) == set(got), f"solo {name} moved the grain"
            idx = ("m", "rt", "x").index(name)
            for key, (solo_value,) in solo_map.items():
                combined_value = got[key][idx]
                if solo_value is None or combined_value is None:
                    assert solo_value == combined_value, f"{key}: solo {name}"
                else:
                    assert float(combined_value) == pytest.approx(
                        float(solo_value),
                    ), f"{key}: solo {name}"

        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)
        # One flat WITH — the dependency-preserving CTE assembly (design D4)
        # must not nest or duplicate chains.
        tree = sqlglot.parse_one(dry.sql, read=dialect)
        assert len(list(tree.find_all(exp.With))) == 1, dry.sql

    @pytest.mark.parametrize("op", ["time_shift", "cumsum"])
    async def test_band_partitioned_and_transform_together(
        self, exec_backend, op: str,
    ) -> None:
        await self._assert_triple(exec_backend, family="band", op=op)

    @pytest.mark.parametrize("op", ["time_shift", "cumsum"])
    async def test_rank_dim_partitioned_and_transform_together(
        self, exec_backend, op: str,
    ) -> None:
        """Design D4's load-bearing case: the rank-dim producer carries an
        internal WITH of its own, so its hoisted CTEs must keep their
        dependency edges through the cross-model prelude (Codex F6)."""
        await self._assert_triple(exec_backend, family="rank", op=op)


class TestPartitionedShiftFix:
    """The placeholder-leak fix (``queries/partitioned-aggregates`` delta): a
    partitioned measure + a temporal transform must execute, and the shifted
    re-aggregation groups by the query grain only — never the attached value."""

    @pytest.mark.parametrize("op", ["time_shift", "change", "change_pct"])
    async def test_partitioned_measure_with_temporal_transform(
        self, exec_backend, op: str,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula=TRANSFORM_FORMULAS[op], name="x"),
            ],
        )
        x_map = with_nulls(GROUP_M_MONTH["col"].keys(), TRANSFORM_X[("col", op)])

        resp = await engine.execute(query)
        got = _keyed(resp, family="col", with_month=True, cols=("rt", "x"))
        assert set(got) == set(x_map)
        for key, (rt_actual, x_actual) in got.items():
            assert float(rt_actual) == pytest.approx(REGION_TOTAL[key[0]]), f"{key}: rt"
            _assert_x(x_actual, x_map[key], key=key)

        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
        assert_scope_closed(dry.sql, dialect=dialect)

    async def test_shifted_reaggregation_grain_excludes_attached_value(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        dry = await engine.execute(q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
            ],
        ), dry_run=True)
        body = _extract_cte_body(dry.sql, r"shifted_\w+")
        assert "__regroup__" not in body, body
        parsed = sqlglot.parse_one(body, read=dialect)
        group = parsed.args.get("group")
        assert group is not None, body
        # Exactly the query grain: region + the shifted month bucket — and
        # never the attached value or its producer (Codex F3).
        assert len(group.expressions) == 2, body
        group_sqls = sorted(e.sql(dialect=dialect) for e in group.expressions)
        assert any("region" in g for g in group_sqls), group_sqls
        assert any("ordered_at" in g for g in group_sqls), group_sqls
        for g in group_sqls:
            assert "amount" not in g, group_sqls
            assert "_cm_" not in g, group_sqls


class TestCardinalityNeutrality:
    """Codex F7 — adding a transform measure must not move the result grain of
    an aggregation-derived dimension (band, bare, AND transform-root)."""

    @pytest.mark.parametrize("family", ["band", "bare", "rank"])
    async def test_adding_transform_is_cardinality_neutral(
        self, exec_backend, family: str,
    ) -> None:
        _, engine = exec_backend
        base = await engine.execute(_cell_query(family, "cumsum", include_x=False))
        plus = await engine.execute(_cell_query(family, "cumsum"))
        base_m = _keyed(base, family=family, with_month=True, cols=("m",))
        plus_m = _keyed(plus, family=family, with_month=True, cols=("m",))
        assert set(base_m) == set(plus_m)
        for key, (m_actual,) in plus_m.items():
            assert float(m_actual) == pytest.approx(float(base_m[key][0]))


class TestConsecutivePeriodsReset:
    async def test_streak_resets_and_recovers_within_banded_group(
        self, exec_backend,
    ) -> None:
        """Codex F7 — a pass→fail→pass sequence inside one banded group:
        band 1 months are 25 / 40 / 85, so ``amount:sum != 40`` gives streaks
        1, 0, 1 (band 0's 30 / 30 gives 1, 2)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[DIM_FAMILY_DIMS["band"][1]],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(
                    formula="consecutive_periods(amount:sum != 40)", name="x",
                ),
            ],
        ))
        got = {
            (int(r["orders.band"]), str(r["orders.ordered_at"])[:7]):
                int(r["orders.x"])
            for r in resp.data
        }
        assert got == {
            (0, "2024-01"): 1, (0, "2024-02"): 2,
            (1, "2024-01"): 1, (1, "2024-02"): 0, (1, "2024-03"): 1,
        }


class TestExplicitTransformPartition:
    async def test_explicit_partition_by_on_transform_wins(self, exec_backend) -> None:
        """``rank(amount:sum, partition_by=region)`` ranks within each region
        regardless of the banded dimension (explicit keys take precedence over
        the auto-grain; only the rank family accepts a transform partition_by)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=DIM_FAMILY_DIMS["band"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(
                    formula="rank(amount:sum, partition_by=region)", name="x",
                ),
            ],
        ))
        got = {
            dim_key(r, family="band", with_month=False): int(r["orders.x"])
            for r in resp.data
        }
        # Within-region rank of the band totals: North 60, 40 → 1, 2; the
        # single South and NULL rows both rank 1.
        assert got == {
            ("North", 0): 1, ("North", 1): 2, ("South", 1): 1, (None, 1): 1,
        }
