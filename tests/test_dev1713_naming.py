"""DEV-1713 Stage 9 — full naming module: result-key / flat-name contract.

These tests are the Step-4 (tests-first) red state for Stage 9. They pin the
caller-facing contract the naming module owns:

* D3 (DEV-1495 bug 1): a joined DIMENSION — base OR derived, single- OR
  multi-hop — surfaces under the DOTTED final-stage key
  (``orders.customers.revenue``), matching cross-model MEASURES and the
  documented result-key contract, NOT the flat ``orders.customers__revenue``.
* Bare named-measure aliasing: a query referencing a saved measure by bare
  name surfaces under the measure NAME, not the formula-derived canonical.
* DEV-1692: multiple arithmetic-wrapped ``time_shift`` calls in one query
  render collision-free CTE names.
* Multi-stage DAG: INNER stages keep the flat ``__`` bind names while a FINAL
  stage that reaches through a join emits dotted keys (the two never mix).

Execution tests run against a seeded file-backed SQLite so result-KEY shape is
verified on real returned rows, not just the emitted SQL.
"""

from __future__ import annotations

import os
import re
import sqlite3
import tempfile
from typing import AsyncIterator, List

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import build_resolved_source_bundle
from slayer.engine.stage_planner import plan_stages
from slayer.sql.generator import generate_planned_stages
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


def _outer_select_columns(sql: str, *, dialect: str = "postgres") -> List[str]:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(parsed, exp.Select):  # pragma: no cover — defensive
        return []
    return [p.alias_or_name for p in parsed.expressions]


# ===========================================================================
# Seeded engine: orders -> customers -> regions, with derived columns.
# ===========================================================================


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, population REAL)")
    cur.executemany(
        "INSERT INTO regions VALUES (?,?,?)",
        [(1, "North", 1000.0), (2, "South", 2000.0)],
    )
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "revenue REAL, signup_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, 1, 100.0, "2024-01-05"),
            (2, 1, 50.0, "2024-02-10"),
            (3, 2, 70.0, "2024-01-20"),
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "amount REAL, created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, 1, 10.0, "2024-01-06"),
            (2, 1, 5.0, "2024-02-11"),
            (3, 2, 7.0, "2024-01-21"),
            (4, 3, 3.0, "2024-01-22"),
            (5, 3, 9.0, "2024-02-01"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="regions",
            sql_table="regions",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="population", type=DataType.DOUBLE),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="revenue", type=DataType.DOUBLE, label="Revenue"),
                Column(name="signup_at", type=DataType.TIMESTAMP),
                Column(name="rev_x2", sql="revenue * 2", type=DataType.DOUBLE, label="Rev x2"),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
    )
    yield SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# D3 — joined dimension keys are dotted (DEV-1495 bug 1)
# ---------------------------------------------------------------------------


class TestJoinedDimensionDottedKeys:
    async def test_joined_derived_dimension_key_is_dotted(self, engine) -> None:
        """The bug-1 shape: a joined DERIVED dimension (``customers.rev_x2``,
        a ``ColumnSqlKey`` with a non-empty path) must project + return rows
        under ``orders.customers.rev_x2``, not ``orders.customers__rev_x2``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.rev_x2")],
            measures=[ModelMeasure(formula="*:count", name="n")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.rev_x2" in resp.columns, resp.columns
        assert "orders.customers__rev_x2" not in resp.columns, resp.columns
        assert "orders.customers.rev_x2" in resp.attributes.dimensions
        assert "orders.customers.rev_x2" in resp.data[0]

    async def test_multi_hop_joined_dimension_key_is_dotted(self, engine) -> None:
        """A two-hop joined dimension (``customers.regions.name``) surfaces
        as ``orders.customers.regions.name`` — the documented multi-hop
        result-key form — not ``orders.customers__regions__name``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.regions.name")],
            measures=[ModelMeasure(formula="*:count", name="n")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.regions.name" in resp.columns, resp.columns
        assert "orders.customers__regions__name" not in resp.columns
        assert "orders.customers.regions.name" in resp.data[0]

    async def test_joined_base_dimension_key_is_dotted(self, engine) -> None:
        """Guard: a joined BASE column dimension (``ColumnKey`` with a path)
        was already dotted; the D3 fix must not regress it."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[ModelMeasure(formula="*:count", name="n")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.region_id" in resp.columns, resp.columns

    async def test_order_by_joined_dimension_matches_dotted_projection(
        self, engine,
    ) -> None:
        """A dim-only query ORDERed BY a joined dimension must sort by the
        SAME dotted key the projection emits — the ORDER BY alias
        (``orders.customers.regions.name``) must match the SELECT alias, not
        the flat ``orders.customers__regions__name`` (which names no projected
        column and fails at execution). Regression for the D3 projection change
        rippling into ORDER BY resolution."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.regions.name")],
            order=[OrderItem(column=ColumnRef(name="customers.regions.name"),
                             direction="asc")],
        )
        # Executes without a "no such column" error and returns the dotted key.
        resp = await engine.execute(query)
        assert "orders.customers.regions.name" in resp.columns, resp.columns
        dry = await engine.execute(query, dry_run=True)
        assert "orders.customers__regions__name" not in dry.sql, dry.sql
        assert "orders.customers.regions.name" in dry.sql

    async def test_cross_model_measure_key_still_dotted(self, engine) -> None:
        """Guard: cross-model MEASURES already surfaced dotted
        (``orders.customers.revenue_sum``); the fix keeps that."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[ModelMeasure(formula="customers.revenue:sum")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.revenue_sum" in resp.columns, resp.columns

    async def test_order_by_joined_dimension_in_cross_model_query(self, engine) -> None:
        """Guard: the cross-model (combined) ORDER BY path also sorts a joined
        dimension by its dotted key and executes cleanly (no flat-alias
        mismatch), alongside a cross-model measure."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.regions.name")],
            measures=[ModelMeasure(formula="customers.revenue:sum")],
            order=[OrderItem(column=ColumnRef(name="customers.regions.name"),
                             direction="asc")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.regions.name" in resp.columns, resp.columns
        assert "orders.customers.revenue_sum" in resp.columns

    async def test_time_dimension_over_joined_column_dotted(self, engine) -> None:
        """Codex F9: a time dimension over a JOINED column keeps the dotted
        leaf key (no granularity suffix), matching a base-column TD."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="customers.signup_at"),
                    granularity=TimeGranularity.MONTH,
                )
            ],
            measures=[ModelMeasure(formula="*:count", name="n")],
        )
        resp = await engine.execute(query)
        assert "orders.customers.signup_at" in resp.columns, resp.columns

    async def test_response_attributes_agree_with_sql_aliases(self, engine) -> None:
        """Codex F6: ``response_meta._slot_result_keys`` (which builds
        ``attributes``) and the generator's ``_full_alias_for_slot`` (which
        builds the SQL projection) must produce the SAME key for a joined
        DERIVED dimension. ``build_response_metadata`` drops any attribute
        whose result key is absent from the SQL projection, so a labeled
        derived joined dim surfaces in ``attributes.dimensions`` under the
        DOTTED key iff both producers agree on the dotted form.

        Uses only LOCAL measures: a derived joined dim combined with a
        cross-model measure is a separate, deliberately-deferred case (DEV-1708
        raises ``NotImplementedError`` for a derived dim used as a cross-model
        shared grain — full support tracked in DEV-1495-b1). The naming
        agreement this test pins needs no cross-model measure."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.rev_x2")],  # labeled -> surfaces
            measures=[
                ModelMeasure(formula="amount:sum"),
                ModelMeasure(formula="*:count", name="n"),
            ],
        )
        resp = await engine.execute(query, dry_run=True)
        # The labeled derived joined dim must appear under the DOTTED key.
        assert "orders.customers.rev_x2" in resp.attributes.dimensions, (
            resp.attributes.dimensions
        )
        assert "orders.customers__rev_x2" not in resp.attributes.dimensions
        # Invariant: every attribute key is a real projected column (no drift).
        attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert attr_keys <= set(resp.columns), (attr_keys, resp.columns)

    async def test_all_slot_types_generator_and_response_agree(self, engine) -> None:
        """Codex F6 (per slot-key type): a single query exercising local
        ColumnKey dim, joined ColumnKey dim, joined ColumnSqlKey (derived)
        dim, joined TimeTruncKey, local aggregate, and star-count. The
        generator's SQL aliases (``_full_alias_for_slot``) and response_meta's
        keys (``_slot_result_keys``) — the two independent producers the shared
        decomposition helper unifies — must agree on the dotted form for every
        one.

        No cross-model measure here: a derived joined dim combined with a
        cross-model aggregate is the DEV-1708-deferred shared-grain case
        (``NotImplementedError``, full support = DEV-1495-b1). Cross-model
        measure key agreement is covered by
        ``test_cross_model_measure_key_still_dotted``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[
                ColumnRef(name="customer_id"),          # local ColumnKey
                ColumnRef(name="customers.region_id"),   # joined ColumnKey
                ColumnRef(name="customers.rev_x2"),      # joined ColumnSqlKey (derived)
            ],
            time_dimensions=[
                TimeDimension(                            # joined TimeTruncKey
                    dimension=ColumnRef(name="customers.signup_at"),
                    granularity=TimeGranularity.MONTH,
                )
            ],
            measures=[
                ModelMeasure(formula="amount:sum"),          # local aggregate
                ModelMeasure(formula="*:count", name="n"),   # star-count
            ],
        )
        resp = await engine.execute(query, dry_run=True)
        expected = {
            "orders.customer_id",
            "orders.customers.region_id",
            "orders.customers.rev_x2",
            "orders.customers.signup_at",
            "orders.amount_sum",
            "orders.n",
        }
        # response_meta side (resp.columns) — every key dotted, none flattened.
        assert set(resp.columns) == expected, resp.columns
        # generator side (SQL projection) — must match response_meta exactly.
        sql_cols = set(_outer_select_columns(resp.sql, dialect="sqlite"))
        assert sql_cols == expected, sql_cols
        # No ``__``-flattened joined key leaked from either producer.
        assert not any("__" in c for c in resp.columns), resp.columns

    async def test_result_key_contract_quartet(self, engine) -> None:
        """The acceptance quartet in one query: local measure, star-count,
        multi-hop joined dim, and a user-renamed measure."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.regions.name")],
            measures=[
                ModelMeasure(formula="amount:sum"),
                ModelMeasure(formula="*:count"),
                ModelMeasure(formula="amount:sum", name="my_total"),
            ],
        )
        resp = await engine.execute(query, dry_run=True)
        cols = set(_outer_select_columns(resp.sql, dialect="sqlite"))
        assert {"orders.amount_sum", "orders._count",
                "orders.customers.regions.name", "orders.my_total"} <= cols, cols


# ---------------------------------------------------------------------------
# Multi-stage DAG — inner flat vs final dotted (Codex F1 / F12)
# ---------------------------------------------------------------------------


async def _new_sql(*, storage, stages, dialect="sqlite") -> str:
    root = stages[-1]
    named = {q.name: q for q in stages[:-1] if q.name}
    bundle = await build_resolved_source_bundle(
        query=root, storage=storage, named_queries=named
    )
    planned = plan_stages(queries=stages, bundle=bundle)
    return generate_planned_stages(planned, bundle=bundle, dialect=dialect)


class TestMultiStageNaming:
    async def test_inner_stage_flat_bind_still_works(self, engine) -> None:
        """A 2-stage DAG: the inner stage reaches through a join and exposes
        the joined dim under the FLAT downstream name (``customers__region_id``),
        which the root binds. Inner stages must stay flat (contract unchanged)."""
        storage = engine.storage
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        root = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name="customers__region_id")],
            measures=[ModelMeasure(formula="amount_sum:max", name="peak")],
        )
        sql = await _new_sql(storage=storage, stages=[stage1, root])
        # The inner stage CTE carries the FLAT column name.
        assert "customers__region_id" in sql, sql
        # And it executes and returns the expected root keys.
        resp = await engine.execute([stage1, root])
        assert set(resp.columns) == {"stage1.customers__region_id", "stage1.peak"}, (
            resp.columns
        )

    async def test_three_stage_flat_propagation_and_dotted_final(self, engine) -> None:
        """Codex F1 + F12: the SAME bug-prone slot — a joined DERIVED dim
        (``customers.rev_x2``, a ``ColumnSqlKey`` with a path) — renders FLAT
        (``customers__rev_x2``) when its owning stage is INNER and DOTTED
        (``orders.customers.rev_x2``) when its owning stage is FINAL. The
        planner's explicit is-final flag is what decides; the two forms never
        cross. An intermediate stage both consumes and re-exports the flat
        form, proving flat propagation through a 3-stage chain."""
        s1 = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[ColumnRef(name="customers.rev_x2")],  # inner: derived join
            measures=[ModelMeasure(formula="amount:sum")],
        )
        s2 = SlayerQuery(
            name="s2",
            source_model="s1",
            dimensions=[ColumnRef(name="customers__rev_x2")],  # consumes flat
            measures=[ModelMeasure(formula="amount_sum:max", name="peak")],
        )
        root = SlayerQuery(
            source_model="s2",
            dimensions=[ColumnRef(name="customers__rev_x2")],  # re-exports flat
            measures=[ModelMeasure(formula="peak:min", name="floor")],
        )
        sql = await _new_sql(storage=engine.storage, stages=[s1, s2, root])
        # The inner stage CTE binds the joined derived dim under the FLAT name.
        assert "customers__rev_x2" in sql, sql
        chain = await engine.execute([s1, s2, root])
        # Downstream binds stayed flat end-to-end; no dotted join key leaks up.
        assert set(chain.columns) == {"s2.customers__rev_x2", "s2.floor"}, (
            chain.columns
        )
        assert not any("customers.rev_x2" in c for c in chain.columns)

        # The SAME derived joined slot, owned by a FINAL single stage, is dotted.
        single = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.rev_x2")],
                measures=[ModelMeasure(formula="amount:sum")],
            )
        )
        assert "orders.customers.rev_x2" in single.columns, single.columns
        assert "orders.customers__rev_x2" not in single.columns


# ---------------------------------------------------------------------------
# Bare named-measure SELECT-alias naming
# ---------------------------------------------------------------------------


class TestLegacyFlattenerDelegation:
    """Codex F7: the legacy virtual-model flatteners must DELEGATE to
    ``flat_name`` (single owner), not keep their own ``.replace('.', '__')``
    bodies. Both are closures (``_alias_to_short`` inside
    ``SlayerQueryEngine._query_as_model``; ``_alias_to_short_local`` inside
    ``enrich_query``), so this inspects the enclosing source to assert the
    delegation call is present — a body that dropped it would fail here."""

    def test_query_as_model_delegates_to_flat_name(self) -> None:
        import inspect

        from slayer.engine.query_engine import SlayerQueryEngine

        src = inspect.getsource(SlayerQueryEngine._query_as_model)
        assert "flat_name(" in src, "expected _query_as_model to call flat_name()"

    def test_enrich_query_delegates_to_flat_name(self) -> None:
        import inspect

        from slayer.engine.enrichment import enrich_query

        src = inspect.getsource(enrich_query)
        assert "flat_name(" in src, "expected enrich_query to call flat_name()"


def _orders_named_model(measures=None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
        measures=measures or [],
    )


class TestBareNamedMeasureResultKey:
    async def test_bare_named_measure_projects_under_name(self) -> None:
        model = _orders_named_model(
            measures=[ModelMeasure(name="rev_total", formula="revenue:sum")]
        )
        query = SlayerQuery(source_model="orders", measures=["rev_total"])
        sql = await _engine_generate(query=query, model=model)
        cols = _outer_select_columns(sql)
        assert "orders.rev_total" in cols, cols
        assert "orders.revenue_sum" not in cols, cols

    async def test_explicit_name_overrides_saved_name(self) -> None:
        """Guard (Codex F4): an explicit ``name`` on the query measure wins
        over the saved measure's own name."""
        model = _orders_named_model(
            measures=[ModelMeasure(name="rev_total", formula="revenue:sum")]
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="rev_total", name="custom")],
        )
        sql = await _engine_generate(query=query, model=model)
        cols = _outer_select_columns(sql)
        assert "orders.custom" in cols, cols
        assert "orders.rev_total" not in cols

    async def test_bare_named_measure_order_by_with_dimension(self) -> None:
        """The DEV-1443 shape: ORDER BY the bare measure name resolves to the
        emitted alias even with a dimension present."""
        model = _orders_named_model(
            measures=[ModelMeasure(name="companies", formula="revenue:count_distinct")]
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=["companies"],
            order=[OrderItem(column="companies", direction="desc")],
        )
        sql = await _engine_generate(query=query, model=model)
        assert '"orders.companies"' in sql, sql
        assert "revenue_count_distinct" not in sql, sql

    async def test_self_qualified_saved_measure_ref_matches_bare(self) -> None:
        """Codex F4 (qualified ref): a SELF-qualified saved-measure reference
        (``orders.rev_total`` on source model ``orders``) is normalized to the
        bare form by ``SlayerQuery.strip_source_model_prefix`` before planning,
        so it preserves the measure NAME exactly like the bare reference — the
        two are equivalent, not divergent. Pinned so the equivalence is
        explicit."""
        model = _orders_named_model(
            measures=[ModelMeasure(name="rev_total", formula="revenue:sum")]
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="orders.rev_total")],
        )
        sql = await _engine_generate(query=query, model=model)
        cols = _outer_select_columns(sql)
        assert "orders.rev_total" in cols, cols
        assert "orders.revenue_sum" not in cols

    def test_measure_named_like_column_is_rejected_at_model_build(self) -> None:
        """Codex F4 (collision): a saved measure whose name equals a column
        name is impossible to construct — columns and measures share one
        namespace, guarded at model validation. This is why the bare-named-
        measure fix can never make a measure name shadow a selected dim of
        the same name: such a pair cannot exist on one model."""
        # Build the colliding measure outside the raises block so only the
        # model-construction call can throw (Sonar S5778 — one throwing
        # invocation per exception assertion).
        colliding = ModelMeasure(name="status", formula="revenue:sum")
        with pytest.raises(ValueError, match="name collision"):
            _orders_named_model(measures=[colliding])


# ---------------------------------------------------------------------------
# DEV-1692 — duplicate time_shift CTE de-collision
# ---------------------------------------------------------------------------


class TestTimeShiftCteDecollision:
    async def test_three_arithmetic_time_shifts_unique_ctes(self) -> None:
        """Three arithmetic-wrapped ``time_shift`` measures in one query must
        each get a distinct CTE name (no ``shifted__t0`` reuse). Proven with a
        regex over the emitted SQL — independent of ``assert_unique_cte_names``
        (which has its own unit coverage) so this pins the DEV-1692 REGRESSION
        even if the helper is absent."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                )
            ],
            measures=[
                ModelMeasure(
                    formula="revenue:sum - time_shift(revenue:sum, -1, 'month')",
                    name="g1",
                ),
                ModelMeasure(
                    formula="revenue:sum - time_shift(revenue:sum, -2, 'month')",
                    name="g2",
                ),
                ModelMeasure(
                    formula="revenue:sum - time_shift(revenue:sum, -3, 'month')",
                    name="g3",
                ),
            ],
        )
        sql = await _engine_generate(query=query, model=model)
        # Every CTE name in the statement is unique (regex over WITH/`,` heads).
        all_ctes = re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(all_ctes) == len(set(all_ctes)), (
            f"duplicate CTE names: {all_ctes}\n{sql}"
        )
        shifted = [c for c in all_ctes if c.startswith("shifted_")]
        assert len(shifted) == 3, f"expected 3 shifted CTEs, got {shifted}\n{sql}"

    async def test_hidden_time_shift_alias_avoids_user_column(self) -> None:
        """Codex (PR #269): the hidden time_shift alias placeholder
        (``_time_shift_inner``) must not shadow a real user measure literally
        named ``_time_shift_inner`` — the transform allocator reserves the
        projected aliases, so the hidden one is bumped to a distinct name and
        the arithmetic reads its own shift, not the user column."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                # A user measure whose name collides with the hidden placeholder.
                ModelMeasure(formula="revenue:sum", name="_time_shift_inner"),
                ModelMeasure(
                    formula="revenue:sum - time_shift(revenue:sum, -1, 'month')",
                    name="growth",
                ),
            ],
        )
        sql = await _engine_generate(query=query, model=model)
        cols = _outer_select_columns(sql, dialect="postgres")
        # The user measure keeps its own key.
        assert "orders._time_shift_inner" in cols, cols
        # The hidden shift alias was bumped off the user's name, not collided.
        assert "orders._time_shift_inner_2" in sql, sql

    async def test_two_consecutive_periods_unique_ctes(self) -> None:
        """DEV-1692 (sibling of time_shift): two arithmetic-wrapped
        consecutive_periods measures are hidden inner slots that share the
        placeholder ``_consecutive_periods_inner`` — their cp_reset/cp_value
        CTEs and value aliases must be de-collided, exactly like time_shift."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="consecutive_periods(revenue:sum > 0) + 1",
                             name="rev_streak"),
                ModelMeasure(formula="consecutive_periods(quantity:sum > 0) + 1",
                             name="qty_streak"),
            ],
        )
        sql = await _engine_generate(query=query, model=model)
        all_ctes = re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(all_ctes) == len(set(all_ctes)), (
            f"duplicate CTE names: {all_ctes}\n{sql}"
        )
        assert len([c for c in all_ctes if c.startswith("cp_reset_")]) == 2, sql
        assert len([c for c in all_ctes if c.startswith("cp_value_")]) == 2, sql
