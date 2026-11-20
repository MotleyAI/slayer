"""Full naming module: result-key / flat-name contract.

These tests are the Step-4 (tests-first) red state for Stage 9. They pin the
caller-facing contract the naming module owns:

* D3: a joined DIMENSION — base OR derived, single- OR
  multi-hop — surfaces under the DOTTED final-stage key
  (``orders.customers.revenue``), matching cross-dataset METRICS and the
  documented result-key contract, NOT the flat ``orders.customers__revenue``.
* Bare named-metric aliasing: a query referencing a saved metric by bare
  name surfaces under the metric NAME, not the expression-derived canonical.
* Multiple arithmetic-wrapped ``time_shift`` calls in one query
  render collision-free CTE names.
* Multi-stage DAG: INNER stages keep the flat ``__`` bind names while a FINAL
  stage that reaches through a relationship emits dotted keys (the two never mix).

Execution tests run against a seeded file-backed SQLite so result-KEY shape is
verified on real returned rows, not just the emitted SQL.
"""

from __future__ import annotations

import inspect
import os
import re
from slayer.storage.sqlite_conn import transaction
import tempfile
from typing import AsyncIterator, List

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.plan import plan_stages
from slayer.sql.generator import generate_planned_stages
from slayer.sql.stage_wrapper import build_flat_rename_wrapper
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate
import slayer.engine.bundle_builder


def _outer_select_columns(sql: str, *, dialect: str = "postgres") -> List[str]:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(parsed, exp.Select):  # pragma: no cover — defensive
        return []
    return [p.alias_or_name for p in parsed.expressions]


# ===========================================================================
# Seeded engine: orders -> customers -> regions, with derived fields.
# ===========================================================================


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    with transaction(db_path) as con:
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

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_dataset(
        SlayerDataset(
            name="regions",
            sql_table="regions",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name", datatype=DataType.STRING),
                DatasetField(name="population", datatype=DataType.FLOAT),
            ],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region_id", datatype=DataType.INTEGER),
                DatasetField(name="revenue", datatype=DataType.FLOAT, label="Revenue"),
                DatasetField(name="signup_at", datatype=DataType.DATETIME),
                DatasetField(name="rev_x2", expression="revenue * 2", datatype=DataType.FLOAT, label="Rev x2"),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="prod",
            default_time_dimension="created_at",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="amount", datatype=DataType.FLOAT),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
    )
    yield SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# D3 — joined dimension keys are dotted
# ---------------------------------------------------------------------------


class TestJoinedDimensionDottedKeys:
    async def test_joined_derived_dimension_key_is_dotted(self, engine: SlayerQueryEngine) -> None:
        """The bug-1 shape: a joined DERIVED dimension (``customers.rev_x2``,
        a ``ColumnSqlKey`` with a non-empty path) must project + return rows
        under ``orders.customers.rev_x2``, not ``orders.customers__rev_x2``."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.rev_x2")],
            metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        )
        resp = await engine.execute(query)
        assert "customers.rev_x2" in resp.columns, resp.columns
        assert "orders.customers__rev_x2" not in resp.columns, resp.columns
        assert "customers.rev_x2" in resp.attributes.dimensions
        assert "customers.rev_x2" in resp.data[0]

    async def test_multi_hop_joined_dimension_key_is_dotted(self, engine: SlayerQueryEngine) -> None:
        """A two-hop joined dimension (``customers.regions.name``) surfaces
        as ``orders.customers.regions.name`` — the documented multi-hop
        result-key form — not ``orders.customers__regions__name``."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.regions.name")],
            metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        )
        resp = await engine.execute(query)
        assert "customers.regions.name" in resp.columns, resp.columns
        assert "orders.customers__regions__name" not in resp.columns
        assert "customers.regions.name" in resp.data[0]

    async def test_joined_base_dimension_key_is_dotted(self, engine: SlayerQueryEngine) -> None:
        """Guard: a joined BASE field dimension (``ColumnKey`` with a path)
        was already dotted; the D3 fix must not regress it."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        )
        resp = await engine.execute(query)
        assert "customers.region_id" in resp.columns, resp.columns

    async def test_order_by_joined_dimension_matches_dotted_projection(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """A dim-only query ORDERed BY a joined dimension must sort by the
        SAME dotted key the projection emits — the ORDER BY alias
        (``orders.customers.regions.name``) must match the SELECT alias, not
        the flat ``orders.customers__regions__name`` (which names no projected
        column and fails at execution). Regression for the D3 projection change
        rippling into ORDER BY resolution."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.regions.name")],
            order=[OrderItem(field="customers.regions.name",
                             direction="asc")],
        )
        # Executes without a "no such column" error and returns the dotted key.
        resp = await engine.execute(query)
        assert "customers.regions.name" in resp.columns, resp.columns
        dry = await engine.execute(query, dry_run=True)
        assert dry.sql is not None
        assert "orders.customers__regions__name" not in dry.sql, dry.sql
        assert "customers.regions.name" in dry.sql

    async def test_cross_model_measure_key_still_dotted(self, engine: SlayerQueryEngine) -> None:
        """Guard: cross-dataset METRICS already surfaced dotted
        (``orders.customers.revenue_sum``); the fix keeps that."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[QueryMetric(metric="sum(customers.revenue)")],
        )
        resp = await engine.execute(query)
        assert "customers.revenue_sum" in resp.columns, resp.columns

    async def test_order_by_joined_dimension_in_cross_model_query(self, engine: SlayerQueryEngine) -> None:
        """Guard: the cross-dataset (combined) ORDER BY path also sorts a joined
        dimension by its dotted key and executes cleanly (no flat-alias
        mismatch), alongside a cross-dataset metric."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.regions.name")],
            metrics=[QueryMetric(metric="sum(customers.revenue)")],
            order=[OrderItem(field="customers.regions.name",
                             direction="asc")],
        )
        resp = await engine.execute(query)
        assert "customers.regions.name" in resp.columns, resp.columns
        assert "customers.revenue_sum" in resp.columns

    async def test_time_dimension_over_joined_column_dotted(self, engine: SlayerQueryEngine) -> None:
        """Codex F9: a time dimension over a JOINED field keeps the dotted
        leaf key (no granularity suffix), matching a base-field TD."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="customers.signup_at",
                    granularity=TimeGranularity.MONTH,
                )
            ],
            metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        )
        resp = await engine.execute(query)
        assert "customers.signup_at" in resp.columns, resp.columns

    async def test_response_attributes_agree_with_sql_aliases(self, engine: SlayerQueryEngine) -> None:
        """Codex F6: ``response_meta._slot_result_keys`` (which builds
        ``attributes``) and the generator's ``_full_alias_for_slot`` (which
        builds the SQL projection) must produce the SAME key for a joined
        DERIVED dimension. ``build_response_metadata`` drops any attribute
        whose result key is absent from the SQL projection, so a labeled
        derived joined dim surfaces in ``attributes.dimensions`` under the
        DOTTED key iff both producers agree on the dotted form.

        Uses only LOCAL metrics: a derived joined dim combined with a
        cross-dataset metric (a derived cross-dataset shared grain) now renders;
        that combination is covered
        end-to-end in tests/test_dev1728_derived_shared_grain.py. The naming
        agreement this test pins needs no cross-dataset metric, so it stays
        focused on the local-metric shape."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.rev_x2")],  # labeled -> surfaces
            metrics=[
                QueryMetric(metric="sum(orders.amount)"),
                QueryMetric(metric="count(orders.*)", alias="n"),
            ],
        )
        resp = await engine.execute(query, dry_run=True)
        # The labeled derived joined dim must appear under the DOTTED key.
        assert "customers.rev_x2" in resp.attributes.dimensions, (
            resp.attributes.dimensions
        )
        assert "orders.customers__rev_x2" not in resp.attributes.dimensions
        # Invariant: every attribute key is a real projected column (no drift).
        attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.metrics)
        assert attr_keys <= set(resp.columns), (attr_keys, resp.columns)

    async def test_all_slot_types_generator_and_response_agree(self, engine: SlayerQueryEngine) -> None:
        """Codex F6 (per slot-key type): local ColumnKey dim, joined ColumnKey
        dim, joined ColumnSqlKey (derived) dim, joined TimeTruncKey, local
        aggregate, cross-dataset aggregate, and star-count. The generator's SQL
        aliases (``_full_alias_for_slot``) and response_meta's keys
        (``_slot_result_keys``) — the two independent producers the shared
        decomposition helper unifies — must agree on the dotted form for every
        one.

        Two queries, not one: kept split for focused per-slot-type naming
        coverage. The plain derived joined dim + cross-dataset aggregate
        combination now renders and is
        covered end-to-end in tests/test_dev1728_derived_shared_grain.py; this
        test stays split so each slot-type's key agreement is pinned in
        isolation."""

        async def _check(query: SlayerQuery, expected: set) -> None:
            resp = await engine.execute(query, dry_run=True)
            # response_meta side (resp.columns) — every key dotted, none flattened.
            assert set(resp.columns) == expected, resp.columns
            # generator side (SQL projection) — must match response_meta exactly.
            assert resp.sql is not None
            sql_cols = set(_outer_select_columns(resp.sql, dialect="sqlite"))
            assert sql_cols == expected, sql_cols
            # No ``__``-flattened joined key leaked from either producer.
            assert not any("__" in c for c in resp.columns), resp.columns

        await _check(
            SlayerQuery(
                population="orders",
                dimensions=[
                    QueryDimension(field="orders.customer_id"),          # local ColumnKey
                    QueryDimension(field="customers.region_id"),   # joined ColumnKey
                    QueryDimension(field="customers.rev_x2"),      # joined ColumnSqlKey (derived)
                ],
                time_dimensions=[
                    TimeDimension(                            # joined TimeTruncKey
                        field="customers.signup_at",
                        granularity=TimeGranularity.MONTH,
                    )
                ],
                metrics=[
                    QueryMetric(metric="sum(orders.amount)"),        # local aggregate
                    QueryMetric(metric="count(orders.*)", alias="n"),  # star-count
                ],
            ),
            {
                "orders.customer_id",
                "customers.region_id",
                "customers.rev_x2",
                "customers.signup_at",
                "orders.amount_sum",
                "n",
            },
        )
        await _check(
            SlayerQuery(
                population="orders",
                dimensions=[
                    QueryDimension(field="orders.customer_id"),          # local ColumnKey
                    QueryDimension(field="customers.region_id"),   # joined ColumnKey
                ],
                time_dimensions=[
                    TimeDimension(                            # joined TimeTruncKey
                        field="customers.signup_at",
                        granularity=TimeGranularity.MONTH,
                    )
                ],
                metrics=[
                    QueryMetric(metric="sum(orders.amount)"),          # local aggregate
                    QueryMetric(metric="sum(customers.revenue)"),  # cross-dataset aggregate
                    QueryMetric(metric="count(orders.*)", alias="n"),   # star-count
                ],
            ),
            {
                "orders.customer_id",
                "customers.region_id",
                "customers.signup_at",
                "orders.amount_sum",
                "customers.revenue_sum",
                "n",
            },
        )

    async def test_result_key_contract_quartet(self, engine) -> None:
        """The acceptance quartet in one query: local metric, star-count,
        multi-hop joined dim, and a user-renamed metric."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.regions.name")],
            metrics=[
                QueryMetric(metric="sum(orders.amount)"),
                QueryMetric(metric="count(orders.*)"),
                QueryMetric(metric="sum(orders.amount)", alias="my_total"),
            ],
        )
        resp = await engine.execute(query, dry_run=True)
        cols = set(_outer_select_columns(resp.sql, dialect="sqlite"))
        assert {"orders.amount_sum", "orders._count",
                "customers.regions.name", "my_total"} <= cols, cols


# ---------------------------------------------------------------------------
# Multi-stage DAG — inner flat vs final dotted (Codex F1 / F12)
# ---------------------------------------------------------------------------


async def _new_sql(*, storage, stages, dialect="sqlite") -> str:
    root = stages[-1]
    named = {q.name: q for q in stages[:-1] if q.name}
    bundle = await slayer.engine.bundle_builder.build_resolved_source_bundle(
        query=root, storage=storage, named_queries=named
    )
    planned = plan_stages(queries=stages, bundle=bundle)
    return generate_planned_stages(planned, bundle=bundle, dialect=dialect)


class TestMultiStageNaming:
    async def test_inner_stage_flat_bind_still_works(self, engine: SlayerQueryEngine) -> None:
        """A 2-stage DAG: the inner stage reaches through a relationship and exposes
        the joined dim under the FLAT downstream name (``customers__region_id``),
        which the root binds. Inner stages must stay flat (contract unchanged)."""
        storage = engine.storage
        stage1 = SlayerQuery(
            name="stage1",
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[QueryMetric(metric="sum(orders.amount)")],
        )
        root = SlayerQuery(
            population="stage1",
            dimensions=[QueryDimension(field="stage1.customers__region_id")],
            metrics=[QueryMetric(metric="max(stage1.amount_sum)", alias="peak")],
        )
        sql = await _new_sql(storage=storage, stages=[stage1, root])
        # The inner stage CTE carries the FLAT field name.
        assert "customers__region_id" in sql, sql
        # And it executes and returns the expected root keys.
        resp = await engine.execute([stage1, root])
        assert set(resp.columns) == {"stage1.customers__region_id", "peak"}, (
            resp.columns
        )

    async def test_three_stage_flat_propagation_and_dotted_final(self, engine: SlayerQueryEngine) -> None:
        """Codex F1 + F12: the SAME bug-prone slot — a joined DERIVED dim
        (``customers.rev_x2``, a ``ColumnSqlKey`` with a path) — renders FLAT
        (``customers__rev_x2``) when its owning stage is INNER and DOTTED
        (``orders.customers.rev_x2``) when its owning stage is FINAL. The
        planner's explicit is-final flag is what decides; the two forms never
        cross. An intermediate stage both consumes and re-exports the flat
        form, proving flat propagation through a 3-stage chain."""
        s1 = SlayerQuery(
            name="s1",
            population="orders",
            dimensions=[QueryDimension(field="customers.rev_x2")],  # inner: derived, across a relationship
            metrics=[QueryMetric(metric="sum(orders.amount)")],
        )
        s2 = SlayerQuery(
            name="s2",
            population="s1",
            dimensions=[QueryDimension(field="s1.customers__rev_x2")],  # consumes flat
            metrics=[QueryMetric(metric="max(s1.amount_sum)", alias="peak")],
        )
        root = SlayerQuery(
            population="s2",
            dimensions=[QueryDimension(field="s2.customers__rev_x2")],  # re-exports flat
            metrics=[QueryMetric(metric="min(s2.peak)", alias="floor")],
        )
        sql = await _new_sql(storage=engine.storage, stages=[s1, s2, root])
        # The inner stage CTE binds the joined derived dim under the FLAT name.
        assert "customers__rev_x2" in sql, sql
        chain = await engine.execute([s1, s2, root])
        # Downstream binds stayed flat end-to-end; no dotted join key leaks up.
        assert set(chain.columns) == {"s2.customers__rev_x2", "floor"}, (
            chain.columns
        )
        assert not any("customers.rev_x2" in c for c in chain.columns)

        # The SAME derived joined slot, owned by a FINAL single stage, is dotted.
        single = await engine.execute(
            SlayerQuery(
                population="orders",
                dimensions=[QueryDimension(field="customers.rev_x2")],
                metrics=[QueryMetric(metric="sum(orders.amount)")],
            )
        )
        assert "customers.rev_x2" in single.columns, single.columns
        assert "orders.customers__rev_x2" not in single.columns


# ---------------------------------------------------------------------------
# Bare named-metric SELECT-alias naming
# ---------------------------------------------------------------------------


class TestFlattenerDelegation:
    """Codex F7: every downstream-bind-name flattener must DELEGATE to
    ``flat_name`` (single owner), not keep its own ``.replace('.', '__')``
    body. The typed pipeline's flattener is ``build_flat_rename_wrapper``
    (the ``_alias_to_short_local`` successor: same
    strip-source-relation-then-``__``-flatten contract for stage CTEs and the
    query-backed virtual-dataset wrap).

    The companion guard over the legacy virtual-dataset
    closure (``_alias_to_short`` inside ``SlayerQueryEngine._query_as_model``)
    is gone with the legacy enrichment stack, leaving ONE flattener to own."""

    def test_stage_flat_rename_wrapper_delegates_to_flat_name(self) -> None:
        src = inspect.getsource(build_flat_rename_wrapper)
        assert "flat_name(" in src, (
            "expected build_flat_rename_wrapper to call flat_name()"
        )


def _orders_named_dataset(metrics=None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
            DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
        ],
        metrics=metrics or [],
    )


class TestBareNamedMeasureResultKey:
    async def test_bare_named_measure_projects_under_name(self) -> None:
        model = _orders_named_dataset(
            metrics=[DatasetMetric(name="rev_total", expression="sum(revenue)")]
        )
        query = SlayerQuery(population="orders", metrics=["orders.rev_total"])
        sql = await _engine_generate(query=query, dataset=model)
        cols = _outer_select_columns(sql)
        assert "orders.rev_total" in cols, cols
        assert "orders.revenue_sum" not in cols, cols

    async def test_explicit_name_overrides_saved_name(self) -> None:
        """Guard (Codex F4): an explicit ``name`` on the query metric wins
        over the saved metric's own name."""
        model = _orders_named_dataset(
            metrics=[DatasetMetric(name="rev_total", expression="sum(revenue)")]
        )
        query = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="orders.rev_total", alias="custom")],
        )
        sql = await _engine_generate(query=query, dataset=model)
        cols = _outer_select_columns(sql)
        assert "custom" in cols, cols
        assert "orders.rev_total" not in cols

    async def test_bare_named_measure_order_by_with_dimension(self) -> None:
        """ORDER BY the bare metric name resolves to the
        emitted alias even with a dimension present."""
        model = _orders_named_dataset(
            metrics=[DatasetMetric(name="companies", expression="count_distinct(revenue)")]
        )
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=["orders.companies"],
            order=[OrderItem(field="orders.companies", direction="desc")],
        )
        sql = await _engine_generate(query=query, dataset=model)
        assert '"orders.companies"' in sql, sql
        assert "revenue_count_distinct" not in sql, sql

    async def test_self_qualified_saved_measure_ref_matches_bare(self) -> None:
        """Codex F4 (qualified ref): a SELF-qualified saved-metric reference
        (``orders.rev_total`` on source dataset ``orders``) resolves at binding
        as the bare form, so it preserves the metric NAME exactly like the bare reference — the
        two are equivalent, not divergent. Pinned so the equivalence is
        explicit."""
        model = _orders_named_dataset(
            metrics=[DatasetMetric(name="rev_total", expression="sum(revenue)")]
        )
        query = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="orders.rev_total")],
        )
        sql = await _engine_generate(query=query, dataset=model)
        cols = _outer_select_columns(sql)
        assert "orders.rev_total" in cols, cols
        assert "orders.revenue_sum" not in cols

    def test_measure_named_like_column_is_rejected_at_model_build(self) -> None:
        """Codex F4 (collision): a saved metric whose name equals a field
        name is impossible to construct — fields and metrics share one
        namespace, guarded at dataset validation. This is why the bare-named-
        metric fix can never make a metric name shadow a selected dim of
        the same name: such a pair cannot exist on one dataset."""
        # Build the colliding metric outside the raises block so only the
        # dataset-construction call can throw (Sonar S5778 — one throwing
        # invocation per exception assertion).
        colliding = DatasetMetric(name="status", expression="sum(revenue)")
        with pytest.raises(ValueError, match="name collision"):
            _orders_named_dataset(metrics=[colliding])


# ---------------------------------------------------------------------------
# Duplicate time_shift CTE de-collision
# ---------------------------------------------------------------------------


class TestTimeShiftCteDecollision:
    async def test_three_arithmetic_time_shifts_unique_ctes(self) -> None:
        """Three arithmetic-wrapped ``time_shift`` metrics in one query must
        each get a distinct join-back CTE name over their one shared relation. Proven with a
        regex over the emitted SQL — independent of ``assert_unique_cte_names``
        (which has its own unit coverage) so this pins the de-collision REGRESSION
        even if the helper is absent."""
        model = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
                DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.created_at",
                    granularity=TimeGranularity.MONTH,
                )
            ],
            metrics=[
                QueryMetric(
                    metric="sum(orders.revenue) - time_shift(sum(orders.revenue), -1, 'month')",
                    alias="g1",
                ),
                QueryMetric(
                    metric="sum(orders.revenue) - time_shift(sum(orders.revenue), -2, 'month')",
                    alias="g2",
                ),
                QueryMetric(
                    metric="sum(orders.revenue) - time_shift(sum(orders.revenue), -3, 'month')",
                    alias="g3",
                ),
            ],
        )
        sql = await _engine_generate(query=query, dataset=model)
        # Every CTE name in the statement is unique (regex over WITH/`,` heads).
        all_ctes = re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(all_ctes) == len(set(all_ctes)), (
            f"duplicate CTE names: {all_ctes}\n{sql}"
        )
        # One input: its shifted relation is shared, each offset joins it back.
        shifted = [c for c in all_ctes if c.startswith("shifted_")]
        assert len(shifted) == 1, f"expected 1 shifted CTE, got {shifted}\n{sql}"
        sjoins = [c for c in all_ctes if c.startswith("sjoin_")]
        assert len(sjoins) == 3, f"expected 3 sjoin CTEs, got {sjoins}\n{sql}"
        offsets = set(re.findall(r"- INTERVAL '(\d) MONTH'", sql))
        assert offsets == {"1", "2", "3"}, sql

    async def test_hidden_time_shift_alias_avoids_user_column(self) -> None:
        """Codex (PR #269): the hidden time_shift alias placeholder
        (``_time_shift_inner``) must not shadow a real user metric literally
        named ``_time_shift_inner`` — the transform allocator reserves the
        projected aliases, so the hidden one is bumped to a distinct name and
        the arithmetic reads its own shift, not the user metric."""
        model = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
                DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at",
                granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                # A user metric whose name collides with the hidden placeholder.
                QueryMetric(metric="sum(orders.revenue)", alias="_time_shift_inner"),
                QueryMetric(
                    metric="sum(orders.revenue) - time_shift(sum(orders.revenue), -1, 'month')",
                    alias="growth",
                ),
            ],
        )
        sql = await _engine_generate(query=query, dataset=model)
        cols = _outer_select_columns(sql, dialect="postgres")
        # The user metric keeps its own key.
        assert "_time_shift_inner" in cols, cols
        # The hidden shift alias was bumped off the user's name, not collided.
        assert "_time_shift_inner_2" in sql, sql

    async def test_two_consecutive_periods_unique_ctes(self) -> None:
        """Sibling of time_shift: two arithmetic-wrapped
        consecutive_periods metrics are hidden inner slots that share the
        placeholder ``_consecutive_periods_inner`` — their cp_reset/cp_value
        CTEs and value aliases must be de-collided, exactly like time_shift."""
        model = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
                DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="quantity", expression="quantity", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            time_dimensions=[TimeDimension(
                field="orders.created_at",
                granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                QueryMetric(metric="consecutive_periods(sum(orders.revenue) > 0) + 1",
                             alias="rev_streak"),
                QueryMetric(metric="consecutive_periods(sum(orders.quantity) > 0) + 1",
                             alias="qty_streak"),
            ],
        )
        sql = await _engine_generate(query=query, dataset=model)
        all_ctes = re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(all_ctes) == len(set(all_ctes)), (
            f"duplicate CTE names: {all_ctes}\n{sql}"
        )
        assert len([c for c in all_ctes if c.startswith("cp_reset_")]) == 2, sql
        assert len([c for c in all_ctes if c.startswith("cp_value_")]) == 2, sql
