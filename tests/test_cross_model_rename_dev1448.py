"""DEV-1448 — user-supplied ``name`` on a join-traversed (cross-model) measure
must propagate to the rendered SQL projection and to downstream nested-DAG
stages, exactly like the local-column rename does today.

Bug shape: stage 1 declares ``{"formula": "<other>.<col>:<agg>", "name": "x"}``.
The current cross-model branch in ``slayer/engine/enrichment.py`` (lines
894-918) calls ``_resolve_cross_model_measure`` and ignores ``qfield.name`` —
the returned ``CrossModelMeasure.alias`` keeps the canonical
``<query_model>.<hop_path>.<col>_<agg>`` form. Result: the top-level result
column key surfaces as the canonical form, and the downstream-stage virtual
model (built by ``_query_as_model`` via ``_alias_to_short(cm.alias)``)
exposes the column as ``<hop_path>__<col>_<agg>`` — so a downstream
``x:max`` reference fails with ``Column 'x' not found``.

Fix: after the cross-model branch appends the ``CrossModelMeasure``, mutate
``cm.alias`` and ``cm.name`` to the user-supplied form when ``qfield.name``
is non-redundant — symmetric with the local-measure rename at lines
1021-1029. The alias drops the hop path entirely (``orders.x``), so the
downstream virtual model column becomes ``x`` and ``x:max`` resolves.

Companion / deferred scope:
* DEV-1445 — cross-model colon-form filter remap (e.g. ``customers.revenue:sum
  > 100`` resolving to user alias). Not in scope here. Tests that probe this
  boundary are marked ``pytest.mark.skip``.
* DEV-1446 — transform-wrapped agg refs (e.g. ``cumsum(customers.revenue:sum)``
  with ``name``). Out of scope; pinned via a skipped test.
"""
from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures: orders → customers (single hop) and orders → customers → regions
# (multi-hop) for the dotted-path rename test.
# ---------------------------------------------------------------------------


async def _save_test_datasource(storage: YAMLStorage) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="revenue", sql="lifetime_revenue", type=DataType.DOUBLE),
        ],
    )


def _customers_model_with_region_join() -> SlayerModel:
    """Customers model that joins to regions — used by the multi-hop test."""
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="revenue", sql="lifetime_revenue", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions",
        sql_table="regions",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="population", sql="population", type=DataType.DOUBLE),
        ],
    )


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


@pytest.fixture
async def orders_customers_engine(tmp_path) -> tuple[SlayerQueryEngine, SlayerModel]:
    """orders → customers (single hop)."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_test_datasource(storage)
    await storage.save_model(_customers_model())
    orders = _orders_model()
    await storage.save_model(orders)
    return SlayerQueryEngine(storage=storage), orders


@pytest.fixture
async def orders_customers_regions_engine(tmp_path) -> tuple[SlayerQueryEngine, SlayerModel]:
    """orders → customers → regions (two hops, for multi-hop rename test)."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_test_datasource(storage)
    await storage.save_model(_regions_model())
    await storage.save_model(_customers_model_with_region_join())
    orders = _orders_model()
    await storage.save_model(orders)
    return SlayerQueryEngine(storage=storage), orders


# ---------------------------------------------------------------------------
# Group A — Single-stage rename: cm.alias must reflect the user-supplied name.
# ---------------------------------------------------------------------------


class TestCrossModelRenameSingleStage:
    async def test_cross_model_rename_top_level_result_key(
        self, orders_customers_engine,
    ) -> None:
        """Top-level query (no nesting). With ``name="cust_rev"`` on a
        cross-model measure, the public projection key swaps only the
        canonical LEAF to the user name — the hop path stays:
        ``orders.customers.cust_rev``. This matches the dot-syntax
        convention every other multi-hop caller-facing key uses. Pins:

        * outer alias has the hop-preserved shape, name is the bare user
          identifier
        * ``cm.user_declared`` survives the rename
        * the INNER ``cm.measure`` (used to build the CTE's aggregate
          expression) intentionally retains the canonical form — only the
          outer ``CrossModelMeasure`` alias is the user-facing handle.
        """
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        assert enriched.cross_model_measures, "no CrossModelMeasure was created"
        cm = enriched.cross_model_measures[0]
        # Outer alias: hop path preserved, leaf swapped to user name.
        assert cm.alias == "orders.customers.cust_rev", (
            f"cross-model alias must keep hop path and swap leaf to user "
            f"name; got {cm.alias!r}"
        )
        # cm.name is the bare user identifier (used as the downstream
        # short form via the special-case in _query_as_model).
        assert cm.name == "cust_rev", (
            f"cross-model name must be the bare user identifier; got "
            f"{cm.name!r}"
        )
        # user_declared is preserved through the rename.
        assert cm.user_declared is True, (
            f"cm.user_declared must remain True after rename; got "
            f"{cm.user_declared!r}"
        )
        # Inner EnrichedMeasure stays canonical — the CTE aggregate
        # expression / format-inference / column-introspection paths
        # depend on the canonical name. Only the OUTER alias is renamed.
        assert cm.measure.name == "revenue_sum", (
            f"inner EnrichedMeasure.name must stay canonical; got "
            f"{cm.measure.name!r}"
        )
        assert cm.measure.alias == "customers.revenue_sum", (
            f"inner EnrichedMeasure.alias must stay canonical; got "
            f"{cm.measure.alias!r}"
        )

    async def test_cross_model_rename_renders_in_sql(
        self, orders_customers_engine,
    ) -> None:
        """The rendered SQL must alias the cross-model aggregate as
        ``orders.customers.cust_rev`` (hop-preserved leaf swap), and
        must NOT contain the canonical ``revenue_sum`` leaf anywhere."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        sql = SQLGenerator(dialect="postgres").generate(enriched=enriched)
        assert '"orders.customers.cust_rev"' in sql, (
            f"renamed alias (hop-preserved) must appear in projected SQL:\n{sql}"
        )
        # The canonical leaf ``revenue_sum`` must not leak as a top-level
        # alias when renamed (it may still appear inside the inner CTE
        # where cm.measure stays canonical, but never as a public
        # projection alias of the form ``orders.customers.revenue_sum``).
        assert '"orders.customers.revenue_sum"' not in sql, (
            f"canonical cross-model public alias must not leak when "
            f"measure is renamed:\n{sql}"
        )

    async def test_cross_model_rename_propagates_to_dry_run_columns(
        self, orders_customers_engine,
    ) -> None:
        """The dry-run ``SlayerResponse.columns`` (driven by
        ``cm.alias``) must contain the hop-preserved renamed key, not
        the canonical leaf form."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert "orders.customers.cust_rev" in resp.columns, (
            f"renamed alias (hop-preserved) must appear in dry-run "
            f"columns; got {resp.columns!r}"
        )
        assert "orders.customers.revenue_sum" not in resp.columns


# ---------------------------------------------------------------------------
# Group B — Nested-DAG: ticket repro. Downstream stage references the
# renamed measure by the user-supplied name.
# ---------------------------------------------------------------------------


class TestCrossModelRenameNestedDAG:
    async def test_cross_model_rename_propagates_to_downstream_stage(
        self, orders_customers_engine,
    ) -> None:
        """Ticket repro: stage 1 declares a cross-model measure with
        ``name="cust_rev"``. Stage 2 references it as ``cust_rev:max``.
        Today this fails with ``Column 'cust_rev' not found in model
        'stage1'``. After the fix, stage 2 must succeed and the rendered
        SQL must expose ``cust_rev`` in the inner-stage projection."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        stage2 = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="cust_rev:max", name="top_cust_rev")],
        )
        resp = await engine.execute(query=[stage1, stage2], dry_run=True)
        sql = resp.sql or ""
        # Outer stage must produce a top_cust_rev column.
        assert "stage1.top_cust_rev" in resp.columns, (
            f"outer stage must project stage1.top_cust_rev; got columns "
            f"{resp.columns!r}\nSQL:\n{sql}"
        )
        # The inner stage must expose ``cust_rev`` (the user-renamed alias)
        # so the outer stage's ``cust_rev:max`` reference resolves.
        assert "cust_rev" in sql, (
            f"inner stage must expose cust_rev for outer stage:\n{sql}"
        )

    async def test_cross_model_rename_downstream_short_form_is_bare_user_name(
        self, orders_customers_engine,
    ) -> None:
        """The downstream-stage virtual model column for a renamed cross-
        model measure is the BARE user name (``cust_rev``), NOT the
        ``__``-flattened path (``customers__cust_rev``). The top-level
        result key preserves the hop path (``orders.customers.cust_rev``);
        the downstream short does not — by design, so a stage-2 caller
        can write ``cust_rev:max`` directly without having to learn how
        the hop path encodes to a single identifier."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        stage2 = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name="status")],
            # Bare reference — only works if downstream column is the
            # bare user name, not the ``__``-flattened encoding.
            measures=[ModelMeasure(formula="cust_rev:max", name="top_cust_rev")],
        )
        resp = await engine.execute(query=[stage1, stage2], dry_run=True)
        assert resp.sql, "dry_run must produce SQL"
        # Stage 2's bare reference resolved — outer projection has the
        # outer-stage measure.
        assert "stage1.top_cust_rev" in resp.columns, (
            f"stage 2 bare reference to user name must resolve; got "
            f"{resp.columns!r}"
        )

    async def test_cross_model_rename_downstream_stage_does_not_see_canonical(
        self, orders_customers_engine,
    ) -> None:
        """Stage 2 referencing the OLD canonical short-form
        ``customers__revenue_sum`` must NOT resolve once the rename is
        honored — the canonical column is no longer in the virtual model's
        column set."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        # Stage 2 attempts to reference the auto-derived flattened name.
        stage2 = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers__revenue_sum:max")],
        )
        with pytest.raises(Exception):  # noqa: BLE001 — the engine raises a domain error
            await engine.execute(query=[stage1, stage2], dry_run=True)


# ---------------------------------------------------------------------------
# Group C — `*:count` and `:count_distinct` cross-model variants.
# ---------------------------------------------------------------------------


class TestCrossModelStarAndCountDistinctRename:
    async def test_cross_model_star_count_rename(
        self, orders_customers_engine,
    ) -> None:
        """``customers.*:count`` with ``name="cust_n"`` must rename the
        cross-model measure with hop path preserved: ``orders.customers.cust_n``."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.*:count", name="cust_n")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        assert enriched.cross_model_measures, "no CrossModelMeasure was created"
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.customers.cust_n", (
            f"cross-model *:count alias must keep hop path; got {cm.alias!r}"
        )

    async def test_cross_model_count_distinct_rename(
        self, orders_customers_engine,
    ) -> None:
        """``customers.id:count_distinct`` with ``name="cust_distinct"`` must
        rename with hop path preserved."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.id:count_distinct", name="cust_distinct"),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.customers.cust_distinct", (
            f"cross-model count_distinct alias must keep hop path; got "
            f"{cm.alias!r}"
        )


# ---------------------------------------------------------------------------
# Group D — Collision guards. Lifted canonical-collision guard must run for
# both local and cross-model renames symmetrically.
# ---------------------------------------------------------------------------


class TestCrossModelRenameCollisionGuards:
    async def test_cross_model_rename_collides_with_local_canonical_raises(
        self, orders_customers_engine,
    ) -> None:
        """A cross-model rename whose ``name`` equals another sibling
        measure's canonical alias must be rejected at enrichment.

        Setup: local ``revenue:sum`` (canonical ``revenue_sum``) + cross-
        model ``customers.revenue:sum`` renamed to ``revenue_sum``. After
        the fix, BOTH would land on ``orders.revenue_sum`` — silently
        merging two distinct aggregates."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                # Cross-model rename target collides with local sibling's canonical.
                ModelMeasure(formula="customers.revenue:sum", name="revenue_sum"),
                ModelMeasure(formula="revenue:sum"),  # NOSONAR(S125) — explanatory note: canonical alias is "revenue_sum" (not commented-out code)
            ],
        )
        with pytest.raises(ValueError, match=r"collides with .*measure|silently merged"):
            await engine._enrich(query=query, model=orders)

    async def test_two_local_renames_mutually_colliding_canonicals_raises(
        self, orders_customers_engine,
    ) -> None:
        """Symmetric collision between two LOCAL renames: A's name equals
        B's canonical AND B's name equals A's canonical. The lifted pre-
        pass must catch the collision regardless of declaration order.

        Pins that the lift-to-pre-pass keeps the existing local-rename
        guard semantics. (The cross-model variant of this symmetric
        scenario is structurally impossible — cross-model canonicals
        always contain dots, while ``ModelMeasure.name`` rejects dots.)
        """
        engine, orders = orders_customers_engine
        # canonical("revenue:sum") = "revenue_sum"
        # canonical("revenue:avg") = "revenue_avg"
        # A's name == B's canonical AND B's name == A's canonical.
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="revenue_avg"),
                ModelMeasure(formula="revenue:avg", name="revenue_sum"),
            ],
        )
        with pytest.raises(ValueError, match=r"collides with .*measure|silently merged"):
            await engine._enrich(query=query, model=orders)

    async def test_cross_model_rename_collides_with_outer_source_column_raises(
        self, tmp_path,
    ) -> None:
        """User-supplied name on a cross-model measure collides with a
        source column on the outer model. The existing pre-pass at lines
        871-878 should catch this regardless of measure kind."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(_customers_model())
        # Add a literal "cust_rev" column on orders to trigger the collision.
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="cust_rev", sql="cust_rev", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        with pytest.raises(ValueError, match=r"collides with a source column"):
            await engine._enrich(query=query, model=orders)

    async def test_cross_model_duplicate_explicit_name_raises(
        self, orders_customers_engine,
    ) -> None:
        """Two cross-model measures sharing the same explicit ``name``
        must be rejected by the same-explicit-name pre-pass (which already
        covers all qfield kinds)."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="metric"),
                ModelMeasure(formula="customers.id:count_distinct", name="metric"),
            ],
        )
        with pytest.raises(ValueError, match=r"both declare name"):
            await engine._enrich(query=query, model=orders)

    async def test_two_local_renames_distinct_canonicals_pass(
        self, orders_customers_engine,
    ) -> None:
        """Local-only regression for the lifted canonical-collision guard:
        two local renames with non-colliding names AND non-colliding
        canonicals must continue to enrich successfully. Pins that the
        lift-to-pre-pass refactor does not introduce a false-positive
        rejection for the local case."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="rev"),
                ModelMeasure(formula="revenue:avg", name="rev_avg"),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        aliases = {m.alias for m in enriched.measures}
        assert "orders.rev" in aliases
        assert "orders.rev_avg" in aliases


# ---------------------------------------------------------------------------
# Group E — Regression guards. The no-rename path and the local-rename path
# must both remain unchanged.
# ---------------------------------------------------------------------------


class TestRenameRegressionGuards:
    async def test_cross_model_no_rename_unchanged(
        self, orders_customers_engine,
    ) -> None:
        """No ``name`` supplied on a cross-model measure: alias stays in
        the canonical ``<query_model>.<hop_path>.<col>_<agg>`` form."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.customers.revenue_sum", (
            f"cross-model alias without rename must stay canonical; got {cm.alias!r}"
        )

    async def test_local_rename_unchanged(
        self, orders_customers_engine,
    ) -> None:
        """Local-measure rename behavior is unchanged by this fix."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="rev")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        m = next(m for m in enriched.measures if m.alias == "orders.rev")
        assert m.name == "rev"

    async def test_cross_model_canonical_unreachable_via_user_name(self) -> None:  # NOSONAR(S7503) — sibling tests in this class do await
        """Documents an invariant: cross-model canonicals always contain
        dots (``customers.revenue_sum``, ``customers.regions.population_sum``)
        but ``ModelMeasure.name`` rejects dots. So a user-supplied ``name``
        can never equal a cross-model canonical — meaning the rename
        branch ALWAYS fires for cross-model when ``name`` is supplied
        (the ``qfield.name != canonical_name`` check is structurally
        true). This test pins that invariant so it surfaces in the
        suite when ``ModelMeasure.name`` validation loosens."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"only letters, digits, and underscores"):
            ModelMeasure(formula="customers.revenue:sum", name="customers.revenue_sum")


# ---------------------------------------------------------------------------
# Group F — label/type propagation through the rename.
# ---------------------------------------------------------------------------


class TestCrossModelRenameLabelAndType:
    async def test_cross_model_rename_label_propagates(
        self, orders_customers_engine,
    ) -> None:
        """``label`` on the qfield must end up on the renamed
        CrossModelMeasure."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(
                    formula="customers.revenue:sum",
                    name="cust_rev",
                    label="Customer revenue",
                ),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.customers.cust_rev"
        assert cm.label == "Customer revenue", (
            f"label must propagate to renamed CrossModelMeasure; got {cm.label!r}"
        )

    async def test_cross_model_rename_type_propagates_to_measure(
        self, orders_customers_engine,
    ) -> None:
        """``type=INT`` on the renamed cross-model measure must propagate
        to ``cm.measure.type`` at enrichment time — the rename must NOT
        drop the declared type.

        Pre-existing gap (out of scope for DEV-1448): the SQL-level CAST
        does not materialise because ``_build_rerooted_enriched`` re-enriches
        a fresh ``ModelMeasure(formula=...)`` without threading the outer
        qfield's ``type=`` into the rerooted measure. This is a DEV-1361
        follow-up for cross-model; here we pin only the enrichment-level
        contract that the type LANDS on ``cm.measure.type``.
        """
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(
                    formula="customers.revenue:sum",
                    name="cust_rev",
                    type=DataType.INT,
                ),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        cm = enriched.cross_model_measures[0]
        # Enrichment-level pin: the declared type lands on the inner
        # EnrichedMeasure even with the rename applied to the outer
        # ``cm.alias``/``cm.name``.
        assert cm.measure.type == DataType.INT, (
            f"declared type=INT must propagate to cm.measure.type after "
            f"rename; got {cm.measure.type!r}"
        )


# ---------------------------------------------------------------------------
# Group G — Filter / ORDER BY interaction with the rename.
# ---------------------------------------------------------------------------


class TestCrossModelRenameFilters:
    async def test_filter_via_user_alias_raises_until_dev_1445(
        self, orders_customers_engine,
    ) -> None:
        """DEV-1445 boundary (revised after Codex review on PR #136):
        same-stage filter ``"cust_rev > 100"`` referencing a renamed
        cross-model measure currently raises ``ValueError`` at strict
        resolution. The SQL generator has no path to route the bare user
        alias to the cross-model CTE's output column, so admitting the
        filter would emit broken SQL (``WHERE orders.cust_rev > 100``
        against a column that doesn't exist on the base table). Until
        DEV-1445 lands the full cross-model filter remap, the supported
        workaround is to restructure as a multi-stage ``source_queries``
        so the cross-model measure becomes a local measure in the
        downstream stage. This test pins the clean-error boundary so we
        notice if/when the behaviour changes.
        """
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            filters=["cust_rev > 100"],
        )
        with pytest.raises(ValueError, match=r"unknown name 'cust_rev'"):
            await engine._enrich(query=query, model=orders)


class TestDeferredCrossModelFilterScope:
    """Cross-model colon-form filter remap is DEV-1445 territory. Pin the
    boundary so the test flips when DEV-1445 lands."""

    @pytest.mark.skip(
        reason=(
            "DEV-1445: cross-model colon-form filter + rename is deferred scope. "
            "Today filter ``customers.revenue:sum > 100`` paired with a rename "
            "does not auto-resolve to the user alias. DEV-1448 fixes only the "
            "projection alias; flip into a real coverage test when DEV-1445 "
            "ships."
        )
    )
    async def test_cross_model_filter_colon_form_with_rename_deferred(
        self, orders_customers_engine,
    ) -> None:
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            filters=["customers.revenue:sum > 100"],
        )
        enriched = await engine._enrich(query=query, model=orders)
        sql = SQLGenerator(dialect="postgres").generate(enriched=enriched)
        # When DEV-1445 lands, assert the filter remaps to the user alias
        # and the SQL has HAVING on the cross-model CTE's output column.
        assert "cust_rev" in sql, sql


# ---------------------------------------------------------------------------
# Group H — Multi-hop and same-query rename/no-rename mix (CTE uniqueness).
# ---------------------------------------------------------------------------


class TestCrossModelRenameMultiHopAndCTEUniqueness:
    async def test_cross_model_rename_multi_hop(
        self, orders_customers_regions_engine,
    ) -> None:
        """Two-hop cross-model measure ``customers.regions.population:sum``
        with ``name="region_pop"`` must rename with the full hop path
        preserved and only the canonical leaf swapped to the user name:
        ``orders.customers.regions.region_pop`` — the same dot-syntax
        shape every other multi-hop caller-facing key uses."""
        engine, orders = orders_customers_regions_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(
                    formula="customers.regions.population:sum",
                    name="region_pop",
                ),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        assert enriched.cross_model_measures, "no CrossModelMeasure was created"
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.customers.regions.region_pop", (
            f"multi-hop cross-model rename must keep the full hop path "
            f"and swap only the leaf to the user name; got {cm.alias!r}"
        )
        # cm.name is still the bare user identifier — that's what the
        # downstream short form will be (no __-flattening of the hops).
        assert cm.name == "region_pop"

    async def test_renamed_and_unrenamed_cross_model_no_collision(
        self, orders_customers_engine,
    ) -> None:
        """One renamed + one unrenamed cross-model measure in the same
        query must produce distinct CTEs and distinct projection aliases.
        Both share the same hop path (``orders.customers.<leaf>``); only
        the leaf differs."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="cust_rev"),
                ModelMeasure(formula="customers.id:count_distinct"),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        aliases = {cm.alias for cm in enriched.cross_model_measures}
        # Renamed: hop-preserved leaf swap.
        assert "orders.customers.cust_rev" in aliases
        # Unrenamed: canonical leaf.
        assert "orders.customers.id_count_distinct" in aliases
        # Distinct.
        assert len(aliases) == 2, (
            f"renamed + unrenamed cross-model must produce 2 distinct aliases; "
            f"got {aliases!r}"
        )


# ---------------------------------------------------------------------------
# Group I — Transform-wrapped cross-model with `name`. DEV-1448 does NOT
# promise to fix this case; pin the current behavior so we know when it
# changes.
# ---------------------------------------------------------------------------


class TestTransformWrappedCrossModelDeferred:
    @pytest.mark.skip(
        reason=(
            "DEV-1448 does not fix transform-wrapped cross-model agg refs. "
            "``cumsum(customers.revenue:sum)`` with a top-level ``name`` "
            "lands in the transform branch (_flatten_spec), not the cross-"
            "model branch — different code path. The inner CrossModelMeasure "
            "stays unrenamed (canonical form). Flip into a coverage test if/"
            "when this case is fixed."
        )
    )
    async def test_transform_wrapped_cross_model_with_name_pinned(
        self, orders_customers_engine,
    ) -> None:
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[],
            measures=[
                ModelMeasure(
                    formula="cumsum(customers.revenue:sum)",
                    name="cum_cust_rev",
                ),
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        # When fixed: assert the inner CrossModelMeasure is also renamed
        # (or the transform output references the user alias consistently).
        # Today: the inner cm stays canonical, transform output gets the name.
        cm = enriched.cross_model_measures[0]
        assert cm.alias == "orders.cum_cust_rev", (
            f"transform-wrapped cross-model rename not implemented; got {cm.alias!r}"
        )
