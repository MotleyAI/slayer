"""User-supplied ``name`` on a cross-model (join-traversed) measure must reach
the rendered SQL projection and downstream nested-DAG stages. A renamed
measure's public key is ``orders.<name>`` (source-model prefix); the cross-model
CTE keeps the canonical ``<hop>.<col>_<agg>`` column.
"""
from __future__ import annotations

import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _norm, _outer_select

def _public_projection_aliases(sql: str) -> list[str]:
    """Result keys the outermost SELECT projects — the public projection."""
    return [proj.alias_or_name for proj in _outer_select(sql).expressions]


def _cte_names(sql: str) -> list[str]:
    """CTE names in ``sql``; ``_cm_*`` ones are the per-cross-model producers."""
    parsed = sqlglot.parse_one(sql, dialect="postgres")
    return [cte.alias_or_name for cte in parsed.find_all(exp.CTE)]


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


# Group A — single-stage rename.


class TestCrossModelRenameSingleStage:
    async def test_cross_model_rename_top_level_result_key(
        self, orders_customers_engine,
    ) -> None:
        """User name reaches the outer projection; the CTE column stays canonical."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.cust_rev"], (
            f"cross-model rename must surface as the public projection key; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        assert "orders.customers.revenue_sum" not in aliases, (
            f"canonical cross-model key must not stay public after rename; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"'
            in sql
        ), (
            f"inner cross-model CTE column must stay canonical after the "
            f"rename:\n{sql}"
        )

    async def test_cross_model_rename_renders_in_sql(
        self, orders_customers_engine,
    ) -> None:
        """Renamed aggregate aliases under the user name; canonical leaf never
        surfaces as a public alias (it legitimately lives inside the CTE)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert '"orders.cust_rev"' in sql, (
            f"renamed alias must appear in projected SQL:\n{sql}"
        )
        aliases = _public_projection_aliases(sql)
        assert "orders.customers.revenue_sum" not in aliases, (
            f"canonical cross-model public alias must not leak when "
            f"measure is renamed; public projection was {aliases!r}\n{sql}"
        )

    async def test_cross_model_rename_propagates_to_dry_run_columns(
        self, orders_customers_engine,
    ) -> None:
        """Dry-run ``columns`` carries the renamed key ``orders.cust_rev`` (no hop)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert "orders.cust_rev" in resp.columns, (
            f"renamed alias must appear in dry-run columns; got "
            f"{resp.columns!r}"
        )
        assert "orders.customers.revenue_sum" not in resp.columns
        assert "orders.customers.cust_rev" not in resp.columns


# Group B — nested-DAG: downstream stage references the renamed measure.


class TestCrossModelRenameNestedDAG:
    async def test_cross_model_rename_propagates_to_downstream_stage(
        self, orders_customers_engine,
    ) -> None:
        """Stage 2's ``cust_rev:max`` resolves against stage 1's renamed measure."""
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
        assert "stage1.top_cust_rev" in resp.columns, (
            f"outer stage must project stage1.top_cust_rev; got columns "
            f"{resp.columns!r}\nSQL:\n{sql}"
        )
        assert "cust_rev" in sql, (
            f"inner stage must expose cust_rev for outer stage:\n{sql}"
        )

    async def test_cross_model_rename_downstream_short_form_is_bare_user_name(
        self, orders_customers_engine,
    ) -> None:
        """Downstream-stage column for a renamed cross-model measure is the bare
        user name, so stage 2 can write ``cust_rev:max`` directly."""
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
        assert resp.sql, "dry_run must produce SQL"
        assert "stage1.top_cust_rev" in resp.columns, (
            f"stage 2 bare reference to user name must resolve; got "
            f"{resp.columns!r}"
        )

    async def test_hidden_cross_model_measure_kept_user_declared_false(
        self, orders_customers_engine,
    ) -> None:
        """A cross-model aggregate hoisted out of an arithmetic formula stays
        hidden: only the division is public, the aggregate lives in the CTE."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum / 100")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.customers.revenue_sum / 100"], (
            f"only the user-declared arithmetic measure may be public; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert "orders.customers.revenue_sum" not in aliases, (
            f"hoisted (hidden) cross-model aggregate must not surface as a "
            f"public column; got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"'
            in sql
        ), f"expected the hoisted cross-model aggregate inside a _cm_ CTE:\n{sql}"
        assert any(name.startswith("_cm_") for name in _cte_names(sql)), (
            f"expected a per-cross-model CTE for the hoisted aggregate:\n{sql}"
        )

    async def test_cross_model_rename_downstream_stage_does_not_see_canonical(
        self, orders_customers_engine,
    ) -> None:
        """Once renamed, the old canonical short ``customers__revenue_sum`` is
        gone from the virtual model, so a stage-2 reference to it must not resolve."""
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
            measures=[ModelMeasure(formula="customers__revenue_sum:max")],
        )
        with pytest.raises(Exception):  # noqa: BLE001 — the engine raises a domain error
            await engine.execute(query=[stage1, stage2], dry_run=True)


# Group C — `*:count` and `:count_distinct` cross-model variants.


class TestCrossModelStarAndCountDistinctRename:
    async def test_cross_model_star_count_rename(
        self, orders_customers_engine,
    ) -> None:
        """``customers.*:count`` renamed ``cust_n`` projects COUNT(*) under it."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.*:count", name="cust_n")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.cust_n"], (
            f"cross-model *:count rename must surface as the public key; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'COUNT(*) AS "customers._count"' in sql, (
            f"cross-model *:count CTE column must stay canonical:\n{sql}"
        )

    async def test_cross_model_count_distinct_rename(
        self, orders_customers_engine,
    ) -> None:
        """``customers.id:count_distinct`` renamed ``cust_distinct`` projects
        COUNT(DISTINCT …) under it."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.id:count_distinct", name="cust_distinct"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.cust_distinct"], (
            f"cross-model count_distinct rename must surface as the public "
            f"key; got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'COUNT(DISTINCT customers.id) AS "customers.id_count_distinct"'
            in sql
        ), f"cross-model count_distinct CTE column must stay canonical:\n{sql}"


# Group C2 — explicit ``type=`` reaches the producer CTE; inferred types don't cast.


class TestCrossModelDeclaredTypeCast:
    async def test_declared_type_casts_producer_column(
        self, orders_customers_engine,
    ) -> None:
        """Explicit INT on a DOUBLE-column sum casts to the declared type."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(
                formula="customers.revenue:sum", name="cust_rev",
                type=DataType.INT,
            )],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS INTEGER)'
            ' AS "customers.revenue_sum"' in sql
        ), f"declared type must win over the source column's:\n{sql}"

    async def test_declared_int_count_still_casts(
        self, orders_customers_engine,
    ) -> None:
        """Explicit INT still casts, unlike inferred INT (COUNT range preserved)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(
                formula="customers.*:count", name="cust_n", type=DataType.INT,
            )],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert 'CAST(COUNT(*) AS INTEGER) AS "customers._count"' in sql, (
            f"explicitly declared INT must still cast:\n{sql}"
        )


# Group D — collision guards (local and cross-model renames, symmetric).


class TestCrossModelRenameCollisionGuards:
    async def test_cross_model_rename_collides_with_local_canonical_raises(
        self, orders_customers_engine,
    ) -> None:
        """Rename target equal to a sibling's canonical alias is rejected, not
        silently merged onto ``orders.revenue_sum``."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="revenue_sum"),
                ModelMeasure(formula="revenue:sum"),  # NOSONAR(S125) — explanatory note: canonical alias is "revenue_sum" (not commented-out code)
            ],
        )
        with pytest.raises(ValueError, match=r"declared more than once"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_rename_leaf_vs_sibling_canonical_leaf_stay_distinct(
        self, orders_customers_engine,
    ) -> None:
        """A measure renamed to a sibling's canonical leaf (``id_count_distinct``)
        stays a distinct public column: the renamed key is source-model-prefixed,
        the unrenamed one hop-qualified, so they can't collide."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="id_count_distinct"),
                ModelMeasure(formula="customers.id:count_distinct"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "orders.id_count_distinct",
            "orders.customers.id_count_distinct",
        ], (
            f"renamed and unrenamed cross-model measures must not merge into "
            f"one public column; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        cm_ctes = [name for name in _cte_names(sql) if name.startswith("_cm_")]
        assert len(set(cm_ctes)) == 2, (
            f"expected 2 distinct cross-model CTEs, got {cm_ctes!r}\nSQL:\n{sql}"
        )
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"' in sql
        assert (
            'COUNT(DISTINCT customers.id) AS "customers.id_count_distinct"'
            in sql
        )

    async def test_two_local_renames_mutually_colliding_canonicals_stay_distinct(
        self, orders_customers_engine,
    ) -> None:
        """Symmetric swap of two local renames (A's name == B's canonical and
        vice versa) keeps both aggregates distinct with their own bodies."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="revenue_avg"),
                ModelMeasure(formula="revenue:avg", name="revenue_sum"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.revenue_avg", "orders.revenue_sum"], (
            f"the symmetric rename swap must keep both measures distinct; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        # The swap didn't swap bodies: ``revenue_avg`` is still the SUM.
        assert 'SUM(orders.amount) AS REAL) AS "orders.revenue_avg"' in sql, (
            f"'revenue_avg' must carry the declared revenue:sum body:\n{sql}"
        )
        assert 'AVG(orders.amount) AS REAL) AS "orders.revenue_sum"' in sql, (
            f"'revenue_sum' must carry the declared revenue:avg body:\n{sql}"
        )

    async def test_cross_model_rename_collides_with_outer_source_column_raises(
        self, tmp_path,
    ) -> None:
        """A rename colliding with a source column on the outer model is rejected."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(_customers_model())
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
        with pytest.raises(ValueError, match=r"matches a source column"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_duplicate_explicit_name_raises(
        self, orders_customers_engine,
    ) -> None:
        """Two cross-model measures with the same explicit ``name`` are rejected."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="metric"),
                ModelMeasure(formula="customers.id:count_distinct", name="metric"),
            ],
        )
        with pytest.raises(ValueError, match=r"'metric' is declared more than once"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_star_count_vs_renamed_sibling_stay_distinct(
        self, orders_customers_engine,
    ) -> None:
        """A ``*:count`` (canonical leaf ``_count``) and a sibling renamed to
        ``_count`` stay distinct: ``orders._count`` vs ``orders.customers._count``."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.*:count"),
                ModelMeasure(formula="customers.revenue:sum", name="_count"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "orders.customers._count",
            "orders._count",
        ], (
            f"the *:count canonical leaf and the renamed sibling must stay "
            f"distinct public columns; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'COUNT(*) AS "customers._count"' in sql, sql
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"' in sql, sql

    async def test_cross_model_rename_collides_with_dimension_downstream_short_raises(
        self, orders_customers_engine,
    ) -> None:
        """A measure renamed to a dimension's ``__``-flattened downstream short
        (``customers__region_id``) is rejected, even though public aliases differ."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[
                ModelMeasure(
                    formula="customers.revenue:sum",
                    name="customers__region_id",
                ),
            ],
        )
        with pytest.raises(
            ValueError, match=r"'customers__region_id' is declared more than once",
        ):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_rename_vs_dimension_stay_distinct(
        self, orders_customers_engine,
    ) -> None:
        """A measure renamed ``region_id`` and a ``customers.region_id`` dimension
        occupy distinct public keys (``orders.region_id`` vs the hop-qualified one)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="region_id")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.customers.region_id", "orders.region_id"], (
            f"the dimension key and the renamed measure key must stay "
            f"distinct; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"' in sql, sql

    async def test_cross_model_rename_vs_arithmetic_mangled_name_stay_distinct(
        self, orders_customers_engine,
    ) -> None:
        """A measure renamed ``revenue_sum__div__100`` and an arithmetic
        ``revenue:sum / 100`` stay distinct; the typed pipeline keeps the readable
        ``orders.revenue_sum / 100`` key rather than the legacy mangled short."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="revenue_sum__div__100"),
                ModelMeasure(formula="revenue:sum / 100"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "orders.revenue_sum__div__100",
            "orders.revenue_sum / 100",
        ], (
            f"the arithmetic measure and the renamed cross-model measure must "
            f"stay distinct public keys; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'SUM(orders.amount) AS REAL) / 100 AS "orders.revenue_sum / 100"' in sql, sql
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "customers.revenue_sum"' in sql, sql

    async def test_two_local_renames_distinct_canonicals_pass(
        self, orders_customers_engine,
    ) -> None:
        """Two local renames with non-colliding names and canonicals resolve
        (no false-positive rejection)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="rev"),
                ModelMeasure(formula="revenue:avg", name="rev_avg"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        aliases = set(_public_projection_aliases(resp.sql or ""))
        assert "orders.rev" in aliases, aliases
        assert "orders.rev_avg" in aliases, aliases


# Group E — regression guards: no-rename and local-rename paths unchanged.


class TestRenameRegressionGuards:
    async def test_cross_model_no_rename_unchanged(
        self, orders_customers_engine,
    ) -> None:
        """Without a name, the public key stays canonical ``<model>.<hop>.<col>_<agg>``."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.customers.revenue_sum"], (
            f"cross-model key without rename must stay canonical; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )

    async def test_local_rename_unchanged(
        self, orders_customers_engine,
    ) -> None:
        """Local rename: the user name becomes the public key over the aggregate."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == ["orders.status", "orders.rev"], sql
        assert 'SUM(orders.amount) AS REAL) AS "orders.rev"' in sql, sql

    async def test_cross_model_canonical_unreachable_via_user_name(self) -> None:  # NOSONAR(S7503) — sibling tests in this class do await
        """Invariant: cross-model canonicals contain dots but ``ModelMeasure.name``
        rejects them, so a name can never equal a cross-model canonical."""
        with pytest.raises(ValidationError, match=r"only letters, digits, and underscores"):
            ModelMeasure(formula="customers.revenue:sum", name="customers.revenue_sum")


# Group F — label/type propagation through the rename.


class TestCrossModelRenameLabelAndType:
    async def test_cross_model_rename_label_propagates(
        self, orders_customers_engine,
    ) -> None:
        """``label`` survives the rename, published under the renamed result key."""
        engine, _ = orders_customers_engine
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
        resp = await engine.execute(query=query, dry_run=True)
        assert _public_projection_aliases(resp.sql or "") == [
            "orders.status", "orders.cust_rev",
        ], resp.sql
        meta = resp.attributes.get("orders.cust_rev")
        assert meta is not None, (
            f"no response metadata under the renamed key; got "
            f"{resp.attributes!r}"
        )
        assert meta.label == "Customer revenue", (
            f"label must propagate to the renamed cross-model measure; got "
            f"{meta.label!r}"
        )

    async def test_cross_model_rename_type_propagates_to_measure(
        self, orders_customers_engine,
    ) -> None:
        """``type=INT`` survives the rename as a CAST in the cross-model CTE."""
        engine, _ = orders_customers_engine
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
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == [
            "orders.status", "orders.cust_rev",
        ], sql
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS INTEGER) '
            'AS "customers.revenue_sum"' in sql
        ), (
            f"declared type=INT must survive the rename as a CAST on the "
            f"cross-model aggregate:\n{sql}"
        )


# Group G — filter / ORDER BY interaction with the rename.


class TestCrossModelRenameOrderBy:
    """ORDER BY via the bare user alias resolves to the cross-model CTE column."""

    async def test_order_by_user_alias_resolves_to_cross_model_cte_column(
        self, orders_customers_engine,
    ) -> None:
        """ORDER BY on the bare user alias sorts on the CTE column, ``desc`` kept."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            order=[OrderItem(column=ColumnRef(name="cust_rev"), direction="desc")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert "ORDER BY" in sql, sql
        order_clause = sql.split("ORDER BY", 1)[1]
        assert '"customers.revenue_sum"' in order_clause, (
            f"ORDER BY via bare user alias must resolve to the cross-model "
            f"CTE's output column:\n{sql}"
        )
        assert "DESC" in order_clause.upper(), (
            f"ORDER BY direction must survive the alias resolution:\n{sql}"
        )


class TestCrossModelRenameFilters:
    async def test_filter_via_user_alias_resolves_to_cross_model_value(
        self, orders_customers_engine,
    ) -> None:
        """Filter ``"cust_rev > 100"`` lands as an outer WHERE on the producer
        CTE's output column, not a base-table column."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            filters=["cust_rev > 100"],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == [
            "orders.status", "orders.cust_rev",
        ], sql
        assert (
            '_cm_orders__customers__revenue_sum."customers.revenue_sum" > 100'
            in _norm(sql)
        ), (
            f"the bare user alias must resolve to a WHERE on the cross-model "
            f"aggregate's value:\n{sql}"
        )
        assert "orders.cust_rev > 100" not in _norm(sql), (
            f"the filter must not be emitted against a non-existent base-table "
            f"column:\n{sql}"
        )


class TestDeferredCrossModelFilterScope:
    async def test_cross_model_filter_colon_form_with_rename_deferred(
        self, orders_customers_engine,
    ) -> None:
        """Colon-form filter ``customers.revenue:sum > 100`` with a rename lands
        as an outer WHERE on the producer CTE column; measure still projects."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            filters=["customers.revenue:sum > 100"],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert "cust_rev" in sql, sql
        assert _public_projection_aliases(sql) == [
            "orders.status", "orders.cust_rev",
        ], sql
        assert (
            '_cm_orders__customers__revenue_sum."customers.revenue_sum" > 100'
            in _norm(sql)
        ), (
            f"colon-form cross-model filter must land as a WHERE on the "
            f"cross-model aggregate's value:\n{sql}"
        )


# Group H — multi-hop and same-query rename/no-rename mix (CTE uniqueness).


class TestCrossModelRenameMultiHopAndCTEUniqueness:
    async def test_cross_model_rename_multi_hop(
        self, orders_customers_regions_engine,
    ) -> None:
        """A two-hop measure renamed ``region_pop`` projects the bare user name;
        the CTE keeps the fully hop-qualified canonical column."""
        engine, _ = orders_customers_regions_engine
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
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "orders.region_pop"], (
            f"multi-hop cross-model rename must project the bare user name "
            f"(no __-flattened hops); got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(regions.population) AS REAL) AS "regions.population_sum"'
            in sql
        ), f"multi-hop CTE column stays canonical (target-rooted):\n{sql}"

    async def test_renamed_and_unrenamed_cross_model_no_collision(
        self, orders_customers_engine,
    ) -> None:
        """One renamed + one unrenamed cross-model measure produce distinct CTEs
        and distinct projection aliases."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="cust_rev"),
                ModelMeasure(formula="customers.id:count_distinct"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert "orders.cust_rev" in aliases, (f"{aliases!r}\n{sql}")
        assert "orders.customers.id_count_distinct" in aliases, (f"{aliases!r}\n{sql}")
        assert len(set(aliases)) == len(aliases) == 3, (
            f"renamed + unrenamed cross-model must produce distinct aliases; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        cm_ctes = [name for name in _cte_names(sql) if name.startswith("_cm_")]
        assert len(set(cm_ctes)) == 2, (
            f"renamed + unrenamed cross-model must produce 2 distinct CTEs; "
            f"got {cm_ctes!r}\nSQL:\n{sql}"
        )


# Group I — transform-wrapped cross-model with `name`; pins current (unsupported).


class TestTransformWrappedCrossModelDeferred:
    @pytest.mark.skip(
        reason=(
            "Transform-wrapped cross-model agg refs are still unsupported. "
            "``cumsum(customers.revenue:sum)`` with a top-level ``name`` goes "
            "down the transform path, not the cross-model path; on the typed "
            "pipeline it raises `Transform 'cumsum' requires an unambiguous "
            "time dimension` for this dimension-only query rather than "
            "renaming the hoisted cross-model aggregate. Flip into a coverage "
            "test if/when this case is fixed."
        )
    )
    async def test_transform_wrapped_cross_model_with_name_pinned(
        self, orders_customers_engine,
    ) -> None:
        engine, _ = orders_customers_engine
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
        resp = await engine.execute(query=query, dry_run=True)
        assert _public_projection_aliases(resp.sql or "") == [
            "orders.status", "orders.cum_cust_rev",
        ], (
            f"transform-wrapped cross-model rename not implemented; got "
            f"{resp.columns!r}"
        )
