"""User-supplied ``name`` on a cross-dataset (relationship-traversed) metric must reach
the rendered SQL projection and downstream nested-DAG stages. A renamed
metric's public key is ``orders.<name>`` (source-dataset prefix); the cross-dataset
CTE keeps the canonical ``<hop>.<col>_<agg>`` column.
"""
from __future__ import annotations

import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _norm, _outer_select

def _public_projection_aliases(sql: str) -> list[str]:
    """Result keys the outermost SELECT projects — the public projection."""
    return [proj.alias_or_name for proj in _outer_select(sql).expressions]


def _cte_names(sql: str) -> list[str]:
    """CTE names in ``sql``; ``_cm_*`` ones are the per-cross-dataset producers."""
    parsed = sqlglot.parse_one(sql, dialect="postgres")
    return [cte.alias_or_name for cte in parsed.find_all(exp.CTE)]


async def _save_test_datasource(storage: YAMLStorage) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )


def _customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        sql_table="customers",
        data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
            DatasetField(name="revenue", expression="lifetime_revenue", datatype=DataType.FLOAT),
        ],
    )


def _customers_dataset_with_region_relationship() -> SlayerDataset:
    """Customers dataset with a relationship to regions — used by the multi-hop test."""
    return SlayerDataset(
        name="customers",
        sql_table="customers",
        data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
            DatasetField(name="revenue", expression="lifetime_revenue", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions",
        sql_table="regions",
        data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="population", expression="population", datatype=DataType.FLOAT),
        ],
    )


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
            DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
            DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


@pytest.fixture
async def orders_customers_engine(tmp_path) -> tuple[SlayerQueryEngine, SlayerDataset]:
    """orders → customers (single hop)."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_test_datasource(storage)
    await storage.save_dataset(_customers_dataset())
    orders = _orders_dataset()
    await storage.save_dataset(orders)
    return SlayerQueryEngine(storage=storage), orders


@pytest.fixture
async def orders_customers_regions_engine(tmp_path) -> tuple[SlayerQueryEngine, SlayerDataset]:
    """orders → customers → regions (two hops, for multi-hop rename test)."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_test_datasource(storage)
    await storage.save_dataset(_regions_dataset())
    await storage.save_dataset(_customers_dataset_with_region_relationship())
    orders = _orders_dataset()
    await storage.save_dataset(orders)
    return SlayerQueryEngine(storage=storage), orders


# Group A — single-stage rename.


class TestCrossModelRenameSingleStage:
    async def test_cross_model_rename_top_level_result_key(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """User name reaches the outer projection; the CTE column stays canonical."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "cust_rev"], (
            f"cross-dataset rename must surface as the public projection key; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        assert "customers.revenue_sum" not in aliases, (
            f"canonical cross-dataset key must not stay public after rename; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"'
            in sql
        ), (
            f"inner cross-dataset CTE column must stay canonical after the "
            f"rename:\n{sql}"
        )

    async def test_cross_model_rename_renders_in_sql(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Renamed aggregate aliases under the user name; canonical leaf never
        surfaces as a public alias (it legitimately lives inside the CTE)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert '"cust_rev"' in sql, (
            f"renamed alias must appear in projected SQL:\n{sql}"
        )
        aliases = _public_projection_aliases(sql)
        assert "customers.revenue_sum" not in aliases, (
            f"canonical cross-dataset public alias must not leak when "
            f"metric is renamed; public projection was {aliases!r}\n{sql}"
        )

    async def test_cross_model_rename_propagates_to_dry_run_columns(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Dry-run ``columns`` carries the renamed key ``orders.cust_rev`` (no hop)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert "cust_rev" in resp.columns, (
            f"renamed alias must appear in dry-run columns; got "
            f"{resp.columns!r}"
        )
        assert "customers.revenue_sum" not in resp.columns
        assert "orders.customers.cust_rev" not in resp.columns


# Group B — nested-DAG: downstream stage references the renamed metric.


class TestCrossModelRenameNestedDAG:
    async def test_cross_model_rename_propagates_to_downstream_stage(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Stage 2's ``max(cust_rev)`` resolves against stage 1's renamed metric."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        stage2 = SlayerQuery(
            population="stage1",
            dimensions=[QueryDimension(field="stage1.status")],
            metrics=[QueryMetric(metric="max(stage1.cust_rev)", alias="top_cust_rev")],
        )
        resp = await engine.execute(query=[stage1, stage2], dry_run=True)
        sql = resp.sql or ""
        assert "top_cust_rev" in resp.columns, (
            f"outer stage must project stage1.top_cust_rev; got columns "
            f"{resp.columns!r}\nSQL:\n{sql}"
        )
        assert "cust_rev" in sql, (
            f"inner stage must expose cust_rev for outer stage:\n{sql}"
        )

    async def test_cross_model_rename_downstream_short_form_is_bare_user_name(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Downstream-stage column for a renamed cross-dataset metric is the bare
        user name, so stage 2 can write ``max(cust_rev)`` directly."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        stage2 = SlayerQuery(
            population="stage1",
            dimensions=[QueryDimension(field="stage1.status")],
            metrics=[QueryMetric(metric="max(stage1.cust_rev)", alias="top_cust_rev")],
        )
        resp = await engine.execute(query=[stage1, stage2], dry_run=True)
        assert resp.sql, "dry_run must produce SQL"
        assert "top_cust_rev" in resp.columns, (
            f"stage 2 bare reference to user name must resolve; got "
            f"{resp.columns!r}"
        )

    async def test_hidden_cross_model_measure_kept_user_declared_false(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A cross-dataset aggregate hoisted out of an arithmetic expression stays
        hidden: only the division is public, the aggregate lives in the CTE."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue) / 100")],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "customers.revenue_sum_100"], (
            f"only the user-declared arithmetic metric may be public; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert "customers.revenue_sum" not in aliases, (
            f"hoisted (hidden) cross-dataset aggregate must not surface as a "
            f"public column; got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"'
            in sql
        ), f"expected the hoisted cross-dataset aggregate inside a _cm_ CTE:\n{sql}"
        assert any(name.startswith("_cm_") for name in _cte_names(sql)), (
            f"expected a per-cross-dataset CTE for the hoisted aggregate:\n{sql}"
        )

    async def test_cross_model_rename_downstream_stage_does_not_see_canonical(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Once renamed, the old canonical short ``customers__revenue_sum`` is
        gone from the virtual dataset, so a stage-2 reference to it must not resolve."""
        engine, _ = orders_customers_engine
        stage1 = SlayerQuery(
            name="stage1",
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        stage2 = SlayerQuery(
            population="stage1",
            dimensions=[QueryDimension(field="stage1.status")],
            metrics=[QueryMetric(metric="max(stage1.customers__revenue_sum)")],
        )
        with pytest.raises(Exception):  # noqa: BLE001 — the engine raises a domain error
            await engine.execute(query=[stage1, stage2], dry_run=True)


# Group C — `count(*)` and `count_distinct(...)` cross-dataset variants.


class TestCrossModelStarAndCountDistinctRename:
    async def test_cross_model_star_count_rename(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """``count(customers.*)`` renamed ``cust_n`` projects COUNT(*) under it."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="count(customers.*)", alias="cust_n")],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "cust_n"], (
            f"cross-dataset count(*) rename must surface as the public key; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'COUNT(*) AS "_count"' in sql, (
            f"cross-dataset count(*) CTE column must stay canonical:\n{sql}"
        )

    async def test_cross_model_count_distinct_rename(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """``count_distinct(customers.id)`` renamed ``cust_distinct`` projects
        COUNT(DISTINCT …) under it."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="count_distinct(customers.id)", alias="cust_distinct"),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "cust_distinct"], (
            f"cross-dataset count_distinct rename must surface as the public "
            f"key; got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'COUNT(DISTINCT customers.id) AS "id_count_distinct"'
            in sql
        ), f"cross-dataset count_distinct CTE column must stay canonical:\n{sql}"


# Group C2 — explicit ``type=`` reaches the producer CTE; inferred types don't cast.


class TestCrossModelDeclaredTypeCast:
    async def test_declared_type_casts_producer_column(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Explicit INT on a DOUBLE-field sum casts to the declared type."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(
                metric="sum(customers.revenue)", alias="cust_rev",
                datatype=DataType.INTEGER,
            )],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS INTEGER)'
            ' AS "revenue_sum"' in sql
        ), f"declared type must win over the source field's:\n{sql}"

    async def test_declared_int_count_still_casts(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Explicit INT still casts, unlike inferred INT (COUNT range preserved)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(
                metric="count(customers.*)", alias="cust_n", datatype=DataType.INTEGER,
            )],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert 'CAST(COUNT(*) AS INTEGER) AS "_count"' in sql, (
            f"explicitly declared INT must still cast:\n{sql}"
        )


# Group D — collision guards (local and cross-dataset renames, symmetric).


class TestCrossModelRenameCollisionGuards:
    async def test_cross_model_rename_collides_with_local_canonical_raises(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A cross-model rename equal to a sibling's name is rejected, not silently merged."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(customers.revenue)", alias="revenue_sum"),
                QueryMetric(metric="sum(orders.revenue)", alias="revenue_sum"),
            ],
        )
        with pytest.raises(ValueError, match=r"declared more than once"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_rename_leaf_vs_sibling_canonical_leaf_stay_distinct(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A metric renamed to a sibling's canonical leaf (``id_count_distinct``)
        stays a distinct public column: the renamed key is source-dataset-prefixed,
        the unrenamed one hop-qualified, so they can't collide."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(customers.revenue)", alias="id_count_distinct"),
                QueryMetric(metric="count_distinct(customers.id)"),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "id_count_distinct",
            "customers.id_count_distinct",
        ], (
            f"renamed and unrenamed cross-dataset metrics must not merge into "
            f"one public column; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        cm_ctes = [name for name in _cte_names(sql) if name.startswith("_cm_")]
        assert len(set(cm_ctes)) == 2, (
            f"expected 2 distinct cross-dataset CTEs, got {cm_ctes!r}\nSQL:\n{sql}"
        )
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"' in sql
        assert (
            'COUNT(DISTINCT customers.id) AS "id_count_distinct"'
            in sql
        )

    async def test_two_local_renames_mutually_colliding_canonicals_stay_distinct(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Symmetric swap of two local renames (A's name == B's canonical and
        vice versa) keeps both aggregates distinct with their own bodies."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(orders.revenue)", alias="revenue_avg"),
                QueryMetric(metric="avg(orders.revenue)", alias="revenue_sum"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "revenue_avg", "revenue_sum"], (
            f"the symmetric rename swap must keep both metrics distinct; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        # The swap didn't swap bodies: ``revenue_avg`` is still the SUM.
        assert 'SUM(orders.amount) AS REAL) AS "revenue_avg"' in sql, (
            f"'revenue_avg' must carry the declared sum(revenue) body:\n{sql}"
        )
        assert 'AVG(orders.amount) AS REAL) AS "revenue_sum"' in sql, (
            f"'revenue_sum' must carry the declared avg(revenue) body:\n{sql}"
        )

    async def test_cross_model_rename_collides_with_outer_source_column_raises(
        self, tmp_path,
    ) -> None:
        """A rename colliding with a source field on the outer dataset is rejected."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_dataset(_customers_dataset())
        orders = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="cust_rev", expression="cust_rev", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        await storage.save_dataset(orders)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
        )
        with pytest.raises(ValueError, match=r"matches a source field"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_duplicate_explicit_name_raises(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Two cross-dataset metrics with the same explicit ``name`` are rejected."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(customers.revenue)", alias="metric"),
                QueryMetric(metric="count_distinct(customers.id)", alias="metric"),
            ],
        )
        with pytest.raises(ValueError, match=r"declared more than once\.\n  at metric 'metric'"):
            await engine.execute(query=query, dry_run=True)

    async def test_cross_model_star_count_vs_renamed_sibling_stay_distinct(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A ``count(*)`` (canonical leaf ``_count``) and a sibling renamed to
        ``_count`` stay distinct: ``orders._count`` vs ``orders.customers._count``."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="count(customers.*)"),
                QueryMetric(metric="sum(customers.revenue)", alias="_count"),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "customers._count",
            "_count",
        ], (
            f"the count(*) canonical leaf and the renamed sibling must stay "
            f"distinct public columns; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'COUNT(*) AS "_count"' in sql, sql
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"' in sql, sql

    async def test_cross_model_rename_collides_with_dimension_downstream_short_raises(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A metric renamed to a dimension's ``__``-flattened downstream short
        (``customers__region_id``) is rejected when the stage is read by name."""
        engine, _ = orders_customers_engine
        stage = SlayerQuery(
            name="s",
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[
                QueryMetric(
                    metric="sum(customers.revenue)",
                    alias="customers__region_id",
                ),
            ],
        )
        downstream = SlayerQuery(population="s", dimensions=[QueryDimension(field="s.customers__region_id")])
        with pytest.raises(
            ValueError, match=r"'customers__region_id'.*flatten to the same downstream name",
        ):
            await engine.execute(query=[stage, downstream], dry_run=True)

    async def test_cross_model_rename_vs_dimension_stay_distinct(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A metric renamed ``region_id`` and a ``customers.region_id`` dimension
        occupy distinct public keys (``orders.region_id`` vs the hop-qualified one)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="region_id")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["customers.region_id", "region_id"], (
            f"the dimension key and the renamed metric key must stay "
            f"distinct; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"' in sql, sql

    async def test_cross_model_rename_vs_arithmetic_mangled_name_stay_distinct(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A metric renamed ``revenue_sum__div__100`` and an arithmetic
        ``sum(revenue) / 100`` stay distinct; the arithmetic one derives the
        sanitized ``orders.revenue_sum_100`` key."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(customers.revenue)", alias="revenue_sum__div__100"),
                QueryMetric(metric="sum(orders.revenue) / 100"),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == [
            "orders.status",
            "revenue_sum__div__100",
            "orders.revenue_sum_100",
        ], (
            f"the arithmetic metric and the renamed cross-dataset metric must "
            f"stay distinct public keys; got {aliases!r}\nSQL:\n{sql}"
        )
        assert len(set(aliases)) == len(aliases), (
            f"public projection has a duplicate alias — silent merge; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )
        assert 'SUM(orders.amount) AS REAL) / 100 AS "orders.revenue_sum_100"' in sql, sql
        assert 'CAST(SUM(customers.lifetime_revenue) AS REAL) AS "revenue_sum"' in sql, sql

    async def test_two_local_renames_distinct_canonicals_pass(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Two local renames with non-colliding names and canonicals resolve
        (no false-positive rejection)."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(orders.revenue)", alias="rev"),
                QueryMetric(metric="avg(orders.revenue)", alias="rev_avg"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        aliases = set(_public_projection_aliases(resp.sql or ""))
        assert "rev" in aliases, aliases
        assert "rev_avg" in aliases, aliases


# Group E — regression guards: no-rename and local-rename paths unchanged.


class TestRenameRegressionGuards:
    async def test_cross_model_no_rename_unchanged(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Without a name, the public key stays canonical ``<dataset>.<hop>.<col>_<agg>``."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "customers.revenue_sum"], (
            f"cross-dataset key without rename must stay canonical; got "
            f"{aliases!r}\nSQL:\n{sql}"
        )

    async def test_local_rename_unchanged(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Local rename: the user name becomes the public key over the aggregate."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(orders.revenue)", alias="rev")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == ["orders.status", "rev"], sql
        assert 'SUM(orders.amount) AS REAL) AS "rev"' in sql, sql

    async def test_cross_model_canonical_unreachable_via_user_name(self) -> None:  # NOSONAR(S7503) — sibling tests in this class do await
        """Invariant: cross-dataset canonicals contain dots but ``DatasetMetric.name``
        rejects them, so a name can never equal a cross-dataset canonical."""
        with pytest.raises(ValidationError, match=r"only letters, digits, and underscores"):
            DatasetMetric(expression="sum(customers.revenue)", name="customers.revenue_sum")


# Group F — label/type propagation through the rename.


class TestCrossModelRenameLabelAndType:
    async def test_cross_model_rename_label_propagates(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """``label`` survives the rename, published under the renamed result key."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(
                    metric="sum(customers.revenue)",
                    alias="cust_rev",
                    label="Customer revenue",
                ),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert _public_projection_aliases(resp.sql or "") == [
            "orders.status", "cust_rev",
        ], resp.sql
        meta = resp.attributes.get("cust_rev")
        assert meta is not None, (
            f"no response metadata under the renamed key; got "
            f"{resp.attributes!r}"
        )
        assert meta.label == "Customer revenue", (
            f"label must propagate to the renamed cross-dataset metric; got "
            f"{meta.label!r}"
        )

    async def test_cross_model_rename_type_propagates_to_measure(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """``type=INT`` survives the rename as a CAST in the cross-dataset CTE."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(
                    metric="sum(customers.revenue)",
                    alias="cust_rev",
                    datatype=DataType.INTEGER,
                ),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == [
            "orders.status", "cust_rev",
        ], sql
        assert (
            'CAST(SUM(customers.lifetime_revenue) AS INTEGER) '
            'AS "revenue_sum"' in sql
        ), (
            f"declared type=INT must survive the rename as a CAST on the "
            f"cross-dataset aggregate:\n{sql}"
        )


# Group G — filter / ORDER BY interaction with the rename.


class TestCrossModelRenameOrderBy:
    """ORDER BY via the bare user alias resolves to the cross-dataset CTE column."""

    async def test_order_by_user_alias_resolves_to_cross_model_cte_column(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """ORDER BY on the bare user alias sorts on the CTE column, ``desc`` kept."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
            order=[OrderItem(field="cust_rev", direction="desc")],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert "ORDER BY" in sql, sql
        order_clause = sql.split("ORDER BY", 1)[1]
        assert '"revenue_sum"' in order_clause, (
            f"ORDER BY via bare user alias must resolve to the cross-dataset "
            f"CTE's output column:\n{sql}"
        )
        assert "DESC" in order_clause.upper(), (
            f"ORDER BY direction must survive the alias resolution:\n{sql}"
        )


class TestCrossModelRenameFilters:
    async def test_filter_via_user_alias_resolves_to_cross_model_value(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Filter ``"cust_rev > 100"`` lands as an outer WHERE on the producer
        CTE's output column, not a base-table column."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
            filter=["cust_rev > 100"],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert _public_projection_aliases(sql) == [
            "orders.status", "cust_rev",
        ], sql
        assert (
            '_cm_orders__customers__revenue_sum."revenue_sum" > 100'
            in _norm(sql)
        ), (
            f"the bare user alias must resolve to a WHERE on the cross-dataset "
            f"aggregate's value:\n{sql}"
        )
        assert "orders.cust_rev > 100" not in _norm(sql), (
            f"the filter must not be emitted against a non-existent base-table "
            f"column:\n{sql}"
        )


class TestDeferredCrossModelFilterScope:
    async def test_cross_model_aggregate_filter_with_rename_deferred(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """Filter ``sum(customers.revenue) > 100`` with a rename lands
        as an outer WHERE on the producer CTE column; metric still projects."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="sum(customers.revenue)", alias="cust_rev")],
            filter=["sum(customers.revenue) > 100"],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        assert "cust_rev" in sql, sql
        assert _public_projection_aliases(sql) == [
            "orders.status", "cust_rev",
        ], sql
        assert (
            '_cm_orders__customers__revenue_sum."revenue_sum" > 100'
            in _norm(sql)
        ), (
            f"cross-dataset aggregate filter must land as a WHERE on the "
            f"cross-dataset aggregate's value:\n{sql}"
        )


# Group H — multi-hop and same-query rename/no-rename mix (CTE uniqueness).


class TestCrossModelRenameMultiHopAndCTEUniqueness:
    async def test_cross_model_rename_multi_hop(
        self, orders_customers_regions_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """A two-hop metric renamed ``region_pop`` projects the bare user name;
        the CTE keeps the fully hop-qualified canonical column."""
        engine, _ = orders_customers_regions_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(
                    metric="sum(customers.regions.population)",
                    alias="region_pop",
                ),
            ],
            to_many_handling="broadcast",
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert aliases == ["orders.status", "region_pop"], (
            f"multi-hop cross-dataset rename must project the bare user name "
            f"(no __-flattened hops); got {aliases!r}\nSQL:\n{sql}"
        )
        assert (
            'CAST(SUM(regions.population) AS REAL) AS "population_sum"'
            in sql
        ), f"multi-hop CTE column stays canonical (target-rooted):\n{sql}"

    async def test_renamed_and_unrenamed_cross_model_no_collision(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        """One renamed + one unrenamed cross-dataset metric produce distinct CTEs
        and distinct projection aliases."""
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(customers.revenue)", alias="cust_rev"),
                QueryMetric(metric="count_distinct(customers.id)"),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        aliases = _public_projection_aliases(sql)
        assert "cust_rev" in aliases, (f"{aliases!r}\n{sql}")
        assert "customers.id_count_distinct" in aliases, (f"{aliases!r}\n{sql}")
        assert len(set(aliases)) == len(aliases) == 3, (
            f"renamed + unrenamed cross-dataset must produce distinct aliases; "
            f"got {aliases!r}\nSQL:\n{sql}"
        )
        cm_ctes = [name for name in _cte_names(sql) if name.startswith("_cm_")]
        assert len(set(cm_ctes)) == 2, (
            f"renamed + unrenamed cross-dataset must produce 2 distinct CTEs; "
            f"got {cm_ctes!r}\nSQL:\n{sql}"
        )


# Group I — transform-wrapped cross-dataset with `name`; pins current (unsupported).


class TestTransformWrappedCrossModelDeferred:
    @pytest.mark.skip(
        reason=(
            "Transform-wrapped cross-dataset agg refs are still unsupported. "
            "``cumsum(sum(customers.revenue))`` with a top-level ``name`` goes "
            "down the transform path, not the cross-dataset path; on the typed "
            "pipeline it raises `Transform 'cumsum' requires an unambiguous "
            "time dimension` for this dimension-only query rather than "
            "renaming the hoisted cross-dataset aggregate. Flip into a coverage "
            "test if/when this case is fixed."
        )
    )
    async def test_transform_wrapped_cross_model_with_name_pinned(
        self, orders_customers_engine: tuple[SlayerQueryEngine, SlayerDataset],
    ) -> None:
        engine, _ = orders_customers_engine
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            time_dimensions=[],
            metrics=[
                QueryMetric(
                    metric="cumsum(sum(customers.revenue))",
                    alias="cum_cust_rev",
                ),
            ],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert _public_projection_aliases(resp.sql or "") == [
            "orders.status", "orders.cum_cust_rev",
        ], (
            f"transform-wrapped cross-dataset rename not implemented; got "
            f"{resp.columns!r}"
        )
