"""Engine-side bare-name resolution after v4 datasource namespacing.

The engine accepts dataset references in two shapes:

* ``engine.execute("orders", ...)`` — run-by-name (resolves a query-backed dataset).
* ``SlayerQuery(population="orders")`` — string population dataset in a query.
* ``DatasetRelationship(to="customers")`` — relationship targets inside datasets/queries.

Each of these is a bare name. After v4 the resolution algorithm is:

1. If a ``data_source`` hint is available (the *parent* dataset's data_source for
   relationship targets, or an explicit kwarg on ``execute``), prefer that datasource.
2. Otherwise fall back to ``storage.resolve_dataset_identity(name)``, which
   applies the priority list and raises ``AmbiguousDatasetError`` on conflict.

These tests build minimal SQLite-backed engines, skip actual DB execution via
``dry_run=True``, and assert against the generated SQL or the error type.
"""

import tempfile

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import AmbiguousDatasetError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def _orders(data_source: str, *, sql_table: str = "orders_t", relationships: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table=sql_table,
        data_source=data_source,
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
        ],
        relationships=relationships or [],
    )


def _customers(data_source: str, *, sql_table: str = "customers_t", with_revenue: bool = False) -> SlayerDataset:
    cols = [
        DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="region", expression="region", datatype=DataType.STRING),
    ]
    if with_revenue:
        cols.append(DatasetField(name="revenue", expression="revenue", datatype=DataType.FLOAT))
    return SlayerDataset(
        name="customers",
        sql_table=sql_table,
        data_source=data_source,
        fields=cols,
    )


def _ds(name: str) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="sqlite", database=":memory:")


async def _engine_with(*datasets: SlayerDataset, datasources: list[str]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    for n in datasources:
        await storage.save_datasource(_ds(n))
    for m in datasets:
        await storage.save_dataset(m)
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


# ---------------------------------------------------------------------------
# population resolution
# ---------------------------------------------------------------------------


class TestSourceModelResolution:
    async def test_unique_bare_name_resolves(self) -> None:
        """Single ``orders`` across all datasources → bare ``population`` works."""
        engine, tmp = await _engine_with(_orders("db_a"), datasources=["db_a"])
        try:
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
            resp = await engine.execute(q, dry_run=True)
            assert resp.sql is not None
            assert "amount" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_ambiguous_bare_source_model_raises(self) -> None:
        """``orders`` exists in two datasources, no priority set → ambiguity."""
        engine, tmp = await _engine_with(
            _orders("db_a"),
            _orders("db_b"),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
            with pytest.raises(AmbiguousDatasetError) as exc:
                await engine.execute(q, dry_run=True)
            assert "db_a" in str(exc.value)
            assert "db_b" in str(exc.value)
        finally:
            tmp.cleanup()

    async def test_priority_disambiguates_source_model(self) -> None:
        engine, tmp = await _engine_with(
            _orders("db_a", sql_table="db_a.orders_t"),
            _orders("db_b", sql_table="db_b.orders_t"),
            datasources=["db_a", "db_b"],
        )
        try:
            await engine.storage.set_datasource_priority(["db_b", "db_a"])
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
            resp = await engine.execute(q, dry_run=True)
            assert resp.sql is not None
            # db_b's table appears, not db_a's.
            assert "db_b.orders_t" in resp.sql
            assert "db_a.orders_t" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_explicit_data_source_kwarg_overrides_priority(self) -> None:
        """``execute(query, data_source=...)`` skips the priority list entirely."""
        engine, tmp = await _engine_with(
            _orders("db_a", sql_table="db_a.orders_t"),
            _orders("db_b", sql_table="db_b.orders_t"),
            datasources=["db_a", "db_b"],
        )
        try:
            await engine.storage.set_datasource_priority(["db_b"])
            q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            assert "db_a.orders_t" in resp.sql
            assert "db_b.orders_t" not in resp.sql
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Relationship target resolution prefers the parent's data_source
# ---------------------------------------------------------------------------


class TestJoinTargetResolution:
    async def test_join_resolves_within_parent_datasource(self) -> None:
        """``orders@db_a`` relates to ``customers``. Both ``customers@db_a`` and
        ``customers@db_b`` exist. The relationship must resolve within db_a — the
        parent dataset's ``data_source`` is the resolution hint, no query-side
        marker required."""
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            relationships=[DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_a", sql_table="db_a.customers_t"),
            _customers("db_b", sql_table="db_b.customers_t"),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["customers.region"],
            })
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            # The db_a customers table is what got joined.
            assert "db_a.customers_t" in resp.sql
            assert "db_b.customers_t" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_join_target_only_in_other_datasource_raises(self) -> None:
        """The relationship target doesn't exist in the parent's datasource and no
        priority is configured → engine raises a relationship-resolution error."""
        orders_a = _orders(
            "db_a",
            relationships=[DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_b"),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["customers.region"],
            })
            with pytest.raises((AmbiguousDatasetError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Run-by-name (engine.execute(str)) follows the same rules
# ---------------------------------------------------------------------------


class TestRunByName:
    async def test_ambiguous_run_by_name_raises(self) -> None:
        """``engine.execute("rev")`` with two query-backed datasets named
        ``rev`` in different datasources → ambiguity."""
        rev_a = SlayerDataset(
            name="rev",
            data_source="db_a",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        rev_b = SlayerDataset(
            name="rev",
            data_source="db_b",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        engine, tmp = await _engine_with(
            _orders("db_a"),
            _orders("db_b"),
            rev_a,
            rev_b,
            datasources=["db_a", "db_b"],
        )
        try:
            with pytest.raises(AmbiguousDatasetError):
                await engine.execute("rev", dry_run=True)
        finally:
            tmp.cleanup()

    async def test_run_by_name_with_data_source_kwarg(self) -> None:
        rev_a = SlayerDataset(
            name="rev",
            data_source="db_a",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        rev_b = SlayerDataset(
            name="rev",
            data_source="db_b",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
            })],
        )
        engine, tmp = await _engine_with(
            _orders("db_a", sql_table="db_a.orders_t"),
            _orders("db_b", sql_table="db_b.orders_t"),
            rev_a,
            rev_b,
            datasources=["db_a", "db_b"],
        )
        try:
            resp = await engine.execute("rev", data_source="db_a", dry_run=True)
            assert resp.sql is not None
            assert "db_a.orders_t" in resp.sql
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Cross-dataset metric / re-rooted resolvers must scope hops the same way
# (CR #6 on PR #92).
# ---------------------------------------------------------------------------


class TestCrossModelMeasureScoping:
    """Hop resolution inside cross-dataset metrics must use the parent's
    ``data_source`` as the priority hint, exactly like the dimension path.
    Without it, ``sum(customers.revenue)`` against ``orders@db_a`` either
    silently joins ``db_b.customers_t`` or raises ``AmbiguousDatasetError``
    when both ``customers@db_a`` and ``customers@db_b`` exist.
    """

    async def test_cross_model_measure_resolves_within_parent_datasource(self) -> None:
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            relationships=[DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_a", sql_table="db_a.customers_t", with_revenue=True),
            _customers("db_b", sql_table="db_b.customers_t", with_revenue=True),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(customers.revenue)"}],
            })
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            assert "db_a.customers_t" in resp.sql
            assert "db_b.customers_t" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_cross_model_measure_only_in_other_datasource_raises(self) -> None:
        """Parent's datasource doesn't contain the relationship target's dataset →
        cross-dataset metric must fail rather than silently using db_b's
        ``customers``.
        """
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            relationships=[DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_b", sql_table="db_b.customers_t", with_revenue=True),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(customers.revenue)"}],
            })
            with pytest.raises((AmbiguousDatasetError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Relationship-target resolution must NOT silently fall back to a bare lookup when
# the parent's datasource is known (CR #5 on PR #92).
# ---------------------------------------------------------------------------


class TestJoinTargetStrictWhenParentScoped:
    async def test_join_target_does_not_silently_resolve_outside_parent_ds(self) -> None:
        """Even before any dimension/metric references ``customers``, the
        relationship-target resolution path must not pick ``customers@db_b`` when
        the parent is ``orders@db_a``. Today the bare-name fallback at
        ``_resolve_join_target`` returns ``customers@db_b`` silently — this
        test pins the corrected behavior: no fallback when parent scoped.
        """
        # We exercise the relationship-target resolver directly: it's called during
        # enrichment regardless of whether dimensions reference the joined
        # table, so a wrong choice here corrupts later phases.
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            relationships=[DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["id"],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_b", sql_table="db_b.customers_t"),
            datasources=["db_a", "db_b"],
        )
        try:
            # Reach in to verify the resolver itself.
            target = await engine.storage.get_dataset("customers", data_source="db_a")
            assert target is None  # Sanity: customers@db_a doesn't exist.

            # The bare-name resolution gives db_b's customers (unique match).
            # The fix should make join_target resolution refuse to fall back
            # to that when the parent already declared a data_source.
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(orders.amount)"}],
                "dimensions": ["customers.region"],
            })
            with pytest.raises((AmbiguousDatasetError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()
