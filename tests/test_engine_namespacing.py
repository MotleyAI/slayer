"""Engine-side bare-name resolution after v4 datasource namespacing.

The engine accepts model references in two shapes:

* ``engine.execute("orders", ...)`` — run-by-name (resolves a query-backed model).
* ``SlayerQuery(source_model="orders")`` — string source model in a query.
* ``ModelJoin(target_model="customers")`` — join targets inside models/queries.

Each of these is a bare name. After v4 the resolution algorithm is:

1. If a ``data_source`` hint is available (the *parent* model's data_source for
   join targets, or an explicit kwarg on ``execute``), prefer that datasource.
2. Otherwise fall back to ``storage.resolve_model_identity(name)``, which
   applies the priority list and raises ``AmbiguousModelError`` on conflict.

These tests build minimal SQLite-backed engines, skip actual DB execution via
``dry_run=True``, and assert against the generated SQL or the error type.
"""

import tempfile

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import AmbiguousModelError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def _orders(data_source: str, *, sql_table: str = "orders_t", joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table=sql_table,
        data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        joins=joins or [],
    )


def _customers(data_source: str, *, sql_table: str = "customers_t", with_revenue: bool = False) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="region", sql="region", type=DataType.TEXT),
    ]
    if with_revenue:
        cols.append(Column(name="revenue", sql="revenue", type=DataType.DOUBLE))
    return SlayerModel(
        name="customers",
        sql_table=sql_table,
        data_source=data_source,
        columns=cols,
    )


def _ds(name: str) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="sqlite", database=":memory:")


async def _engine_with(*models: SlayerModel, datasources: list[str]) -> tuple:
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    for n in datasources:
        await storage.save_datasource(_ds(n))
    for m in models:
        await storage.save_model(m)
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


# ---------------------------------------------------------------------------
# source_model resolution
# ---------------------------------------------------------------------------


class TestSourceModelResolution:
    async def test_unique_bare_name_resolves(self) -> None:
        """Single ``orders`` across all datasources → bare ``source_model`` works."""
        engine, tmp = await _engine_with(_orders("db_a"), datasources=["db_a"])
        try:
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
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
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            with pytest.raises(AmbiguousModelError) as exc:
                await engine.execute(q, dry_run=True)
            assert "db_a" in str(exc.value) and "db_b" in str(exc.value)
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
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
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
            q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            assert "db_a.orders_t" in resp.sql
            assert "db_b.orders_t" not in resp.sql
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Join target resolution prefers the parent's data_source
# ---------------------------------------------------------------------------


class TestJoinTargetResolution:
    async def test_join_resolves_within_parent_datasource(self) -> None:
        """``orders@db_a`` joins ``customers``. Both ``customers@db_a`` and
        ``customers@db_b`` exist. The join must resolve within db_a — the
        parent model's ``data_source`` is the resolution hint, no query-side
        marker required."""
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
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
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["customers.region"],
            )
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            # The db_a customers table is what got joined.
            assert "db_a.customers_t" in resp.sql
            assert "db_b.customers_t" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_join_target_only_in_other_datasource_raises(self) -> None:
        """The join target doesn't exist in the parent's datasource and no
        priority is configured → engine raises a join-resolution error."""
        orders_a = _orders(
            "db_a",
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_b"),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["customers.region"],
            )
            with pytest.raises((AmbiguousModelError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Run-by-name (engine.execute(str)) follows the same rules
# ---------------------------------------------------------------------------


class TestRunByName:
    async def test_ambiguous_run_by_name_raises(self) -> None:
        """``engine.execute("rev")`` with two query-backed models named
        ``rev`` in different datasources → ambiguity."""
        rev_a = SlayerModel(
            name="rev",
            data_source="db_a",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
            )],
        )
        rev_b = SlayerModel(
            name="rev",
            data_source="db_b",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
            )],
        )
        engine, tmp = await _engine_with(
            _orders("db_a"),
            _orders("db_b"),
            rev_a,
            rev_b,
            datasources=["db_a", "db_b"],
        )
        try:
            with pytest.raises(AmbiguousModelError):
                await engine.execute("rev", dry_run=True)
        finally:
            tmp.cleanup()

    async def test_run_by_name_with_data_source_kwarg(self) -> None:
        rev_a = SlayerModel(
            name="rev",
            data_source="db_a",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
            )],
        )
        rev_b = SlayerModel(
            name="rev",
            data_source="db_b",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
            )],
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
# Cross-model measure / re-rooted resolvers must scope hops the same way
# (CR #6 on PR #92).
# ---------------------------------------------------------------------------


class TestCrossModelMeasureScoping:
    """Hop resolution inside cross-model measures must use the parent's
    ``data_source`` as the priority hint, exactly like the dimension path.
    Without it, ``customers.revenue:sum`` against ``orders@db_a`` either
    silently joins ``db_b.customers_t`` or raises ``AmbiguousModelError``
    when both ``customers@db_a`` and ``customers@db_b`` exist.
    """

    async def test_cross_model_measure_resolves_within_parent_datasource(self) -> None:
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
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
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customers.revenue:sum"}],
            )
            resp = await engine.execute(q, data_source="db_a", dry_run=True)
            assert resp.sql is not None
            assert "db_a.customers_t" in resp.sql
            assert "db_b.customers_t" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_cross_model_measure_only_in_other_datasource_raises(self) -> None:
        """Parent's datasource doesn't contain the join target's model →
        cross-model measure must fail rather than silently using db_b's
        ``customers``.
        """
        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
                join_type=JoinType.LEFT,
            )],
        )
        engine, tmp = await _engine_with(
            orders_a,
            _customers("db_b", sql_table="db_b.customers_t", with_revenue=True),
            datasources=["db_a", "db_b"],
        )
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customers.revenue:sum"}],
            )
            with pytest.raises((AmbiguousModelError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Join-target resolution must NOT silently fall back to a bare lookup when
# the parent's datasource is known (CR #5 on PR #92).
# ---------------------------------------------------------------------------


class TestJoinTargetStrictWhenParentScoped:
    async def test_join_target_does_not_silently_resolve_outside_parent_ds(self) -> None:
        """Even before any dimension/measure references ``customers``, the
        join-target resolution path must not pick ``customers@db_b`` when
        the parent is ``orders@db_a``. Today the bare-name fallback at
        ``_resolve_join_target`` returns ``customers@db_b`` silently — this
        test pins the corrected behavior: no fallback when parent scoped.
        """
        # We exercise the join-target resolver directly: it's called during
        # enrichment regardless of whether dimensions reference the joined
        # table, so a wrong choice here corrupts later phases.
        from slayer.engine.query_engine import SlayerQueryEngine  # noqa: F401 (clarity)

        orders_a = _orders(
            "db_a",
            sql_table="db_a.orders_t",
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
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
            target = await engine.storage.get_model("customers", data_source="db_a")
            assert target is None  # Sanity: customers@db_a doesn't exist.

            # The bare-name resolution gives db_b's customers (unique match).
            # The fix should make join_target resolution refuse to fall back
            # to that when the parent already declared a data_source.
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["customers.region"],
            )
            with pytest.raises((AmbiguousModelError, ValueError), match=r"customers"):
                await engine.execute(q, data_source="db_a", dry_run=True)
        finally:
            tmp.cleanup()
